package broker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// 磁盘布局：
//   <dir>/topics/<topic>.jsonl   消息日志（追加写）
//   <dir>/subs.json              订阅与已确认偏移（原子重写）
//   <dir>/acks.jsonl             确认去向记录（追加写）
//   <dir>/history.jsonl          消息历史（领取/超时/拒绝/确认/隔离/重投，追加写）

type subDisk struct {
	Name         string              `json:"name"`
	Topic        string              `json:"topic"`
	Acked        []int64             `json:"acked"`
	EpochSeq     uint64              `json:"epoch_seq"`
	Attempts     map[int64]int       `json:"attempts,omitempty"`
	Quarantined  []int64             `json:"quarantined,omitempty"`
	BackoffUntil map[int64]time.Time `json:"backoff_until,omitempty"`
}

type stateDisk struct {
	Subs []subDisk `json:"subs"`
}

func (b *Broker) topicsDir() string { return filepath.Join(b.dir, "topics") }

func (b *Broker) load() error {
	if err := os.MkdirAll(b.topicsDir(), 0o755); err != nil {
		return err
	}
	// 主题与消息日志
	entries, err := os.ReadDir(b.topicsDir())
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".jsonl") {
			continue
		}
		topic := strings.TrimSuffix(e.Name(), ".jsonl")
		f, err := os.Open(filepath.Join(b.topicsDir(), e.Name()))
		if err != nil {
			return err
		}
		sc := bufio.NewScanner(f)
		sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var m Message
			if err := json.Unmarshal([]byte(line), &m); err != nil {
				f.Close()
				return fmt.Errorf("load topic %s: %w", topic, err)
			}
			b.topics[topic] = append(b.topics[topic], m)
		}
		f.Close()
		if err := sc.Err(); err != nil {
			return err
		}
		if _, ok := b.topics[topic]; !ok {
			b.topics[topic] = nil
		}
	}
	// 订阅与确认位置
	data, err := os.ReadFile(filepath.Join(b.dir, "subs.json"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		var sd stateDisk
		if err := json.Unmarshal(data, &sd); err != nil {
			return fmt.Errorf("load subs: %w", err)
		}
		for _, s := range sd.Subs {
			st := &subState{
				name:         s.Name,
				topic:        s.Topic,
				acked:        map[int64]bool{},
				inFlight:     map[int64]*inFlight{},
				epochSeq:     s.EpochSeq,
				attempts:     map[int64]int{},
				quarantined:  map[int64]bool{},
				backoffUntil: map[int64]time.Time{},
			}
			for _, off := range s.Acked {
				st.acked[off] = true
			}
			for off, n := range s.Attempts {
				st.attempts[off] = n
			}
			for _, off := range s.Quarantined {
				st.quarantined[off] = true
			}
			for off, t := range s.BackoffUntil {
				st.backoffUntil[off] = t
			}
			b.subs[s.Topic+"/"+s.Name] = st
		}
	}
	// 确认去向记录
	af, err := os.Open(filepath.Join(b.dir, "acks.jsonl"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		defer af.Close()
		sc := bufio.NewScanner(af)
		sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var r AckRecord
			if err := json.Unmarshal([]byte(line), &r); err != nil {
				return fmt.Errorf("load acks: %w", err)
			}
			b.acks = append(b.acks, r)
		}
		if err := sc.Err(); err != nil {
			return err
		}
	}
	// 消息历史
	hf, err := os.Open(filepath.Join(b.dir, "history.jsonl"))
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		defer hf.Close()
		sc := bufio.NewScanner(hf)
		sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
		for sc.Scan() {
			line := strings.TrimSpace(sc.Text())
			if line == "" {
				continue
			}
			var r HistoryRecord
			if err := json.Unmarshal([]byte(line), &r); err != nil {
				return fmt.Errorf("load history: %w", err)
			}
			b.history = append(b.history, r)
		}
		if err := sc.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) appendMessageLocked(m Message) error {
	f, err := os.OpenFile(filepath.Join(b.topicsDir(), m.Topic+".jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

// appendHistoryLocked 追加一条消息历史并落盘。调用方须持有锁。
func (b *Broker) appendHistoryLocked(r HistoryRecord) {
	b.history = append(b.history, r)
	f, err := os.OpenFile(filepath.Join(b.dir, "history.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return
	}
	defer f.Close()
	data, err := json.Marshal(r)
	if err != nil {
		return
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return
	}
	f.Sync()
}

func (b *Broker) appendAckLocked(r AckRecord) error {
	f, err := os.OpenFile(filepath.Join(b.dir, "acks.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(data, '\n')); err != nil {
		return err
	}
	return f.Sync()
}

// flushLocked 原子重写订阅状态文件。调用方须持有锁。
func (b *Broker) flushLocked() error {
	sd := stateDisk{}
	for _, s := range b.subs {
		d := subDisk{Name: s.name, Topic: s.topic, EpochSeq: s.epochSeq,
			Attempts: s.attempts, BackoffUntil: s.backoffUntil}
		for off := range s.acked {
			d.Acked = append(d.Acked, off)
		}
		for off := range s.quarantined {
			d.Quarantined = append(d.Quarantined, off)
		}
		sd.Subs = append(sd.Subs, d)
	}
	data, err := json.MarshalIndent(sd, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(b.dir, "subs.json.tmp")
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(b.dir, "subs.json"))
}
