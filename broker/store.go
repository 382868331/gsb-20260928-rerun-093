package broker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// 磁盘布局：
//   <dir>/topics/<topic>.jsonl   消息日志（追加写）
//   <dir>/subs.json              订阅与已确认偏移（原子重写）
//   <dir>/acks.jsonl             确认去向记录（追加写）

type subDisk struct {
	Name     string  `json:"name"`
	Topic    string  `json:"topic"`
	Acked    []int64 `json:"acked"`
	EpochSeq uint64  `json:"epoch_seq"`
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
				name:     s.Name,
				topic:    s.Topic,
				acked:    map[int64]bool{},
				inFlight: map[int64]*inFlight{},
				epochSeq: s.EpochSeq,
			}
			for _, off := range s.Acked {
				st.acked[off] = true
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
		d := subDisk{Name: s.name, Topic: s.topic, EpochSeq: s.epochSeq}
		for off := range s.acked {
			d.Acked = append(d.Acked, off)
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
