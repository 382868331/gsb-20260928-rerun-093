package broker

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// 磁盘布局：
//   <dir>/topics/<topic>.jsonl   消息日志（追加写）
//   <dir>/subs.json              订阅、已确认偏移、退避、隔离与放弃状态（原子重写）
//   <dir>/acks.jsonl             确认去向记录（追加写）
//   <dir>/history.jsonl          消息历史：每次领取/超时/拒绝/确认/隔离/人工重投（追加写）

type subDisk struct {
	Name        string              `json:"name"`
	Topic       string              `json:"topic"`
	Acked       []int64             `json:"acked"`
	EpochSeq    uint64              `json:"epoch_seq"`
	Failures    map[int64]int       `json:"failures,omitempty"`
	RetryAt     map[int64]time.Time `json:"retry_at,omitempty"`
	Quarantined []int64             `json:"quarantined,omitempty"`
	Abandoned   []int64             `json:"abandoned,omitempty"`
	MaxFailures int                 `json:"max_failures,omitempty"`
	BackoffBase time.Duration       `json:"backoff_base,omitempty"`
	GrantSeq    uint64              `json:"grant_seq,omitempty"`
	Grants      []PartitionGrant    `json:"grants,omitempty"`
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
				name:        s.Name,
				topic:       s.Topic,
				acked:       map[int64]bool{},
				inFlight:    map[int64]*inFlight{},
				epochSeq:    s.EpochSeq,
				failures:    map[int64]int{},
				retryAt:     map[int64]time.Time{},
				quarantined: map[int64]bool{},
				abandoned:   map[int64]bool{},
				maxFailures: s.MaxFailures,
				backoffBase: s.BackoffBase,
				grants:      map[string]*PartitionGrant{},
				grantSeq:    s.GrantSeq,
			}
			if st.maxFailures < 1 {
				st.maxFailures = defaultMaxFailures
			}
			if st.backoffBase <= 0 {
				st.backoffBase = defaultBackoffBase
			}
			for _, off := range s.Acked {
				st.acked[off] = true
			}
			for off, n := range s.Failures {
				st.failures[off] = n
			}
			for off, t := range s.RetryAt {
				st.retryAt[off] = t
			}
			for _, off := range s.Quarantined {
				st.quarantined[off] = true
			}
			for _, off := range s.Abandoned {
				st.abandoned[off] = true
			}
			for i := range s.Grants {
				g := s.Grants[i]
				st.grants[g.ID] = &g
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
			var e HistoryEvent
			if err := json.Unmarshal([]byte(line), &e); err != nil {
				return fmt.Errorf("load history: %w", err)
			}
			b.history = append(b.history, e)
		}
		if err := sc.Err(); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) processGrantsAndFlushLocked(s *subState, now time.Time) error {
	changed, err := b.processGrantsLocked(s, now)
	if err != nil {
		return err
	}
	if changed {
		return b.flushLocked()
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

func (b *Broker) appendHistoryLocked(e HistoryEvent) error {
	f, err := os.OpenFile(filepath.Join(b.dir, "history.jsonl"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	data, err := json.Marshal(e)
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
		d := subDisk{
			Name:        s.name,
			Topic:       s.topic,
			EpochSeq:    s.epochSeq,
			Failures:    s.failures,
			RetryAt:     s.retryAt,
			MaxFailures: s.maxFailures,
			BackoffBase: s.backoffBase,
			GrantSeq:    s.grantSeq,
		}
		for off := range s.acked {
			d.Acked = append(d.Acked, off)
		}
		for off := range s.quarantined {
			d.Quarantined = append(d.Quarantined, off)
		}
		for off := range s.abandoned {
			d.Abandoned = append(d.Abandoned, off)
		}
		for _, g := range s.grants {
			d.Grants = append(d.Grants, *cloneGrant(g))
		}
		sort.Slice(d.Grants, func(i, j int) bool { return d.Grants[i].Seq < d.Grants[j].Seq })
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
