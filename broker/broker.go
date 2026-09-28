// Package broker 实现本地持久化消息投递运行系统：
// 主题、订阅、消费位置、独占投递（带超时转交与纪元 fencing）、确认与重启恢复。
package broker

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Message 是一条持久化消息。
type Message struct {
	Topic     string    `json:"topic"`
	Offset    int64     `json:"offset"`
	Key       string    `json:"key"`
	Payload   []byte    `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

// ID 返回消息全局标识。
func (m Message) ID() string { return fmt.Sprintf("%s:%d", m.Topic, m.Offset) }

// Delivery 表示一次投递，Token 为本次投递的确认凭据（含纪元）。
type Delivery struct {
	Message  Message
	Token    string
	Epoch    uint64
	Owner    string
	Deadline time.Time
}

// AckRecord 记录一条消息的最终去向（由谁确认）。
type AckRecord struct {
	Topic    string    `json:"topic"`
	Offset   int64     `json:"offset"`
	Sub      string    `json:"sub"`
	Consumer string    `json:"consumer"`
	Epoch    uint64    `json:"epoch"`
	At       time.Time `json:"at"`
}

var (
	ErrTopicExists  = errors.New("broker: topic already exists")
	ErrNoTopic      = errors.New("broker: no such topic")
	ErrSubExists    = errors.New("broker: subscription already exists")
	ErrNoSub        = errors.New("broker: no such subscription")
	ErrStaleToken   = errors.New("broker: stale or invalid delivery token")
	ErrNotOwner     = errors.New("broker: consumer is not the current owner")
	ErrAlreadyAcked = errors.New("broker: message already acknowledged")
)

type inFlight struct {
	owner    string
	epoch    uint64
	deadline time.Time
}

type subState struct {
	name     string
	topic    string
	acked    map[int64]bool
	inFlight map[int64]*inFlight
	epochSeq uint64
}

// Broker 是消息投递运行系统本体。
type Broker struct {
	dir string

	mu     sync.Mutex
	topics map[string][]Message
	subs   map[string]*subState // key: topic+"/"+name
	acks   []AckRecord          // 全量确认去向记录（持久化）
	now    func() time.Time
}

// Open 打开（或创建）位于 dir 的 Broker，并从磁盘恢复状态。
func Open(dir string) (*Broker, error) {
	b := &Broker{
		dir:    dir,
		topics: map[string][]Message{},
		subs:   map[string]*subState{},
		now:    time.Now,
	}
	if err := b.load(); err != nil {
		return nil, err
	}
	return b, nil
}

// Close 落盘全部状态。
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.flushLocked()
}

// CreateTopic 建立主题。
func (b *Broker) CreateTopic(name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.topics[name]; ok {
		return ErrTopicExists
	}
	b.topics[name] = nil
	return b.flushLocked()
}

// CreateSubscription 在主题上建立订阅（订阅名即消费组名）。
func (b *Broker) CreateSubscription(topic, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.topics[topic]; !ok {
		return ErrNoTopic
	}
	key := topic + "/" + name
	if _, ok := b.subs[key]; ok {
		return ErrSubExists
	}
	b.subs[key] = &subState{
		name:     name,
		topic:    topic,
		acked:    map[int64]bool{},
		inFlight: map[int64]*inFlight{},
	}
	return b.flushLocked()
}

// Publish 持久化一条消息到主题，返回偏移。
func (b *Broker) Publish(topic, key string, payload []byte) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	msgs, ok := b.topics[topic]
	if !ok {
		return 0, ErrNoTopic
	}
	m := Message{
		Topic:     topic,
		Offset:    int64(len(msgs)),
		Key:       key,
		Payload:   append([]byte(nil), payload...),
		Timestamp: b.now(),
	}
	b.topics[topic] = append(msgs, m)
	if err := b.appendMessageLocked(m); err != nil {
		return 0, err
	}
	return m.Offset, nil
}

// Pull 以 consumerID 身份从订阅拉取最多 max 条消息，
// 每条获得 visibilityTimeout 的独占可见期。超时未确认的消息会被回收并转交。
func (b *Broker) Pull(topic, sub, consumerID string, max int, visibilityTimeout time.Duration) ([]Delivery, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return nil, err
	}
	msgs := b.topics[topic]
	now := b.now()

	// 回收超时在途消息：旧 Token 即刻失效（fencing）。
	for off, f := range s.inFlight {
		if !f.deadline.After(now) {
			delete(s.inFlight, off)
		}
	}

	var out []Delivery
	for off := int64(0); off < int64(len(msgs)) && len(out) < max; off++ {
		if s.acked[off] {
			continue
		}
		if _, held := s.inFlight[off]; held {
			continue
		}
		s.epochSeq++
		f := &inFlight{owner: consumerID, epoch: s.epochSeq, deadline: now.Add(visibilityTimeout)}
		s.inFlight[off] = f
		out = append(out, Delivery{
			Message:  msgs[off],
			Token:    token(topic, sub, off, f.epoch),
			Epoch:    f.epoch,
			Owner:    consumerID,
			Deadline: f.deadline,
		})
	}
	if len(out) > 0 {
		if err := b.flushLocked(); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// Ack 确认一次投递。Token 必须与当前持有人及纪元匹配，
// 超时转交后旧持有人的 Token 会被拒绝，不会误伤新持有人。
func (b *Broker) Ack(topic, sub, consumerID, tok string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return err
	}
	off, epoch, err := parseToken(topic, sub, tok)
	if err != nil {
		return ErrStaleToken
	}
	if s.acked[off] {
		return ErrAlreadyAcked
	}
	f, held := s.inFlight[off]
	if !held || f.epoch != epoch {
		return ErrStaleToken
	}
	if f.owner != consumerID {
		return ErrNotOwner
	}
	s.acked[off] = true
	delete(s.inFlight, off)
	b.acks = append(b.acks, AckRecord{
		Topic:    topic,
		Offset:   off,
		Sub:      sub,
		Consumer: consumerID,
		Epoch:    epoch,
		At:       b.now(),
	})
	if err := b.appendAckLocked(b.acks[len(b.acks)-1]); err != nil {
		return err
	}
	return b.flushLocked()
}

// Position 返回订阅的消费位置：已确认的连续前缀长度（即下一条待消费偏移）。
func (b *Broker) Position(topic, sub string) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return 0, err
	}
	pos := int64(0)
	for s.acked[pos] {
		pos++
	}
	return pos, nil
}

// AckedBy 返回每条消息的实际去向（offset -> 确认它的消费者）。
func (b *Broker) AckedBy(topic, sub string) (map[int64]string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, err := b.subLocked(topic, sub); err != nil {
		return nil, err
	}
	out := map[int64]string{}
	for _, r := range b.acks {
		if r.Topic == topic && r.Sub == sub {
			out[r.Offset] = r.Consumer
		}
	}
	return out, nil
}

// AckRecords 返回全量确认记录（按确认时间排序）。
func (b *Broker) AckRecords() []AckRecord {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := append([]AckRecord(nil), b.acks...)
	sort.Slice(out, func(i, j int) bool { return out[i].At.Before(out[j].At) })
	return out
}

func (b *Broker) subLocked(topic, sub string) (*subState, error) {
	s, ok := b.subs[topic+"/"+sub]
	if !ok {
		return nil, ErrNoSub
	}
	return s, nil
}

func token(topic, sub string, off int64, epoch uint64) string {
	return fmt.Sprintf("%s/%s/%d/%d", topic, sub, off, epoch)
}

func parseToken(topic, sub, tok string) (int64, uint64, error) {
	parts := strings.Split(tok, "/")
	if len(parts) != 4 {
		return 0, 0, ErrStaleToken
	}
	t, s := parts[0], parts[1]
	off, err1 := strconv.ParseInt(parts[2], 10, 64)
	epoch, err2 := strconv.ParseUint(parts[3], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0, ErrStaleToken
	}
	if t != topic || s != sub {
		return 0, 0, ErrStaleToken
	}
	return off, epoch, nil
}
