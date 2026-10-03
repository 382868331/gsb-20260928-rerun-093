// Package broker 实现本地持久化消息投递运行系统：
// 主题、订阅、消费位置、独占投递（带超时转交与纪元 fencing）、确认与重启恢复；
// 以及延迟投递、失败退避、隔离与人工重新投递、全量消息历史。
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
	// VisibleAt 之前消息对消费者不可见（延迟投递）。零值表示立即可见。
	VisibleAt time.Time `json:"visible_at,omitempty"`
}

// ID 返回消息全局标识。
func (m Message) ID() string { return fmt.Sprintf("%s:%d", m.Topic, m.Offset) }

// Delivery 表示一次投递，Token 为本次投递的确认凭据（含纪元）。
// Attempt 为第几次尝试（首次为 1，退避/超时/人工重投后递增）。
type Delivery struct {
	Message  Message
	Token    string
	Epoch    uint64
	Owner    string
	Deadline time.Time
	Attempt  int
}

// 历史事件类型。
const (
	EventDeliver    = "deliver"    // 一次领取（投递给某消费者）
	EventTimeout    = "timeout"    // 可见期超时被回收
	EventNack       = "nack"       // 持有人明确拒绝（进入退避）
	EventAck        = "ack"        // 确认完成
	EventQuarantine = "quarantine" // 失败次数达上限被隔离
	EventRedeliver  = "redeliver"  // 人工重新投递（保留原身份，开启新尝试）
	EventAbandon    = "abandon"    // 人工明确放弃（终态，不再投递，也不算确认）
)

// HistoryEvent 是消息历史中的一条记录：每一次领取、超时、拒绝、
// 确认、隔离与人工重投都会留下一条，最终确认者不会代替全部尝试记录。
type HistoryEvent struct {
	Topic    string    `json:"topic"`
	Offset   int64     `json:"offset"`
	Sub      string    `json:"sub"`
	Event    string    `json:"event"`
	Consumer string    `json:"consumer"` // 投递/确认的消费者，或人工重投的操作者
	Epoch    uint64    `json:"epoch"`
	Attempt  int       `json:"attempt"`
	At       time.Time `json:"at"`
	Detail   string    `json:"detail,omitempty"`
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
	ErrTopicExists    = errors.New("broker: topic already exists")
	ErrNoTopic        = errors.New("broker: no such topic")
	ErrSubExists      = errors.New("broker: subscription already exists")
	ErrNoSub          = errors.New("broker: no such subscription")
	ErrStaleToken     = errors.New("broker: stale or invalid delivery token")
	ErrNotOwner       = errors.New("broker: consumer is not the current owner")
	ErrAlreadyAcked   = errors.New("broker: message already acknowledged")
	ErrNoMessage      = errors.New("broker: no such message")
	ErrNotQuarantined = errors.New("broker: message is not quarantined")
)

// 默认重试策略。
const (
	defaultMaxFailures = 5
	defaultBackoffBase = time.Second
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

	failures    map[int64]int       // 每条消息累计失败（nack）次数
	retryAt     map[int64]time.Time // 退避截止时间，此前不再投递
	quarantined map[int64]bool      // 隔离集合：不再自动投递，不阻塞后续消息
	abandoned   map[int64]bool      // 放弃集合：人工明确放弃的终态，不等于确认
	maxFailures int
	backoffBase time.Duration

	partitions int               // 分区数（默认 1），分区按消息 Key 哈希归属
	grantSeq   uint64            // 临时授权序号
	grants     map[string]*Grant // 临时授权（交接）记录，含已关闭
}

// Counts 是订阅的消息数量核对视图，用于分别核对
// 已交付、已确认、隔离、放弃与仍待处理的消息数量。
type Counts struct {
	Total       int // 主题已发布的消息总数
	Delivered   int // 至少被投递过一次的消息数（按历史去重，含已确认/隔离/放弃）
	Acked       int // 已确认（业务完成）的消息数
	Quarantined int // 当前被隔离的消息数
	Abandoned   int // 人工明确放弃的消息数
	Pending     int // 仍待处理：未确认、未隔离、未放弃
}

// Broker 是消息投递运行系统本体。
type Broker struct {
	dir string

	mu      sync.Mutex
	topics  map[string][]Message
	subs    map[string]*subState // key: topic+"/"+name
	acks    []AckRecord          // 全量确认去向记录（持久化）
	history []HistoryEvent       // 全量消息历史（持久化）
	now     func() time.Time
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
		name:        name,
		topic:       topic,
		acked:       map[int64]bool{},
		inFlight:    map[int64]*inFlight{},
		failures:    map[int64]int{},
		retryAt:     map[int64]time.Time{},
		quarantined: map[int64]bool{},
		abandoned:   map[int64]bool{},
		maxFailures: defaultMaxFailures,
		backoffBase: defaultBackoffBase,
		partitions:  1,
		grants:      map[string]*Grant{},
	}
	return b.flushLocked()
}

// SetRetryPolicy 设置订阅的失败退避策略：maxFailures 次拒绝后隔离，
// 第 n 次失败后的退避时长为 backoffBase * 2^(n-1)。
func (b *Broker) SetRetryPolicy(topic, sub string, maxFailures int, backoffBase time.Duration) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return err
	}
	if maxFailures < 1 {
		maxFailures = 1
	}
	if backoffBase < 0 {
		backoffBase = 0
	}
	s.maxFailures = maxFailures
	s.backoffBase = backoffBase
	return b.flushLocked()
}

// Publish 持久化一条消息到主题，返回偏移。消息立即可见。
func (b *Broker) Publish(topic, key string, payload []byte) (int64, error) {
	return b.publish(topic, key, payload, 0)
}

// PublishDelayed 持久化一条延迟消息：delay 之后才对消费者可见。
func (b *Broker) PublishDelayed(topic, key string, payload []byte, delay time.Duration) (int64, error) {
	return b.publish(topic, key, payload, delay)
}

func (b *Broker) publish(topic, key string, payload []byte, delay time.Duration) (int64, error) {
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
	if delay > 0 {
		m.VisibleAt = m.Timestamp.Add(delay)
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
	reclaimed := false
	for off, f := range s.inFlight {
		if !f.deadline.After(now) {
			if err := b.recordHistoryLocked(s, off, EventTimeout, f.owner, f.epoch, s.failures[off]+1, ""); err != nil {
				return nil, err
			}
			delete(s.inFlight, off)
			reclaimed = true
		}
	}
	if reclaimed {
		if err := b.flushLocked(); err != nil {
			return nil, err
		}
	}

	var out []Delivery
	for off := int64(0); off < int64(len(msgs)) && len(out) < max; off++ {
		if s.acked[off] {
			continue
		}
		if s.quarantined[off] {
			continue // 隔离不阻塞同订阅后续可处理消息
		}
		if s.abandoned[off] {
			continue // 人工放弃的终态消息不再投递
		}
		if msgs[off].VisibleAt.After(now) {
			continue // 延迟投递尚未到期
		}
		if t, backing := s.retryAt[off]; backing && t.After(now) {
			continue // 失败退避中
		}
		if _, held := s.inFlight[off]; held {
			continue
		}
		g := b.activeGrantLocked(s, partitionOf(msgs[off].Key, s.partitions), now)
		if g != nil && consumerID != g.To {
			continue // 分区已临时授权他人处理，到期归还前原处理者与其他人都拉不到
		}
		s.epochSeq++
		f := &inFlight{owner: consumerID, epoch: s.epochSeq, deadline: now.Add(visibilityTimeout)}
		s.inFlight[off] = f
		attempt := s.failures[off] + 1
		detail := ""
		if g != nil {
			detail = "grant=" + g.ID + " reason=" + g.Reason
		}
		if err := b.recordHistoryLocked(s, off, EventDeliver, consumerID, f.epoch, attempt, detail); err != nil {
			return nil, err
		}
		out = append(out, Delivery{
			Message:  msgs[off],
			Token:    token(topic, sub, off, f.epoch),
			Epoch:    f.epoch,
			Owner:    consumerID,
			Deadline: f.deadline,
			Attempt:  attempt,
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
	delete(s.retryAt, off)
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
	if err := b.recordHistoryLocked(s, off, EventAck, consumerID, epoch, s.failures[off]+1, ""); err != nil {
		return err
	}
	return b.flushLocked()
}

// Nack 拒绝一次投递：Token 校验同 Ack。消息进入退避，
// 退避结束后可被重新投递；失败次数达到上限则被隔离，
// 隔离期间不阻塞同订阅后续可处理消息。
func (b *Broker) Nack(topic, sub, consumerID, tok string) error {
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
	delete(s.inFlight, off)
	s.failures[off]++
	now := b.now()
	backoff := s.backoffBase << (s.failures[off] - 1)
	s.retryAt[off] = now.Add(backoff)
	if err := b.recordHistoryLocked(s, off, EventNack, consumerID, epoch, s.failures[off], ""); err != nil {
		return err
	}
	if s.failures[off] >= s.maxFailures {
		s.quarantined[off] = true
		delete(s.retryAt, off)
		if err := b.recordHistoryLocked(s, off, EventQuarantine, consumerID, epoch, s.failures[off], ""); err != nil {
			return err
		}
	}
	return b.flushLocked()
}

// Redeliver 人工重新投递一条被隔离的消息：保留原消息身份（主题/偏移/内容），
// 清除隔离与失败计数，下一次 Pull 以新纪元、新尝试投递。操作者记入历史。
func (b *Broker) Redeliver(topic, sub string, offset int64, operator string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return err
	}
	if offset < 0 || offset >= int64(len(b.topics[topic])) {
		return ErrNoMessage
	}
	if s.acked[offset] {
		return ErrAlreadyAcked
	}
	if !s.quarantined[offset] {
		return ErrNotQuarantined
	}
	delete(s.quarantined, offset)
	delete(s.retryAt, offset)
	s.failures[offset] = 0
	if err := b.recordHistoryLocked(s, offset, EventRedeliver, operator, 0, 1, "manual redeliver"); err != nil {
		return err
	}
	return b.flushLocked()
}

// Quarantined 返回订阅当前被隔离的消息偏移（升序）。
func (b *Broker) Quarantined(topic, sub string) ([]int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return nil, err
	}
	var out []int64
	for off := range s.quarantined {
		out = append(out, off)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// Abandon 人工明确放弃一条被隔离的消息：终态，不再投递，
// 但不算业务确认（AckedBy 中不会出现）。操作者记入历史，
// 消费位置的推进可据此解释。
func (b *Broker) Abandon(topic, sub string, offset int64, operator string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return err
	}
	if offset < 0 || offset >= int64(len(b.topics[topic])) {
		return ErrNoMessage
	}
	if s.acked[offset] {
		return ErrAlreadyAcked
	}
	if !s.quarantined[offset] {
		return ErrNotQuarantined
	}
	delete(s.quarantined, offset)
	delete(s.retryAt, offset)
	s.abandoned[offset] = true
	if err := b.recordHistoryLocked(s, offset, EventAbandon, operator, 0, s.failures[offset], "manual abandon"); err != nil {
		return err
	}
	return b.flushLocked()
}

// Abandoned 返回订阅当前被人工放弃的消息偏移（升序）。
func (b *Broker) Abandoned(topic, sub string) ([]int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return nil, err
	}
	var out []int64
	for off := range s.abandoned {
		out = append(out, off)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// Counts 返回订阅的数量核对视图：已交付（按历史去重）、已确认、
// 隔离、放弃与仍待处理的消息数量，重启后依然可核对。
func (b *Broker) Counts(topic, sub string) (Counts, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return Counts{}, err
	}
	delivered := map[int64]bool{}
	for _, e := range b.history {
		if e.Topic == topic && e.Sub == sub && e.Event == EventDeliver {
			delivered[e.Offset] = true
		}
	}
	c := Counts{
		Total:       len(b.topics[topic]),
		Delivered:   len(delivered),
		Acked:       len(s.acked),
		Quarantined: len(s.quarantined),
		Abandoned:   len(s.abandoned),
	}
	c.Pending = c.Total - c.Acked - c.Quarantined - c.Abandoned
	return c, nil
}

// History 返回指定消息在订阅下的全部历史（按发生顺序）。
// offset 传负数则返回该订阅全部消息的历史。
func (b *Broker) History(topic, sub string, offset int64) ([]HistoryEvent, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, err := b.subLocked(topic, sub); err != nil {
		return nil, err
	}
	var out []HistoryEvent
	for _, e := range b.history {
		if e.Topic == topic && e.Sub == sub && (offset < 0 || e.Offset == offset) {
			out = append(out, e)
		}
	}
	return out, nil
}

// recordHistoryLocked 追加一条历史并落盘。调用方须持有锁。
func (b *Broker) recordHistoryLocked(s *subState, off int64, event, consumer string, epoch uint64, attempt int, detail string) error {
	e := HistoryEvent{
		Topic:    s.topic,
		Offset:   off,
		Sub:      s.name,
		Event:    event,
		Consumer: consumer,
		Epoch:    epoch,
		Attempt:  attempt,
		At:       b.now(),
		Detail:   detail,
	}
	b.history = append(b.history, e)
	return b.appendHistoryLocked(e)
}

// Position 返回订阅的连续确认位置：从 0 起连续已确认（业务完成）的前缀长度。
// 隔离或放弃不等于确认，不会推进该位置；空洞不会被掩盖。
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

// DeliverablePosition 返回订阅的连续已处理位置：从 0 起连续
// 已确认、已隔离或已放弃的前缀长度，即投递可以越过的位置。
// 它与 Position 的差值就是尚未业务确认完成的空洞，
// 可用 Quarantined / Abandoned / History 解释空洞的去向。
func (b *Broker) DeliverablePosition(topic, sub string) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return 0, err
	}
	pos := int64(0)
	for s.acked[pos] || s.quarantined[pos] || s.abandoned[pos] {
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
