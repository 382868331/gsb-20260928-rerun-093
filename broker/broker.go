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
	GrantID  string
}

// 历史事件类型。
const (
	EventDeliver     = "deliver"    // 一次领取（投递给某消费者）
	EventTimeout     = "timeout"    // 可见期超时被回收
	EventNack        = "nack"       // 持有人明确拒绝（进入退避）
	EventAck         = "ack"        // 确认完成
	EventQuarantine  = "quarantine" // 失败次数达上限被隔离
	EventRedeliver   = "redeliver"  // 人工重新投递（保留原身份，开启新尝试）
	EventAbandon     = "abandon"    // 人工明确放弃（终态，不再投递，也不算确认）
	EventGrant       = "grant"      // 建立临时分区授权（可能尚未开始）
	EventGrantStart  = "grant_start"
	EventGrantCancel = "grant_cancel"
	EventGrantReturn = "grant_return"
	EventGrantRevoke = "grant_revoke"
)

// HistoryEvent 是消息历史中的一条记录：每一次领取、超时、拒绝、
// 确认、隔离与人工重投都会留下一条，最终确认者不会代替全部尝试记录。
type HistoryEvent struct {
	Topic        string    `json:"topic"`
	Offset       int64     `json:"offset"`
	Sub          string    `json:"sub"`
	Event        string    `json:"event"`
	Consumer     string    `json:"consumer"` // 投递/确认的消费者，或人工重投的操作者
	Epoch        uint64    `json:"epoch"`
	Attempt      int       `json:"attempt"`
	At           time.Time `json:"at"`
	Detail       string    `json:"detail,omitempty"`
	Partition    string    `json:"partition,omitempty"`
	GrantID      string    `json:"grant_id,omitempty"`
	Reason       string    `json:"reason,omitempty"`
	FromConsumer string    `json:"from_consumer,omitempty"`
	ToConsumer   string    `json:"to_consumer,omitempty"`
	Partitions   []string  `json:"partitions,omitempty"`
	StartsAt     time.Time `json:"starts_at,omitempty"`
	ExpiresAt    time.Time `json:"expires_at,omitempty"`
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
	ErrInvalidGrant   = errors.New("broker: invalid partition grant")
	ErrGrantNotFound  = errors.New("broker: partition grant not found")
	ErrGrantStarted   = errors.New("broker: partition grant already started")
	ErrGrantFinished  = errors.New("broker: partition grant already finished")
	ErrPartitionBusy  = errors.New("broker: partition has an overlapping grant")
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
	grantID  string
}

type HandoverMode string

const (
	// HandoverWait 保留旧持有人已领取消息的处理权，等待其自然确认或拒绝。
	HandoverWait HandoverMode = "wait"
	// HandoverRevoke 立即撤回旧处理权，旧令牌马上失效。
	HandoverRevoke HandoverMode = "revoke"
)

const (
	grantScheduled = "scheduled"
	grantActive    = "active"
	grantDraining  = "draining"
	grantCompleted = "completed"
	grantCanceled  = "canceled"
)

// PartitionGrant 描述一个有明确范围和期限的临时分区授权。
type PartitionGrant struct {
	ID           string       `json:"id"`
	Topic        string       `json:"topic"`
	Sub          string       `json:"sub"`
	Partitions   []string     `json:"partitions"`
	FromConsumer string       `json:"from_consumer"`
	ToConsumer   string       `json:"to_consumer"`
	Reason       string       `json:"reason"`
	Mode         HandoverMode `json:"mode"`
	CreatedAt    time.Time    `json:"created_at"`
	StartsAt     time.Time    `json:"starts_at"`
	ExpiresAt    time.Time    `json:"expires_at"`
	Status       string       `json:"status"`
	Seq          uint64       `json:"seq"`
}

// PartitionWork 是交接前的分区工作清单。
type PartitionWork struct {
	Partition   string
	InFlight    []int64 // 已领取但尚未确认/拒绝
	Retrying    []int64 // 退避后等待自动重投
	Quarantined []int64 // 已隔离，等待人工重投或放弃
	Unstarted   []int64 // 尚未进入处理流程的消息
}

// PartitionGrantRequest 请求把一组分区临时交给另一名逻辑消费者。
type PartitionGrantRequest struct {
	Partitions   []string
	FromConsumer string
	ToConsumer   string
	Reason       string
	Mode         HandoverMode
	StartsAt     time.Time
	ExpiresAt    time.Time
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
	grants      map[string]*PartitionGrant
	grantSeq    uint64
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
		grants:      map[string]*PartitionGrant{},
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
	if err := b.processGrantsAndFlushLocked(s, now); err != nil {
		return nil, err
	}

	// 回收超时在途消息：旧 Token 即刻失效（fencing）。
	reclaimed := false
	for off, f := range s.inFlight {
		if !f.deadline.After(now) {
			if err := b.recordMessageHistoryLocked(s, off, EventTimeout, f.owner, f.epoch, s.failures[off]+1, f.grantID, ""); err != nil {
				return nil, err
			}
			delete(s.inFlight, off)
			if err := b.completeDrainingGrantsLocked(s, []string{b.topics[topic][off].Key}, now); err != nil {
				return nil, err
			}
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
		if !b.canPullLocked(s, msgs[off].Key, consumerID, now) {
			continue
		}
		s.epochSeq++
		grantID := b.activeGrantIDLocked(s, msgs[off].Key, now)
		f := &inFlight{owner: consumerID, epoch: s.epochSeq, deadline: now.Add(visibilityTimeout), grantID: grantID}
		s.inFlight[off] = f
		attempt := s.failures[off] + 1
		if err := b.recordMessageHistoryLocked(s, off, EventDeliver, consumerID, f.epoch, attempt, grantID, ""); err != nil {
			return nil, err
		}
		out = append(out, Delivery{
			Message:  msgs[off],
			Token:    token(topic, sub, off, f.epoch),
			Epoch:    f.epoch,
			Owner:    consumerID,
			Deadline: f.deadline,
			Attempt:  attempt,
			GrantID:  grantID,
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
	if err := b.processGrantsAndFlushLocked(s, b.now()); err != nil {
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
	if err := b.recordMessageHistoryLocked(s, off, EventAck, consumerID, epoch, s.failures[off]+1, f.grantID, ""); err != nil {
		return err
	}
	partitions := []string{b.topics[topic][off].Key}
	if err := b.completeDrainingGrantsLocked(s, partitions, b.now()); err != nil {
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
	if err := b.processGrantsAndFlushLocked(s, b.now()); err != nil {
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
	if err := b.recordMessageHistoryLocked(s, off, EventNack, consumerID, epoch, s.failures[off], f.grantID, ""); err != nil {
		return err
	}
	if s.failures[off] >= s.maxFailures {
		s.quarantined[off] = true
		delete(s.retryAt, off)
		if err := b.recordMessageHistoryLocked(s, off, EventQuarantine, consumerID, epoch, s.failures[off], f.grantID, ""); err != nil {
			return err
		}
	}
	if err := b.completeDrainingGrantsLocked(s, []string{b.topics[topic][off].Key}, b.now()); err != nil {
		return err
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
	now := b.now()
	if err := b.processGrantsAndFlushLocked(s, now); err != nil {
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
	if err := b.recordMessageHistoryLocked(s, offset, EventRedeliver, operator, 0, 1, b.activeGrantIDLocked(s, b.topics[topic][offset].Key, now), "manual redeliver"); err != nil {
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
	now := b.now()
	if err := b.processGrantsAndFlushLocked(s, now); err != nil {
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
	if err := b.recordMessageHistoryLocked(s, offset, EventAbandon, operator, 0, s.failures[offset], b.activeGrantIDLocked(s, b.topics[topic][offset].Key, now), "manual abandon"); err != nil {
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

// PartitionWorkView 返回一组分区中尚未完成、等待重投和隔离的消息。
func (b *Broker) PartitionWorkView(topic, sub string, partitions []string) (map[string]PartitionWork, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return nil, err
	}
	now := b.now()
	if err := b.processGrantsAndFlushLocked(s, now); err != nil {
		return nil, err
	}
	wanted := map[string]bool{}
	for _, p := range partitions {
		if p == "" {
			return nil, ErrInvalidGrant
		}
		wanted[p] = true
	}
	out := map[string]PartitionWork{}
	for p := range wanted {
		out[p] = PartitionWork{Partition: p}
	}
	for off, m := range b.topics[topic] {
		off64 := int64(off)
		if !wanted[m.Key] || s.acked[off64] || s.abandoned[off64] {
			continue
		}
		w := out[m.Key]
		switch {
		case s.quarantined[off64]:
			w.Quarantined = append(w.Quarantined, off64)
		case s.inFlight[off64] != nil:
			w.InFlight = append(w.InFlight, off64)
		case !m.VisibleAt.After(now) && !s.retryAt[off64].After(now) && b.hasDeliveredLocked(topic, sub, off64):
			w.Retrying = append(w.Retrying, off64)
		case s.retryAt[off64].After(now):
			w.Retrying = append(w.Retrying, off64)
		default:
			w.Unstarted = append(w.Unstarted, off64)
		}
		out[m.Key] = w
	}
	for p, w := range out {
		sort.Slice(w.InFlight, func(i, j int) bool { return w.InFlight[i] < w.InFlight[j] })
		sort.Slice(w.Retrying, func(i, j int) bool { return w.Retrying[i] < w.Retrying[j] })
		sort.Slice(w.Quarantined, func(i, j int) bool { return w.Quarantined[i] < w.Quarantined[j] })
		sort.Slice(w.Unstarted, func(i, j int) bool { return w.Unstarted[i] < w.Unstarted[j] })
		out[p] = w
	}
	return out, nil
}

// CreatePartitionGrant 创建带明确范围、期限和交接原因的临时授权。
// StartsAt 在未来时安排尚未开始；立即开始时按 mode 决定等待旧处理或撤回旧权。
func (b *Broker) CreatePartitionGrant(topic, sub string, req PartitionGrantRequest) (*PartitionGrant, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return nil, err
	}
	now := b.now()
	parts, err := normalizePartitions(req.Partitions)
	if err != nil {
		return nil, err
	}
	if req.FromConsumer == "" || req.ToConsumer == "" || req.FromConsumer == req.ToConsumer {
		return nil, ErrInvalidGrant
	}
	if req.Mode != HandoverWait && req.Mode != HandoverRevoke {
		return nil, ErrInvalidGrant
	}
	if req.StartsAt.IsZero() {
		req.StartsAt = now
	}
	if req.ExpiresAt.IsZero() || !req.ExpiresAt.After(req.StartsAt) {
		return nil, ErrInvalidGrant
	}
	if req.StartsAt.Before(now) {
		req.StartsAt = now
	}
	if err := b.processGrantsAndFlushLocked(s, now); err != nil {
		return nil, err
	}
	if b.hasOverlappingGrantLocked(s, parts, req.StartsAt, req.ExpiresAt, "") {
		return nil, ErrPartitionBusy
	}
	s.grantSeq++
	g := &PartitionGrant{
		ID:           fmt.Sprintf("grant-%d", s.grantSeq),
		Topic:        topic,
		Sub:          sub,
		Partitions:   parts,
		FromConsumer: req.FromConsumer,
		ToConsumer:   req.ToConsumer,
		Reason:       req.Reason,
		Mode:         req.Mode,
		CreatedAt:    now,
		StartsAt:     req.StartsAt,
		ExpiresAt:    req.ExpiresAt,
		Status:       grantScheduled,
		Seq:          s.grantSeq,
	}
	s.grants[g.ID] = g
	if err := b.recordGrantLifecycleLocked(s, g, EventGrant, req.FromConsumer, "grant scheduled"); err != nil {
		return nil, err
	}
	if err := b.processGrantsAndFlushLocked(s, now); err != nil {
		return nil, err
	}
	return cloneGrant(g), b.flushLocked()
}

// CancelPartitionGrant 取消尚未开始的临时安排；已经开始的事实保留。
func (b *Broker) CancelPartitionGrant(topic, sub, grantID, operator string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return err
	}
	now := b.now()
	if err := b.processGrantsAndFlushLocked(s, now); err != nil {
		return err
	}
	g, err := b.grantLocked(s, topic, sub, grantID)
	if err != nil {
		return err
	}
	if g.Status != grantScheduled {
		return ErrGrantStarted
	}
	g.Status = grantCanceled
	if err := b.recordGrantLifecycleLocked(s, g, EventGrantCancel, operator, "scheduled grant canceled"); err != nil {
		return err
	}
	return b.flushLocked()
}

// ReturnPartitionGrant 提前归还。wait 等待临时消费者在途收尾；revoke 立即终止旧授权。
func (b *Broker) ReturnPartitionGrant(topic, sub, grantID, operator string, mode HandoverMode) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return err
	}
	now := b.now()
	if err := b.processGrantsAndFlushLocked(s, now); err != nil {
		return err
	}
	g, err := b.grantLocked(s, topic, sub, grantID)
	if err != nil {
		return err
	}
	if mode == "" {
		mode = g.Mode
	}
	if mode != HandoverWait && mode != HandoverRevoke {
		return ErrInvalidGrant
	}
	if g.Status != grantActive && g.Status != grantDraining {
		return ErrGrantFinished
	}
	g.ExpiresAt = now
	if mode == HandoverRevoke {
		if err := b.revokeGrantInflightLocked(s, g, now, EventGrantRevoke, operator, "grant force returned"); err != nil {
			return err
		}
		g.Status = grantCompleted
	} else {
		g.Status = grantDraining
		if err := b.recordGrantLifecycleLocked(s, g, EventGrantReturn, operator, "grant returned; waiting for in-flight work"); err != nil {
			return err
		}
	}
	if err := b.completeDrainingGrantsLocked(s, g.Partitions, now); err != nil {
		return err
	}
	return b.flushLocked()
}

// PartitionGrants 返回订阅的授权事实（按建立顺序），包含已取消和已完成记录。
func (b *Broker) PartitionGrants(topic, sub string) ([]PartitionGrant, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return nil, err
	}
	if err := b.processGrantsAndFlushLocked(s, b.now()); err != nil {
		return nil, err
	}
	var out []PartitionGrant
	for _, g := range s.grants {
		out = append(out, *cloneGrant(g))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
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
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return nil, err
	}
	if err := b.processGrantsAndFlushLocked(s, b.now()); err != nil {
		return nil, err
	}
	if offset >= 0 && offset >= int64(len(b.topics[topic])) {
		return nil, ErrNoMessage
	}
	var out []HistoryEvent
	for _, e := range b.history {
		if e.Topic == topic && e.Sub == sub && (offset < 0 || e.Offset == offset || (e.Offset < 0 && e.Partition == b.topics[topic][offset].Key)) {
			out = append(out, e)
		}
	}
	return out, nil
}

// recordMessageHistoryLocked 追加一条消息事件并落盘。调用方须持有锁。
func (b *Broker) recordMessageHistoryLocked(s *subState, off int64, event, consumer string, epoch uint64, attempt int, grantID, detail string) error {
	e := HistoryEvent{
		Topic:     s.topic,
		Offset:    off,
		Sub:       s.name,
		Event:     event,
		Consumer:  consumer,
		Epoch:     epoch,
		Attempt:   attempt,
		At:        b.now(),
		Detail:    detail,
		Partition: b.topics[s.topic][off].Key,
		GrantID:   grantID,
	}
	if g := s.grants[grantID]; g != nil {
		e.FromConsumer = g.FromConsumer
		e.ToConsumer = g.ToConsumer
		e.Reason = g.Reason
		e.Partitions = append([]string(nil), g.Partitions...)
		e.StartsAt = g.StartsAt
		e.ExpiresAt = g.ExpiresAt
	}
	b.history = append(b.history, e)
	return b.appendHistoryLocked(e)
}

func (b *Broker) recordGrantLifecycleLocked(s *subState, g *PartitionGrant, event, consumer, detail string) error {
	for _, p := range g.Partitions {
		e := HistoryEvent{
			Topic:        s.topic,
			Offset:       -1,
			Sub:          s.name,
			Event:        event,
			Consumer:     consumer,
			At:           b.now(),
			Detail:       detail,
			Partition:    p,
			GrantID:      g.ID,
			Reason:       g.Reason,
			FromConsumer: g.FromConsumer,
			ToConsumer:   g.ToConsumer,
			Partitions:   append([]string(nil), g.Partitions...),
			StartsAt:     g.StartsAt,
			ExpiresAt:    g.ExpiresAt,
		}
		b.history = append(b.history, e)
		if err := b.appendHistoryLocked(e); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) processGrantsLocked(s *subState, now time.Time) (bool, error) {
	changed := false
	var grants []*PartitionGrant
	for _, g := range s.grants {
		grants = append(grants, g)
	}
	sort.Slice(grants, func(i, j int) bool { return grants[i].Seq < grants[j].Seq })
	for _, g := range grants {
		switch g.Status {
		case grantScheduled:
			if g.StartsAt.After(now) {
				continue
			}
			g.Status = grantActive
			changed = true
			if g.Mode == HandoverRevoke {
				if err := b.revokeInflightLocked(s, g.Partitions, g.ToConsumer, "", g.ID, EventGrantRevoke, g.FromConsumer, now, "old processing authority revoked at grant start"); err != nil {
					return changed, err
				}
			}
			if err := b.recordGrantLifecycleLocked(s, g, EventGrantStart, g.ToConsumer, "temporary grant started"); err != nil {
				return changed, err
			}
		case grantActive:
			if g.ExpiresAt.After(now) {
				continue
			}
			g.Status = grantDraining
			changed = true
			if err := b.recordGrantLifecycleLocked(s, g, EventGrantReturn, g.FromConsumer, "grant expired; temporary authority returned"); err != nil {
				return changed, err
			}
		}
		if g.Status == grantDraining {
			done := true
			for off, f := range s.inFlight {
				if f.grantID == g.ID && containsString(g.Partitions, b.topics[s.topic][off].Key) {
					done = false
					break
				}
			}
			if done {
				g.Status = grantCompleted
				changed = true
			}
		}
	}
	return changed, nil
}

func (b *Broker) completeDrainingGrantsLocked(s *subState, partitions []string, now time.Time) error {
	for _, g := range s.grants {
		if g.Status != grantDraining || !anyString(g.Partitions, partitions) {
			continue
		}
		done := true
		for off, f := range s.inFlight {
			if f.grantID == g.ID && containsString(g.Partitions, b.topics[s.topic][off].Key) {
				done = false
				break
			}
		}
		if done {
			g.Status = grantCompleted
		}
	}
	return nil
}

func (b *Broker) revokeGrantInflightLocked(s *subState, g *PartitionGrant, now time.Time, event, operator, detail string) error {
	return b.revokeInflightLocked(s, g.Partitions, "", g.ID, g.ID, event, operator, now, detail)
}

func (b *Broker) revokeInflightLocked(s *subState, partitions []string, keepOwner, filterGrantID, eventGrantID string, event, operator string, now time.Time, detail string) error {
	for off, f := range s.inFlight {
		p := b.topics[s.topic][off].Key
		if !containsString(partitions, p) {
			continue
		}
		if keepOwner != "" && f.owner == keepOwner {
			continue
		}
		if filterGrantID != "" && f.grantID != filterGrantID {
			continue
		}
		if err := b.recordRevokeLocked(s, off, f, event, operator, eventGrantID, detail); err != nil {
			return err
		}
		delete(s.inFlight, off)
	}
	return nil
}

func (b *Broker) recordRevokeLocked(s *subState, off int64, f *inFlight, event, operator, grantID, detail string) error {
	e := HistoryEvent{
		Topic:     s.topic,
		Offset:    off,
		Sub:       s.name,
		Event:     event,
		Consumer:  operator,
		Epoch:     f.epoch,
		Attempt:   s.failures[off] + 1,
		At:        b.now(),
		Detail:    detail,
		Partition: b.topics[s.topic][off].Key,
		GrantID:   grantID,
	}
	if g := s.grants[grantID]; g != nil {
		e.FromConsumer = g.FromConsumer
		e.ToConsumer = g.ToConsumer
		e.Reason = g.Reason
		e.Partitions = append([]string(nil), g.Partitions...)
		e.StartsAt = g.StartsAt
		e.ExpiresAt = g.ExpiresAt
	}
	b.history = append(b.history, e)
	return b.appendHistoryLocked(e)
}

func (b *Broker) canPullLocked(s *subState, partition, consumer string, now time.Time) bool {
	for _, g := range s.grants {
		if !containsString(g.Partitions, partition) {
			continue
		}
		if g.Status == grantActive && !g.StartsAt.After(now) && g.ExpiresAt.After(now) && g.ToConsumer != consumer {
			return false
		}
		if g.Status == grantDraining && g.ToConsumer == consumer {
			return false
		}
	}
	return true
}

func (b *Broker) activeGrantIDLocked(s *subState, partition string, now time.Time) string {
	var active *PartitionGrant
	for _, g := range s.grants {
		if g.Status == grantActive && !g.StartsAt.After(now) && g.ExpiresAt.After(now) && containsString(g.Partitions, partition) {
			if active == nil || g.Seq > active.Seq {
				active = g
			}
		}
	}
	if active == nil {
		return ""
	}
	return active.ID
}

func (b *Broker) hasOverlappingGrantLocked(s *subState, parts []string, start, expire time.Time, exceptID string) bool {
	for _, g := range s.grants {
		if g.ID == exceptID || g.Status == grantCompleted || g.Status == grantCanceled {
			continue
		}
		if g.ExpiresAt.After(start) || g.StartsAt.After(expire) || !anyString(g.Partitions, parts) {
			continue
		}
		return true
	}
	return false
}

func (b *Broker) grantLocked(s *subState, topic, sub, id string) (*PartitionGrant, error) {
	g, ok := s.grants[id]
	if !ok || g.Topic != topic || g.Sub != sub {
		return nil, ErrGrantNotFound
	}
	return g, nil
}

func (b *Broker) hasDeliveredLocked(topic, sub string, off int64) bool {
	for _, e := range b.history {
		if e.Topic == topic && e.Sub == sub && e.Offset == off && e.Event == EventDeliver {
			return true
		}
	}
	return false
}

func normalizePartitions(parts []string) ([]string, error) {
	if len(parts) == 0 {
		return nil, ErrInvalidGrant
	}
	seen := map[string]bool{}
	var out []string
	for _, p := range parts {
		if p == "" || seen[p] {
			continue
		}
		seen[p] = true
		out = append(out, p)
	}
	if len(out) == 0 {
		return nil, ErrInvalidGrant
	}
	sort.Strings(out)
	return out, nil
}

func cloneGrant(g *PartitionGrant) *PartitionGrant {
	if g == nil {
		return nil
	}
	c := *g
	c.Partitions = append([]string(nil), g.Partitions...)
	return &c
}

func containsString(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}

func anyString(xs, ys []string) bool {
	for _, y := range ys {
		if containsString(xs, y) {
			return true
		}
	}
	return false
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
