package delivery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	statusAvailable = "available"
	statusLeased    = "leased"
	statusAcked     = "acked"
)

var (
	ErrNotFound           = errors.New("delivery: topic or subscription not found")
	ErrAlreadyExists      = errors.New("delivery: topic already exists")
	ErrInvalidDelivery    = errors.New("delivery: delivery token is stale or invalid")
	ErrBrokerClosed       = errors.New("delivery: broker is closed")
	ErrEmptyConsumer      = errors.New("delivery: consumer id is empty")
	ErrInvalidStartOffset = errors.New("delivery: start offset is invalid")
)

type StartPosition int

const (
	StartAtLatest StartPosition = iota
	StartAtEarliest
	StartAtOffset
)

type SubscribeOptions struct {
	Position StartPosition
	Offset   int64
}

type Message struct {
	ID     string    `json:"id"`
	Seq    int64     `json:"seq"`
	Body   string    `json:"body"`
	Topic  string    `json:"topic"`
	SentAt time.Time `json:"sent_at"`
}

type Delivery struct {
	Message
	Subscription string    `json:"subscription"`
	ConsumerID   string    `json:"consumer_id"`
	Token        string    `json:"token"`
	Attempt      int       `json:"attempt"`
	LeasedUntil  time.Time `json:"leased_until"`
}

type DeliveryRecord struct {
	Seq         int64     `json:"seq"`
	ConsumerID  string    `json:"consumer_id"`
	Token       string    `json:"token"`
	Attempt     int       `json:"attempt"`
	LeasedAt    time.Time `json:"leased_at"`
	LeasedUntil time.Time `json:"leased_until"`
}

type MessageState struct {
	Seq         int64            `json:"seq"`
	Status      string           `json:"status"`
	ConsumerID  string           `json:"consumer_id,omitempty"`
	Token       string           `json:"token,omitempty"`
	Attempt     int              `json:"attempt"`
	LeasedUntil time.Time        `json:"leased_until,omitempty"`
	AckedAt     time.Time        `json:"acked_at,omitempty"`
	Deliveries  []DeliveryRecord `json:"deliveries,omitempty"`
}

type Broker struct {
	mu     sync.Mutex
	store  *Store
	cond   *sync.Cond
	lease  time.Duration
	closed bool
	stop   chan struct{}
	wg     sync.WaitGroup
}

type Option func(*Broker)

func WithLeaseDuration(d time.Duration) Option {
	return func(b *Broker) { b.lease = d }
}

func Open(dir string, opts ...Option) (*Broker, error) {
	store, err := openStore(dir)
	if err != nil {
		return nil, err
	}
	b := &Broker{store: store, lease: 30 * time.Second, stop: make(chan struct{})}
	for _, opt := range opts {
		opt(b)
	}
	if b.lease <= 0 {
		return nil, fmt.Errorf("delivery: lease duration must be positive")
	}
	b.cond = sync.NewCond(&b.mu)
	if err := b.recoverInterruptedLeases(); err != nil {
		return nil, err
	}
	b.wg.Add(1)
	go b.reapExpiredLeases()
	return b, nil
}

func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.stop)
	b.cond.Broadcast()
	b.mu.Unlock()
	b.wg.Wait()
	return b.store.Close()
}

func (b *Broker) CreateTopic(topic string) error {
	if topic == "" {
		return fmt.Errorf("delivery: topic is empty")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.store.data.Topics[topic] != nil {
		return ErrAlreadyExists
	}
	b.store.data.Topics[topic] = &TopicState{Name: topic}
	return b.store.saveLocked()
}

func (b *Broker) Subscribe(topic, subscription string, opts SubscribeOptions) error {
	if subscription == "" {
		return fmt.Errorf("delivery: subscription is empty")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.store.data.Topics[topic]
	if !ok {
		return ErrNotFound
	}
	subs, ok := b.store.data.Subscriptions[topic]
	if !ok {
		subs = map[string]*SubscriptionState{}
		b.store.data.Subscriptions[topic] = subs
	}
	if _, exists := subs[subscription]; exists {
		return nil
	}
	start := int64(len(t.Messages)) + 1
	switch opts.Position {
	case StartAtLatest:
	case StartAtEarliest:
		start = 1
	case StartAtOffset:
		if opts.Offset < 1 {
			return ErrInvalidStartOffset
		}
		start = opts.Offset
	}
	if start < 1 || start > int64(len(t.Messages))+1 {
		return ErrInvalidStartOffset
	}
	state := &SubscriptionState{Name: subscription, NextSeq: start, Items: map[int64]*MessageState{}}
	for seq := start; seq <= int64(len(t.Messages)); seq++ {
		state.Items[seq] = &MessageState{Seq: seq, Status: statusAvailable}
	}
	subs[subscription] = state
	b.cond.Broadcast()
	return b.store.saveLocked()
}

func (b *Broker) Publish(topic, body string) (Message, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	t, ok := b.store.data.Topics[topic]
	if !ok {
		return Message{}, ErrNotFound
	}
	seq := int64(len(t.Messages)) + 1
	msg := Message{ID: fmt.Sprintf("%s-%020d", topic, seq), Seq: seq, Body: body, Topic: topic, SentAt: b.store.now()}
	t.Messages = append(t.Messages, msg)
	for _, sub := range b.store.data.Subscriptions[topic] {
		if seq >= sub.NextSeq {
			sub.Items[seq] = &MessageState{Seq: seq, Status: statusAvailable}
		}
	}
	b.cond.Broadcast()
	err := b.store.saveLocked()
	return msg, err
}

func (b *Broker) Receive(ctx context.Context, topic, subscription, consumerID string) (Delivery, error) {
	if strings.TrimSpace(consumerID) == "" {
		return Delivery{}, ErrEmptyConsumer
	}
	if err := ctx.Err(); err != nil {
		return Delivery{}, err
	}
	ctxDone := make(chan struct{})
	defer close(ctxDone)
	go func() {
		select {
		case <-ctx.Done():
			b.mu.Lock()
			b.cond.Broadcast()
			b.mu.Unlock()
		case <-ctxDone:
		case <-b.stop:
		}
	}()

	b.mu.Lock()
	defer b.mu.Unlock()
	for {
		if b.closed {
			return Delivery{}, ErrBrokerClosed
		}
		if err := ctx.Err(); err != nil {
			return Delivery{}, err
		}
		sub, err := b.subscriptionLocked(topic, subscription)
		if err != nil {
			return Delivery{}, err
		}
		_ = b.reclaimExpiredLocked(b.store.now())
		if d, ok := b.claimLocked(topic, subscription, consumerID, sub); ok {
			return d, b.store.saveLocked()
		}
		b.cond.Wait()
	}
}

func (b *Broker) Ack(topic, subscription, consumerID, token string, seq int64) error {
	if token == "" {
		return ErrInvalidDelivery
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	sub, err := b.subscriptionLocked(topic, subscription)
	if err != nil {
		return err
	}
	state := sub.Items[seq]
	if state == nil || state.Status != statusLeased || state.ConsumerID != consumerID || state.Token != token {
		return ErrInvalidDelivery
	}
	state.Status = statusAcked
	state.AckedAt = b.store.now()
	state.ConsumerID = ""
	state.Token = ""
	state.LeasedUntil = time.Time{}
	b.cond.Broadcast()
	return b.store.saveLocked()
}

func (b *Broker) Snapshot() Snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()
	_ = b.reclaimExpiredLocked(b.store.now())
	return b.store.snapshot()
}

func (b *Broker) subscriptionLocked(topic, subscription string) (*SubscriptionState, error) {
	if b.store.data.Topics[topic] == nil {
		return nil, ErrNotFound
	}
	subs := b.store.data.Subscriptions[topic]
	if subs == nil {
		return nil, ErrNotFound
	}
	sub := subs[subscription]
	if sub == nil {
		return nil, ErrNotFound
	}
	return sub, nil
}

func (b *Broker) claimLocked(topic, subscription, consumerID string, sub *SubscriptionState) (Delivery, bool) {
	var target *MessageState
	var seq int64 = 1<<63 - 1
	for next, state := range sub.Items {
		if state.Status == statusAvailable && next < seq {
			target = state
			seq = next
		}
	}
	if target == nil {
		return Delivery{}, false
	}
	target.Attempt++
	target.Status = statusLeased
	target.ConsumerID = consumerID
	target.Token = b.store.newToken()
	now := b.store.now()
	target.LeasedUntil = now.Add(b.lease)
	target.Deliveries = append(target.Deliveries, DeliveryRecord{
		Seq: target.Seq, ConsumerID: consumerID, Token: target.Token, Attempt: target.Attempt,
		LeasedAt: now, LeasedUntil: target.LeasedUntil,
	})
	msg := b.store.data.Topics[topic].Messages[target.Seq-1]
	return Delivery{
		Message: msg, Subscription: subscription, ConsumerID: consumerID, Token: target.Token,
		Attempt: target.Attempt, LeasedUntil: target.LeasedUntil,
	}, true
}

func (b *Broker) reclaimExpiredLocked(now time.Time) bool {
	changed := false
	for _, subs := range b.store.data.Subscriptions {
		for _, sub := range subs {
			for _, state := range sub.Items {
				if state.Status == statusLeased && !state.LeasedUntil.After(now) {
					state.Status = statusAvailable
					state.ConsumerID = ""
					state.Token = ""
					state.LeasedUntil = time.Time{}
					changed = true
				}
			}
		}
	}
	if changed {
		b.cond.Broadcast()
	}
	return changed
}

func (b *Broker) recoverInterruptedLeases() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	changed := false
	for _, subs := range b.store.data.Subscriptions {
		for _, sub := range subs {
			for _, state := range sub.Items {
				if state.Status != statusLeased {
					continue
				}
				state.Status = statusAvailable
				state.ConsumerID = ""
				state.Token = ""
				state.LeasedUntil = time.Time{}
				changed = true
			}
		}
	}
	if changed {
		b.cond.Broadcast()
		return b.store.saveLocked()
	}
	return nil
}

func (b *Broker) reapExpiredLeases() {
	defer b.wg.Done()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			return
		case <-ticker.C:
			b.mu.Lock()
			if b.reclaimExpiredLocked(b.store.now()) {
				_ = b.store.saveLocked()
			}
			b.mu.Unlock()
		}
	}
}
