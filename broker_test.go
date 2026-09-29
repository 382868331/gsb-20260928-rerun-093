package delivery

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestPersistentAckSurvivesRestartAndInterruptRedelivers(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")

	b, err := Open(dir, WithLeaseDuration(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CreateTopic("orders"); err != nil {
		t.Fatal(err)
	}
	if err := b.Subscribe("orders", "workers", SubscribeOptions{Position: StartAtEarliest}); err != nil {
		t.Fatal(err)
	}
	acked, err := b.Publish("orders", "done")
	if err != nil {
		t.Fatal(err)
	}
	interrupted, err := b.Publish("orders", "retry")
	if err != nil {
		t.Fatal(err)
	}

	first, err := b.Receive(context.Background(), "orders", "workers", "c1")
	if err != nil {
		t.Fatal(err)
	}
	if first.Seq != acked.Seq || first.Attempt != 1 {
		t.Fatalf("unexpected first delivery: %+v", first)
	}
	if err := b.Ack("orders", "workers", first.ConsumerID, first.Token, first.Seq); err != nil {
		t.Fatal(err)
	}
	second, err := b.Receive(context.Background(), "orders", "workers", "c1")
	if err != nil {
		t.Fatal(err)
	}
	if second.Seq != interrupted.Seq || second.Attempt != 1 {
		t.Fatalf("unexpected interrupted delivery: %+v", second)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b, err = Open(dir, WithLeaseDuration(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })

	redelivery, err := b.Receive(context.Background(), "orders", "workers", "c2")
	if err != nil {
		t.Fatal(err)
	}
	if redelivery.Seq != interrupted.Seq || redelivery.Attempt != 2 || redelivery.ConsumerID != "c2" {
		t.Fatalf("interrupted message was not redelivered: %+v", redelivery)
	}
	if err := b.Ack("orders", "workers", redelivery.ConsumerID, redelivery.Token, redelivery.Seq); err != nil {
		t.Fatal(err)
	}
	states := b.Snapshot().Topics[0].Subscriptions[0].Messages
	if len(states) != 2 || states[0].Status != statusAcked || states[1].Status != statusAcked {
		t.Fatalf("both messages should be acked: %+v", states)
	}
}

func TestExclusiveLeaseAndStaleAckAfterTimeout(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b, err := Open(dir, WithLeaseDuration(40*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.CreateTopic("jobs"); err != nil {
		t.Fatal(err)
	}
	if err := b.Subscribe("jobs", "shared", SubscribeOptions{Position: StartAtEarliest}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish("jobs", "only job"); err != nil {
		t.Fatal(err)
	}

	oldHolder, err := b.Receive(context.Background(), "jobs", "shared", "old")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	newHolder, err := b.Receive(ctx, "jobs", "shared", "new")
	if err != nil {
		t.Fatal(err)
	}
	if newHolder.Seq != oldHolder.Seq || newHolder.Token == oldHolder.Token || newHolder.ConsumerID != "new" || newHolder.Attempt != 2 {
		t.Fatalf("timeout did not transfer the lease: old=%+v new=%+v", oldHolder, newHolder)
	}

	if err := b.Ack("jobs", "shared", oldHolder.ConsumerID, oldHolder.Token, oldHolder.Seq); !errors.Is(err, ErrInvalidDelivery) {
		t.Fatalf("old ack should be rejected, got %v", err)
	}
	if err := b.Ack("jobs", "shared", newHolder.ConsumerID, newHolder.Token, newHolder.Seq); err != nil {
		t.Fatalf("new holder should ack: %v", err)
	}
	state := b.Snapshot().Topics[0].Subscriptions[0].Messages[0]
	if state.Status != statusAcked || len(state.Deliveries) != 2 || state.Deliveries[0].ConsumerID != "old" || state.Deliveries[1].ConsumerID != "new" {
		t.Fatalf("unexpected delivery trail: %+v", state)
	}
}

func TestOnlyOneCurrentHolderForSharedMessage(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b, err := Open(dir, WithLeaseDuration(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = b.Close() })
	_ = b.CreateTopic("queue")
	_ = b.Subscribe("queue", "shared", SubscribeOptions{Position: StartAtEarliest})
	result := make(chan Delivery, 1)
	consumers := []string{"a", "b"}
	for _, consumer := range consumers {
		go func(id string) {
			d, recvErr := b.Receive(context.Background(), "queue", "shared", id)
			if recvErr == nil {
				select {
				case result <- d:
				default:
				}
			}
		}(consumer)
	}
	time.Sleep(50 * time.Millisecond)
	_, _ = b.Publish("queue", "one")
	select {
	case d := <-result:
		if d.Seq != 1 || (d.ConsumerID != "a" && d.ConsumerID != "b") {
			t.Fatalf("message was delivered to an invalid consumer: %+v", d)
		}
		first := d
		if err := b.Ack("queue", "shared", d.ConsumerID, d.Token, d.Seq); err != nil {
			t.Fatal(err)
		}
		state := b.Snapshot().Topics[0].Subscriptions[0].Messages[0]
		if state.Status != statusAcked || state.Attempt != 1 || len(state.Deliveries) != 1 {
			t.Fatalf("message should have exactly one delivery trail: %+v", state)
		}
		_ = first
	case <-time.After(time.Second):
		t.Fatal("waiting consumer did not receive message")
	}
}
