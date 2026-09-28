package broker

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func setup(t *testing.T, dir string) *Broker {
	t.Helper()
	b, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := b.CreateTopic("orders"); err != nil && !errors.Is(err, ErrTopicExists) {
		t.Fatalf("create topic: %v", err)
	}
	if err := b.CreateSubscription("orders", "workers"); err != nil && !errors.Is(err, ErrSubExists) {
		t.Fatalf("create sub: %v", err)
	}
	return b
}

func publishN(t *testing.T, b *Broker, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := b.Publish("orders", fmt.Sprintf("k%d", i), []byte(fmt.Sprintf("payload-%d", i))); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
}

// 确认过的消息在普通重启后不能全部重来。
func TestAckedMessagesSurviveRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	publishN(t, b, 5)

	ds, err := b.Pull("orders", "workers", "c1", 3, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 3 {
		t.Fatalf("want 3 deliveries, got %d", len(ds))
	}
	for _, d := range ds {
		if err := b.Ack("orders", "workers", "c1", d.Token); err != nil {
			t.Fatalf("ack: %v", err)
		}
	}
	pos, _ := b.Position("orders", "workers")
	if pos != 3 {
		t.Fatalf("position = %d, want 3", pos)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启
	b2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	pos, _ = b2.Position("orders", "workers")
	if pos != 3 {
		t.Fatalf("after restart position = %d, want 3", pos)
	}
	ds, err = b2.Pull("orders", "workers", "c1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 2 || ds[0].Message.Offset != 3 || ds[1].Message.Offset != 4 {
		t.Fatalf("after restart got %+v, want offsets 3,4 only", ds)
	}
}

// 消费者处理中断（未确认即重启）后允许重新投递。
func TestCrashAllowsRedelivery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	publishN(t, b, 2)

	ds, err := b.Pull("orders", "workers", "c1", 2, time.Hour)
	if err != nil || len(ds) != 2 {
		t.Fatalf("pull: %v len=%d", err, len(ds))
	}
	// 模拟崩溃：不确认、不优雅关闭，直接丢弃实例。
	b2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ds2, err := b2.Pull("orders", "workers", "c2", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds2) != 2 {
		t.Fatalf("after crash want redelivery of 2, got %d", len(ds2))
	}
}

// 多消费者共享任务：一条消息同一时刻只有一个持有人。
func TestSingleOwnerWhileHeld(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	publishN(t, b, 4)

	ds, err := b.Pull("orders", "workers", "c1", 4, time.Hour)
	if err != nil || len(ds) != 4 {
		t.Fatalf("pull: %v len=%d", err, len(ds))
	}
	// c2 在可见期内拉不到任何消息。
	ds2, err := b.Pull("orders", "workers", "c2", 4, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds2) != 0 {
		t.Fatalf("c2 should get nothing while c1 holds all, got %d", len(ds2))
	}
	// c2 用 c1 的 token 确认也不行。
	if err := b.Ack("orders", "workers", "c2", ds[0].Token); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("want ErrNotOwner, got %v", err)
	}
}

// 超时转交后，旧持有人的确认不能误伤新持有人。
func TestTimeoutHandoverStaleAckRejected(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	publishN(t, b, 1)

	// 控制时钟，避免真实睡眠。
	now := time.Now()
	b.now = func() time.Time { return now }

	ds, err := b.Pull("orders", "workers", "c1", 1, 50*time.Millisecond)
	if err != nil || len(ds) != 1 {
		t.Fatalf("pull: %v len=%d", err, len(ds))
	}
	oldToken := ds[0].Token
	oldEpoch := ds[0].Epoch

	// 可见期过后，c2 接手。
	now = now.Add(time.Second)
	ds2, err := b.Pull("orders", "workers", "c2", 1, time.Hour)
	if err != nil || len(ds2) != 1 {
		t.Fatalf("handover pull: %v len=%d", err, len(ds2))
	}
	if ds2[0].Epoch <= oldEpoch {
		t.Fatalf("epoch must increase on handover: %d -> %d", oldEpoch, ds2[0].Epoch)
	}

	// 旧持有人迟到确认：必须被拒绝，且不影响新持有人。
	if err := b.Ack("orders", "workers", "c1", oldToken); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("stale ack: want ErrStaleToken, got %v", err)
	}
	if err := b.Ack("orders", "workers", "c2", ds2[0].Token); err != nil {
		t.Fatalf("new owner ack must succeed: %v", err)
	}
	pos, _ := b.Position("orders", "workers")
	if pos != 1 {
		t.Fatalf("position = %d, want 1", pos)
	}
	// 重复确认报错。
	if err := b.Ack("orders", "workers", "c2", ds2[0].Token); !errors.Is(err, ErrAlreadyAcked) {
		t.Fatalf("re-ack: want ErrAlreadyAcked, got %v", err)
	}
}

// 保存记录并重启，核对每条消息的实际去向。
func TestRestartVerifyDestinations(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	publishN(t, b, 6)

	now := time.Now()
	b.now = func() time.Time { return now }

	// c1 拿 3 条确认 2 条，第 3 条超时转给 c2。
	ds, _ := b.Pull("orders", "workers", "c1", 3, 50*time.Millisecond)
	for _, d := range ds[:2] {
		if err := b.Ack("orders", "workers", "c1", d.Token); err != nil {
			t.Fatal(err)
		}
	}
	now = now.Add(time.Second)
	ds2, _ := b.Pull("orders", "workers", "c2", 10, time.Hour)
	if len(ds2) != 4 { // 转交的 1 条 + 未投递的 3 条
		t.Fatalf("c2 want 4, got %d", len(ds2))
	}
	for _, d := range ds2 {
		if err := b.Ack("orders", "workers", "c2", d.Token); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启后核对：6 条全部有去向，c1=2 条（offset 0,1），c2=4 条（offset 2..5）。
	b2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	dest, err := b2.AckedBy("orders", "workers")
	if err != nil {
		t.Fatal(err)
	}
	if len(dest) != 6 {
		t.Fatalf("want 6 destinations, got %d: %v", len(dest), dest)
	}
	for off := int64(0); off < 2; off++ {
		if dest[off] != "c1" {
			t.Fatalf("offset %d want c1, got %q", off, dest[off])
		}
	}
	for off := int64(2); off < 6; off++ {
		if dest[off] != "c2" {
			t.Fatalf("offset %d want c2, got %q", off, dest[off])
		}
	}
	pos, _ := b2.Position("orders", "workers")
	if pos != 6 {
		t.Fatalf("position = %d, want 6", pos)
	}
	// 重启后无消息可再投递。
	ds3, _ := b2.Pull("orders", "workers", "c3", 10, time.Minute)
	if len(ds3) != 0 {
		t.Fatalf("nothing should remain, got %d", len(ds3))
	}
}

// 消息内容本身也要持久化。
func TestMessagePayloadPersisted(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	if _, err := b.Publish("orders", "k", []byte("hello")); err != nil {
		t.Fatal(err)
	}
	b.Close()

	b2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := b2.Pull("orders", "workers", "c1", 1, time.Minute)
	if err != nil || len(ds) != 1 {
		t.Fatalf("pull: %v len=%d", err, len(ds))
	}
	if string(ds[0].Message.Payload) != "hello" || ds[0].Message.Key != "k" {
		t.Fatalf("payload mismatch: %+v", ds[0].Message)
	}
}
