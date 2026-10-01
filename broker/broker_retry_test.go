package broker

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// 延迟投递：到期前不可见，到期后正常投递。
func TestDelayedDelivery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	now := time.Now()
	b.now = func() time.Time { return now }

	if _, err := b.PublishDelayed("orders", "k", []byte("later"), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish("orders", "k2", []byte("now")); err != nil {
		t.Fatal(err)
	}
	ds, err := b.Pull("orders", "workers", "c1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 1 || ds[0].Message.Offset != 1 {
		t.Fatalf("delayed message must be invisible, got %+v", ds)
	}
	if err := b.Ack("orders", "workers", "c1", ds[0].Token); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute)
	ds, err = b.Pull("orders", "workers", "c2", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 1 || ds[0].Message.Offset != 0 || string(ds[0].Message.Payload) != "later" {
		t.Fatalf("after delay want offset 0, got %+v", ds)
	}
}

// 失败退避：Nack 后退避期内不再投递，退避结束后以新尝试重新投递。
func TestNackBackoffAndRetry(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	if err := b.SetRetryPolicy("orders", "workers", 10, 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	b.now = func() time.Time { return now }
	publishN(t, b, 1)

	ds, _ := b.Pull("orders", "workers", "c1", 1, time.Hour)
	if len(ds) != 1 || ds[0].Attempt != 1 {
		t.Fatalf("first delivery: %+v", ds)
	}
	if err := b.Nack("orders", "workers", "c1", ds[0].Token); err != nil {
		t.Fatal(err)
	}
	// 退避期内拉不到。
	now = now.Add(50 * time.Millisecond)
	ds, _ = b.Pull("orders", "workers", "c1", 1, time.Hour)
	if len(ds) != 0 {
		t.Fatalf("backoff should hide message, got %+v", ds)
	}
	// 退避结束后重新投递，Attempt 递增。
	now = now.Add(100 * time.Millisecond)
	ds, _ = b.Pull("orders", "workers", "c1", 1, time.Hour)
	if len(ds) != 1 || ds[0].Attempt != 2 {
		t.Fatalf("retry after backoff: %+v", ds)
	}
	// 非持有人不能 Nack。
	if err := b.Nack("orders", "workers", "c2", ds[0].Token); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("nack by non-owner: want ErrNotOwner, got %v", err)
	}
	if err := b.Ack("orders", "workers", "c1", ds[0].Token); err != nil {
		t.Fatal(err)
	}
}

// 多次失败后隔离：不阻塞后续消息，消费位置可越过隔离点，
// 但隔离消息不被误认成已确认。
func TestQuarantineDoesNotBlock(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	if err := b.SetRetryPolicy("orders", "workers", 2, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	b.now = func() time.Time { return now }
	publishN(t, b, 3)

	// offset 0 连续失败两次 -> 隔离。
	for i := 0; i < 2; i++ {
		ds, _ := b.Pull("orders", "workers", "c1", 1, time.Hour)
		if len(ds) != 1 || ds[0].Message.Offset != 0 {
			t.Fatalf("round %d: %+v", i, ds)
		}
		if err := b.Nack("orders", "workers", "c1", ds[0].Token); err != nil {
			t.Fatal(err)
		}
		now = now.Add(time.Second)
	}
	q, err := b.Quarantined("orders", "workers")
	if err != nil || len(q) != 1 || q[0] != 0 {
		t.Fatalf("quarantined = %v, err = %v", q, err)
	}
	// 后续消息照常投递并确认。
	ds, _ := b.Pull("orders", "workers", "c2", 10, time.Hour)
	if len(ds) != 2 || ds[0].Message.Offset != 1 || ds[1].Message.Offset != 2 {
		t.Fatalf("quarantine must not block later messages, got %+v", ds)
	}
	for _, d := range ds {
		if err := b.Ack("orders", "workers", "c2", d.Token); err != nil {
			t.Fatal(err)
		}
	}
	// 两个位置分开表示：连续确认位置停在 0（隔离不等于业务确认），
	// 可投递位置为 3（隔离视为已处理，投递可越过）。
	pos, _ := b.Position("orders", "workers")
	if pos != 0 {
		t.Fatalf("position = %d, want 0 (quarantine is not ack)", pos)
	}
	dpos, _ := b.DeliverablePosition("orders", "workers")
	if dpos != 3 {
		t.Fatalf("deliverable position = %d, want 3 (quarantine counts as resolved)", dpos)
	}
	// 隔离消息没有确认去向，不会被误认成已完成。
	dest, _ := b.AckedBy("orders", "workers")
	if _, ok := dest[0]; ok {
		t.Fatalf("quarantined offset 0 must not appear in AckedBy: %v", dest)
	}
	// 数量核对：3 条全部交付过，2 条确认，1 条隔离，无待处理。
	c, err := b.Counts("orders", "workers")
	if err != nil {
		t.Fatal(err)
	}
	want := Counts{Total: 3, Delivered: 3, Acked: 2, Quarantined: 1, Abandoned: 0, Pending: 0}
	if c != want {
		t.Fatalf("counts = %+v, want %+v", c, want)
	}
}

// 人工重投：保留原消息身份（偏移/内容），以新纪元新尝试投递，历史完整。
func TestManualRedeliver(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	if err := b.SetRetryPolicy("orders", "workers", 1, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	publishN(t, b, 1)

	ds, _ := b.Pull("orders", "workers", "c1", 1, time.Hour)
	if err := b.Nack("orders", "workers", "c1", ds[0].Token); err != nil {
		t.Fatal(err)
	}
	q, _ := b.Quarantined("orders", "workers")
	if len(q) != 1 {
		t.Fatalf("want quarantined, got %v", q)
	}
	// 未隔离的消息不能人工重投。
	if err := b.Redeliver("orders", "workers", 5, "ops"); !errors.Is(err, ErrNoMessage) {
		t.Fatalf("redeliver missing: want ErrNoMessage, got %v", err)
	}
	if err := b.Redeliver("orders", "workers", 0, "ops-oncall"); err != nil {
		t.Fatal(err)
	}
	q, _ = b.Quarantined("orders", "workers")
	if len(q) != 0 {
		t.Fatalf("quarantine should be cleared, got %v", q)
	}
	ds2, _ := b.Pull("orders", "workers", "c2", 1, time.Hour)
	if len(ds2) != 1 {
		t.Fatalf("redelivered message must be pullable, got %+v", ds2)
	}
	d := ds2[0]
	if d.Message.Offset != 0 || string(d.Message.Payload) != "payload-0" || d.Epoch <= ds[0].Epoch {
		t.Fatalf("identity/epoch wrong: %+v (first epoch %d)", d, ds[0].Epoch)
	}
	if err := b.Ack("orders", "workers", "c2", d.Token); err != nil {
		t.Fatal(err)
	}
	// 历史：deliver, nack, quarantine, redeliver, deliver, ack 全在。
	h, err := b.History("orders", "workers", 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{EventDeliver, EventNack, EventQuarantine, EventRedeliver, EventDeliver, EventAck}
	if len(h) != len(want) {
		t.Fatalf("history = %+v", h)
	}
	for i, e := range h {
		if e.Event != want[i] {
			t.Fatalf("history[%d] = %s, want %s (full: %+v)", i, e.Event, want[i], h)
		}
	}
	if h[3].Consumer != "ops-oncall" {
		t.Fatalf("redeliver operator = %q", h[3].Consumer)
	}
	// 最终确认者不代替全部尝试：AckedBy 只有 c2，历史里 c1 的尝试仍在。
	dest, _ := b.AckedBy("orders", "workers")
	if dest[0] != "c2" {
		t.Fatalf("AckedBy = %v", dest)
	}
}

// 历史持久化：重启后每一次领取/超时/拒绝/确认仍然可查。
func TestHistorySurvivesRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	now := time.Now()
	b.now = func() time.Time { return now }
	publishN(t, b, 1)

	ds, _ := b.Pull("orders", "workers", "c1", 1, 50*time.Millisecond)
	now = now.Add(time.Second) // 超时回收
	ds2, _ := b.Pull("orders", "workers", "c2", 1, time.Hour)
	if err := b.Nack("orders", "workers", "c2", ds2[0].Token); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	ds3, _ := b.Pull("orders", "workers", "c3", 1, time.Hour)
	if err := b.Ack("orders", "workers", "c3", ds3[0].Token); err != nil {
		t.Fatal(err)
	}
	_ = ds
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	h, err := b2.History("orders", "workers", 0)
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, e := range h {
		kinds = append(kinds, e.Event)
	}
	want := []string{EventDeliver, EventTimeout, EventDeliver, EventNack, EventDeliver, EventAck}
	if len(kinds) != len(want) {
		t.Fatalf("history after restart = %v", kinds)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("history after restart = %v, want %v", kinds, want)
		}
	}
}

// 确认边界重启 + 消费者换人 + 旧令牌迟到：
// 投递顺序与连续消费位置必须正确，跳过失败消息不能误认前序完成。
func TestRestartAtAckBoundaryWithStaleToken(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	if err := b.SetRetryPolicy("orders", "workers", 10, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	b.now = func() time.Time { return now }
	publishN(t, b, 4)

	// c1 拿 0,1,2，确认 0,1，2 不确认（崩溃在确认边界）。
	ds, _ := b.Pull("orders", "workers", "c1", 3, time.Hour)
	for _, d := range ds[:2] {
		if err := b.Ack("orders", "workers", "c1", d.Token); err != nil {
			t.Fatal(err)
		}
	}
	stale := ds[2].Token
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：未确认的 2 号允许重投，位置停在 2。
	b2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	b2.now = func() time.Time { return now }
	pos, _ := b2.Position("orders", "workers")
	if pos != 2 {
		t.Fatalf("position after restart = %d, want 2", pos)
	}
	// c2 接手，按序拿到 2,3。
	ds2, _ := b2.Pull("orders", "workers", "c2", 10, time.Hour)
	if len(ds2) != 2 || ds2[0].Message.Offset != 2 || ds2[1].Message.Offset != 3 {
		t.Fatalf("c2 got %+v, want offsets 2,3 in order", ds2)
	}
	// c1 旧令牌迟到：拒绝，且不影响位置。
	if err := b2.Ack("orders", "workers", "c1", stale); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("stale ack: want ErrStaleToken, got %v", err)
	}
	// c2 确认 3 号但 2 号 Nack：位置仍停在 2，不能因跳过失败消息误认前序完成。
	if err := b2.Ack("orders", "workers", "c2", ds2[1].Token); err != nil {
		t.Fatal(err)
	}
	if err := b2.Nack("orders", "workers", "c2", ds2[0].Token); err != nil {
		t.Fatal(err)
	}
	pos, _ = b2.Position("orders", "workers")
	if pos != 2 {
		t.Fatalf("position with failed offset 2 = %d, want 2", pos)
	}
	// 退避过后 2 号重投并确认，位置连续推进到 4。
	now = now.Add(time.Second)
	ds3, _ := b2.Pull("orders", "workers", "c3", 10, time.Hour)
	if len(ds3) != 1 || ds3[0].Message.Offset != 2 {
		t.Fatalf("redelivery got %+v, want offset 2 only", ds3)
	}
	if err := b2.Ack("orders", "workers", "c3", ds3[0].Token); err != nil {
		t.Fatal(err)
	}
	pos, _ = b2.Position("orders", "workers")
	if pos != 4 {
		t.Fatalf("final position = %d, want 4", pos)
	}
	// 去向核对：0,1->c1，2->c3，3->c2。
	dest, _ := b2.AckedBy("orders", "workers")
	want := map[int64]string{0: "c1", 1: "c1", 2: "c3", 3: "c2"}
	for off, who := range want {
		if dest[off] != who {
			t.Fatalf("dest[%d] = %q, want %q (%v)", off, dest[off], who, dest)
		}
	}
}
