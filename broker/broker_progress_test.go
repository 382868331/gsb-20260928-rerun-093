package broker

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// 隔离不等于确认：连续确认位置与可投递位置分开表示，
// 人工重投或明确放弃后，两个位置的推进各有依据、可由历史解释。
func TestPositionSplitQuarantineRedeliverAbandon(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	if err := b.SetRetryPolicy("orders", "workers", 1, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	b.now = func() time.Time { return now }
	publishN(t, b, 3)

	// offset 0 失败一次即隔离；1,2 正常确认。
	ds, _ := b.Pull("orders", "workers", "c1", 1, time.Hour)
	if err := b.Nack("orders", "workers", "c1", ds[0].Token); err != nil {
		t.Fatal(err)
	}
	ds, _ = b.Pull("orders", "workers", "c1", 10, time.Hour)
	for _, d := range ds {
		if err := b.Ack("orders", "workers", "c1", d.Token); err != nil {
			t.Fatal(err)
		}
	}
	// 隔离的 0 号是未完成的空洞：确认位置停在 0，可投递位置为 3。
	pos, _ := b.Position("orders", "workers")
	if pos != 0 {
		t.Fatalf("position = %d, want 0 (hole at quarantined offset 0)", pos)
	}
	dpos, _ := b.DeliverablePosition("orders", "workers")
	if dpos != 3 {
		t.Fatalf("deliverable = %d, want 3", dpos)
	}

	// 人工重投 0 号：可投递位置回退到 0（它重新变成待处理）。
	if err := b.Redeliver("orders", "workers", 0, "ops"); err != nil {
		t.Fatal(err)
	}
	dpos, _ = b.DeliverablePosition("orders", "workers")
	if dpos != 0 {
		t.Fatalf("deliverable after redeliver = %d, want 0 (offset 0 pending again)", dpos)
	}
	// 重投后再次失败：重新隔离，不得产生任何确认。
	ds, _ = b.Pull("orders", "workers", "c2", 1, time.Hour)
	if len(ds) != 1 || ds[0].Message.Offset != 0 {
		t.Fatalf("redelivery: %+v", ds)
	}
	failedRedeliverToken := ds[0].Token
	if err := b.Nack("orders", "workers", "c2", failedRedeliverToken); err != nil {
		t.Fatal(err)
	}
	c, _ := b.Counts("orders", "workers")
	if c.Acked != 2 || c.Quarantined != 1 || c.Pending != 0 {
		t.Fatalf("counts after failed redeliver = %+v", c)
	}
	// 重投失败的旧令牌迟到：拒绝，不产生确认。
	if err := b.Ack("orders", "workers", "c2", failedRedeliverToken); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("late ack of failed redeliver: want ErrStaleToken, got %v", err)
	}
	_ = now

	// 人工明确放弃：可投递位置推进到 3，确认位置仍为 0（放弃不等于确认）。
	if err := b.Abandon("orders", "workers", 0, "ops-lead"); err != nil {
		t.Fatal(err)
	}
	pos, _ = b.Position("orders", "workers")
	if pos != 0 {
		t.Fatalf("position after abandon = %d, want 0 (abandon is not ack)", pos)
	}
	dpos, _ = b.DeliverablePosition("orders", "workers")
	if dpos != 3 {
		t.Fatalf("deliverable after abandon = %d, want 3", dpos)
	}
	// 放弃后不再投递，也不能重投或重复放弃。
	ds, _ = b.Pull("orders", "workers", "c3", 10, time.Hour)
	if len(ds) != 0 {
		t.Fatalf("abandoned message must not be delivered, got %+v", ds)
	}
	if err := b.Redeliver("orders", "workers", 0, "ops"); !errors.Is(err, ErrNotQuarantined) {
		t.Fatalf("redeliver abandoned: want ErrNotQuarantined, got %v", err)
	}
	if err := b.Abandon("orders", "workers", 0, "ops"); !errors.Is(err, ErrNotQuarantined) {
		t.Fatalf("re-abandon: want ErrNotQuarantined, got %v", err)
	}
	// 已确认的消息不能放弃。
	if err := b.Abandon("orders", "workers", 1, "ops"); !errors.Is(err, ErrAlreadyAcked) {
		t.Fatalf("abandon acked: want ErrAlreadyAcked, got %v", err)
	}

	// 历史可解释每一步位置变化。
	h, err := b.History("orders", "workers", 0)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{EventDeliver, EventNack, EventQuarantine, EventRedeliver, EventDeliver, EventNack, EventQuarantine, EventAbandon}
	if len(h) != len(want) {
		t.Fatalf("history = %+v", h)
	}
	for i, e := range h {
		if e.Event != want[i] {
			t.Fatalf("history[%d] = %s, want %s (full: %+v)", i, e.Event, want[i], h)
		}
	}
	if h[len(h)-1].Consumer != "ops-lead" {
		t.Fatalf("abandon operator = %q", h[len(h)-1].Consumer)
	}
	// 去向核对：放弃不算确认。
	dest, _ := b.AckedBy("orders", "workers")
	if _, ok := dest[0]; ok {
		t.Fatalf("abandoned offset 0 must not appear in AckedBy: %v", dest)
	}
	c, _ = b.Counts("orders", "workers")
	wantC := Counts{Total: 3, Delivered: 3, Acked: 2, Quarantined: 0, Abandoned: 1, Pending: 0}
	if c != wantC {
		t.Fatalf("counts = %+v, want %+v", c, wantC)
	}

	// 重启后：放弃状态、位置与数量核对保持不变。
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	pos, _ = b2.Position("orders", "workers")
	dpos, _ = b2.DeliverablePosition("orders", "workers")
	if pos != 0 || dpos != 3 {
		t.Fatalf("after restart position = %d, deliverable = %d; want 0/3", pos, dpos)
	}
	ab, _ := b2.Abandoned("orders", "workers")
	if len(ab) != 1 || ab[0] != 0 {
		t.Fatalf("abandoned after restart = %v", ab)
	}
	c2, _ := b2.Counts("orders", "workers")
	if c2 != wantC {
		t.Fatalf("counts after restart = %+v, want %+v", c2, wantC)
	}
	h2, _ := b2.History("orders", "workers", 0)
	if len(h2) != len(want) {
		t.Fatalf("history after restart = %+v", h2)
	}
}

// 重投成功：确认位置越过原隔离点连续推进，旧令牌迟到不重复确认。
func TestRedeliverSuccessAdvancesPosition(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	if err := b.SetRetryPolicy("orders", "workers", 1, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	publishN(t, b, 2)

	ds, _ := b.Pull("orders", "workers", "c1", 2, time.Hour)
	stale := ds[0].Token
	if err := b.Nack("orders", "workers", "c1", stale); err != nil {
		t.Fatal(err)
	}
	if err := b.Ack("orders", "workers", "c1", ds[1].Token); err != nil {
		t.Fatal(err)
	}
	pos, _ := b.Position("orders", "workers")
	if pos != 0 {
		t.Fatalf("position = %d, want 0 (offset 0 quarantined)", pos)
	}
	if err := b.Redeliver("orders", "workers", 0, "ops"); err != nil {
		t.Fatal(err)
	}
	ds2, _ := b.Pull("orders", "workers", "c2", 1, time.Hour)
	if len(ds2) != 1 || ds2[0].Message.Offset != 0 {
		t.Fatalf("redelivery: %+v", ds2)
	}
	if err := b.Ack("orders", "workers", "c2", ds2[0].Token); err != nil {
		t.Fatal(err)
	}
	// 旧令牌迟到：拒绝，不重复确认。
	if err := b.Ack("orders", "workers", "c1", stale); !errors.Is(err, ErrAlreadyAcked) {
		t.Fatalf("late stale ack: want ErrAlreadyAcked, got %v", err)
	}
	pos, _ = b.Position("orders", "workers")
	if pos != 2 {
		t.Fatalf("position after redeliver+ack = %d, want 2", pos)
	}
	c, _ := b.Counts("orders", "workers")
	want := Counts{Total: 2, Delivered: 2, Acked: 2, Quarantined: 0, Abandoned: 0, Pending: 0}
	if c != want {
		t.Fatalf("counts = %+v, want %+v", c, want)
	}
	if got := len(b.AckRecords()); got != 2 {
		t.Fatalf("ack records = %d, want 2 (no duplicate ack)", got)
	}
}
