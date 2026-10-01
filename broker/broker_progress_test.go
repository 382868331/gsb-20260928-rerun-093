package broker

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// 隔离只是隔离，不是业务确认：第一个消息被隔离、后续消息被确认后，
// 连续确认位置（AckPosition）与消费位置（Position）都必须停在空洞前，
// 只有“可继续投递后续消息”的 DeliverPosition 越过它。
func TestQuarantineHoleKeepsPositionsApart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	if err := b.SetRetryPolicy("orders", "workers", 1, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	b.now = func() time.Time { return now }
	publishN(t, b, 3)

	// offset 0 失败一次 -> 隔离。
	ds, _ := b.Pull("orders", "workers", "c1", 1, time.Hour)
	if err := b.Nack("orders", "workers", "c1", ds[0].Token); err != nil {
		t.Fatal(err)
	}
	// 后续消息照常投递并确认。
	now = now.Add(time.Second)
	ds, _ = b.Pull("orders", "workers", "c2", 10, time.Hour)
	if len(ds) != 2 || ds[0].Message.Offset != 1 || ds[1].Message.Offset != 2 {
		t.Fatalf("got %+v, want offsets 1,2", ds)
	}
	for _, d := range ds {
		if err := b.Ack("orders", "workers", "c2", d.Token); err != nil {
			t.Fatal(err)
		}
	}

	pos, _ := b.Position("orders", "workers")
	ackPos, _ := b.AckPosition("orders", "workers")
	dpos, _ := b.DeliverPosition("orders", "workers")
	if pos != 0 || ackPos != 0 {
		t.Fatalf("quarantined hole: position=%d ackPosition=%d, want 0/0", pos, ackPos)
	}
	if dpos != 3 {
		t.Fatalf("deliver position=%d, want 3 (later messages already deliverable)", dpos)
	}

	p, err := b.Progress("orders", "workers")
	if err != nil {
		t.Fatal(err)
	}
	if p.Total != 3 || p.Acked != 2 || p.Quarantined != 1 || p.Abandoned != 0 || p.Pending != 0 {
		t.Fatalf("progress = %+v", p)
	}
	if p.Acked+p.Quarantined+p.Abandoned+p.Pending != p.Total {
		t.Fatalf("counts must partition total: %+v", p)
	}
	if p.Delivered != 3 {
		t.Fatalf("delivered = %d, want 3", p.Delivered)
	}
}

// 人工明确放弃被隔离消息：消费位置依据放弃事件越过它，但连续确认位置不动；
// 放弃后永不重投，且放弃与确认严格区分。
func TestAbandonAdvancesPositionButNotAckPosition(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	if err := b.SetRetryPolicy("orders", "workers", 1, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	b.now = func() time.Time { return now }
	publishN(t, b, 2)

	ds, _ := b.Pull("orders", "workers", "c1", 1, time.Hour)
	if err := b.Nack("orders", "workers", "c1", ds[0].Token); err != nil {
		t.Fatal(err)
	}
	// 未隔离的消息不能放弃。
	if err := b.Abandon("orders", "workers", 1, "ops", "x"); !errors.Is(err, ErrNotQuarantined) {
		t.Fatalf("abandon non-quarantined: want ErrNotQuarantined, got %v", err)
	}
	now = now.Add(time.Second)
	ds2, _ := b.Pull("orders", "workers", "c2", 10, time.Hour)
	if len(ds2) != 1 || ds2[0].Message.Offset != 1 {
		t.Fatalf("got %+v, want offset 1", ds2)
	}
	if err := b.Ack("orders", "workers", "c2", ds2[0].Token); err != nil {
		t.Fatal(err)
	}

	// 放弃前：空洞仍在。
	if pos, _ := b.Position("orders", "workers"); pos != 0 {
		t.Fatalf("position before abandon = %d, want 0", pos)
	}
	if err := b.Abandon("orders", "workers", 0, "ops-oncall", "poison message"); err != nil {
		t.Fatal(err)
	}
	// 放弃后：消费位置推进到 2，依据是 abandon 历史事件；连续确认位置仍为 0。
	if pos, _ := b.Position("orders", "workers"); pos != 2 {
		t.Fatalf("position after abandon = %d, want 2", pos)
	}
	if ackPos, _ := b.AckPosition("orders", "workers"); ackPos != 0 {
		t.Fatalf("ack position after abandon = %d, want 0 (abandon is not an ack)", ackPos)
	}
	ab, _ := b.Abandoned("orders", "workers")
	if len(ab) != 1 || ab[0] != 0 {
		t.Fatalf("abandoned = %v", ab)
	}
	dest, _ := b.AckedBy("orders", "workers")
	if _, ok := dest[0]; ok {
		t.Fatalf("abandoned message must not appear in AckedBy: %v", dest)
	}
	// 已放弃的消息不能再人工重投，Pull 也永远拿不到。
	if err := b.Redeliver("orders", "workers", 0, "ops"); !errors.Is(err, ErrNotQuarantined) {
		t.Fatalf("redeliver abandoned: want ErrNotQuarantined, got %v", err)
	}
	if got, _ := b.Pull("orders", "workers", "c3", 10, time.Hour); len(got) != 0 {
		t.Fatalf("abandoned message must never redeliver, got %+v", got)
	}
	h, _ := b.History("orders", "workers", 0)
	last := h[len(h)-1]
	if last.Event != EventAbandon || last.Consumer != "ops-oncall" || last.Detail != "poison message" {
		t.Fatalf("abandon history = %+v", last)
	}

	p, _ := b.Progress("orders", "workers")
	if p.Total != 2 || p.Acked != 1 || p.Quarantined != 0 || p.Abandoned != 1 || p.Pending != 0 {
		t.Fatalf("progress = %+v", p)
	}
}

// 尝试序号跨超时、退避与人工重投连续累计，重投后不会从 1 重来；
// 历史保存每一次尝试，最终只确认一次。
func TestAttemptNumberingSurvivesRedeliver(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	if err := b.SetRetryPolicy("orders", "workers", 1, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	b.now = func() time.Time { return now }
	publishN(t, b, 1)

	d1, _ := b.Pull("orders", "workers", "c1", 1, 50*time.Millisecond)
	if d1[0].Attempt != 1 {
		t.Fatalf("attempt1 = %d", d1[0].Attempt)
	}
	now = now.Add(time.Second) // 超时回收
	d2, _ := b.Pull("orders", "workers", "c2", 1, time.Hour)
	if d2[0].Attempt != 2 {
		t.Fatalf("attempt2 after timeout = %d, want 2", d2[0].Attempt)
	}
	if err := b.Nack("orders", "workers", "c2", d2[0].Token); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	// 第二次失败直接达隔离上限（maxFailures=1 已在首次 nack 触发隔离；
	// 此时消息已隔离，无法再次 pull），改为人工重投。
	q, _ := b.Quarantined("orders", "workers")
	if len(q) != 1 || q[0] != 0 {
		t.Fatalf("quarantined = %v", q)
	}
	if err := b.Redeliver("orders", "workers", 0, "ops"); err != nil {
		t.Fatal(err)
	}
	d3, _ := b.Pull("orders", "workers", "c3", 1, time.Hour)
	if len(d3) != 1 || d3[0].Attempt != 3 {
		t.Fatalf("redelivered attempt = %d, want 3 (history continues, never reset)", d3[0].Attempt)
	}
	tok := d3[0].Token

	// 重投后的这一次处理失败（崩溃/超时），重启再次投递：旧令牌不得重复确认。
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	b2.now = func() time.Time { return now.Add(time.Hour) }
	if err := b2.Ack("orders", "workers", "c3", tok); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("token from before restart: want ErrStaleToken, got %v", err)
	}
	d4, _ := b2.Pull("orders", "workers", "c4", 1, time.Hour)
	if len(d4) != 1 || d4[0].Attempt != 4 {
		t.Fatalf("attempt4 after restart = %+v, want attempt 4", d4)
	}
	if err := b2.Ack("orders", "workers", "c4", d4[0].Token); err != nil {
		t.Fatal(err)
	}
	// 任何迟到令牌再确认都只能得到 already-acked，绝不产生第二条确认记录。
	if err := b2.Ack("orders", "workers", "c3", tok); !errors.Is(err, ErrAlreadyAcked) {
		t.Fatalf("late stale ack after completion: want ErrAlreadyAcked, got %v", err)
	}
	if recs := b2.AckRecords(); len(recs) != 1 {
		t.Fatalf("exactly one ack record expected, got %d", len(recs))
	}

	// 完整历史：deliver,timeout,deliver,nack,quarantine,redeliver,deliver,deliver,ack
	h, _ := b2.History("orders", "workers", 0)
	want := []string{
		EventDeliver, EventTimeout, EventDeliver, EventNack, EventQuarantine,
		EventRedeliver, EventDeliver, EventDeliver, EventAck,
	}
	if len(h) != len(want) {
		t.Fatalf("history = %+v", h)
	}
	wantAttempt := []int{1, 1, 2, 2, 2, 2, 3, 4, 4}
	for i := range want {
		if h[i].Attempt != wantAttempt[i] {
			t.Fatalf("attempt at history[%d] = %d, want %d (%+v)", i, h[i].Attempt, wantAttempt[i], h)
		}
	}
	for i, e := range h {
		if e.Event != want[i] {
			t.Fatalf("history[%d]=%s want %s (%+v)", i, e.Event, want[i], h)
		}
	}

	p, _ := b2.Progress("orders", "workers")
	if p.Total != 1 || p.Delivered != 1 || p.Acked != 1 || p.Quarantined != 0 || p.Abandoned != 0 || p.Pending != 0 {
		t.Fatalf("progress = %+v", p)
	}
}
