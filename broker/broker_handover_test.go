package broker

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

// 交接预览列出在途、退避等待与隔离消息，并给出交接前后的消费位置。
func TestHandoverPreviewListsPending(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	publishN(t, b, 6)
	if err := b.SetRetryPolicy("orders", "workers", 2, time.Hour); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	b.now = func() time.Time { return now }

	// 0,1 在途；2 退避等待重投；3 隔离；4,5 未投递。
	ds, _ := b.Pull("orders", "workers", "c1", 4, 100*time.Hour)
	if len(ds) != 4 {
		t.Fatalf("pull: %d", len(ds))
	}
	// offset 3 失败两次被隔离。
	if err := b.Nack("orders", "workers", "c1", ds[3].Token); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour) // 跳过退避
	d3, err := b.Pull("orders", "workers", "c1", 1, time.Hour)
	if err != nil || len(d3) != 1 || d3[0].Message.Offset != 3 {
		t.Fatalf("re-pull for quarantine: %v %+v", err, d3)
	}
	if err := b.Nack("orders", "workers", "c1", d3[0].Token); err != nil {
		t.Fatal(err)
	}
	// offset 2 失败一次，进入退避等待重投。
	if err := b.Nack("orders", "workers", "c1", ds[2].Token); err != nil {
		t.Fatal(err)
	}
	if q, _ := b.Quarantined("orders", "workers"); len(q) != 1 || q[0] != 3 {
		t.Fatalf("quarantined = %v, want [3]", q)
	}

	p, err := b.HandoverPreview("orders", "workers", "c2")
	if err != nil {
		t.Fatal(err)
	}
	if p.From != "" || p.To != "c2" {
		t.Fatalf("from/to = %q/%q", p.From, p.To)
	}
	if fmt.Sprint(p.InFlight) != "[0 1]" {
		t.Fatalf("in-flight = %v, want [0 1]", p.InFlight)
	}
	if fmt.Sprint(p.Retrying) != "[2]" {
		t.Fatalf("retrying = %v, want [2]", p.Retrying)
	}
	if fmt.Sprint(p.Quarantined) != "[3]" {
		t.Fatalf("quarantined = %v, want [3]", p.Quarantined)
	}
	if p.PositionBefore != 0 || p.PositionAfter != p.PositionBefore {
		t.Fatalf("positions = %d/%d", p.PositionBefore, p.PositionAfter)
	}
	if p.DeliverableBefore != 0 || p.DeliverableAfter != p.DeliverableBefore {
		t.Fatalf("deliverable = %d/%d", p.DeliverableBefore, p.DeliverableAfter)
	}
	// 预览不改变状态：c1 的在途消息仍可确认。
	if err := b.Ack("orders", "workers", "c1", ds[0].Token); err != nil {
		t.Fatalf("preview must not change state: %v", err)
	}
}

// 撤回式交接：旧消费者迟到的确认与重投请求被明确拒绝，新消费者接手。
func TestHandoverRevokeRejectsStaleConsumer(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	publishN(t, b, 3)

	ds, _ := b.Pull("orders", "workers", "c1", 2, time.Hour)
	oldToken := ds[0].Token

	p, err := b.Handover("orders", "workers", "c2", HandoverOptions{Reason: "c1 失联"})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(p.InFlight) != "[0 1]" {
		t.Fatalf("preview in-flight = %v", p.InFlight)
	}
	if owner, _ := b.Owner("orders", "workers"); owner != "c2" {
		t.Fatalf("owner = %q, want c2", owner)
	}

	// 旧消费者迟到的确认与重投（nack）请求必须被拒绝。
	if err := b.Ack("orders", "workers", "c1", oldToken); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("late ack: want ErrStaleToken, got %v", err)
	}
	if err := b.Nack("orders", "workers", "c1", oldToken); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("late nack: want ErrStaleToken, got %v", err)
	}
	// 旧消费者也不能再拉取该分区。
	if _, err := b.Pull("orders", "workers", "c1", 1, time.Hour); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("old owner pull: want ErrNotOwner, got %v", err)
	}

	// 新消费者接手全部未完成消息并确认。
	ds2, err := b.Pull("orders", "workers", "c2", 10, time.Hour)
	if err != nil || len(ds2) != 3 {
		t.Fatalf("c2 pull: %v len=%d", err, len(ds2))
	}
	for _, d := range ds2 {
		if err := b.Ack("orders", "workers", "c2", d.Token); err != nil {
			t.Fatal(err)
		}
	}
	dest, _ := b.AckedBy("orders", "workers")
	for off := int64(0); off < 3; off++ {
		if dest[off] != "c2" {
			t.Fatalf("offset %d acked by %q, want c2", off, dest[off])
		}
	}
	if pos, _ := b.Position("orders", "workers"); pos != 3 {
		t.Fatalf("position = %d, want 3", pos)
	}
}

// 等待式交接：原消费者完成当前处理前不确认交接，完成后才移交。
func TestHandoverWaitForCurrentProcessing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	publishN(t, b, 2)

	ds, _ := b.Pull("orders", "workers", "c1", 1, time.Hour)
	if _, err := b.Handover("orders", "workers", "c2", HandoverOptions{Wait: true}); !errors.Is(err, ErrHandoverPending) {
		t.Fatalf("want ErrHandoverPending, got %v", err)
	}
	if owner, _ := b.Owner("orders", "workers"); owner != "" {
		t.Fatalf("owner changed too early: %q", owner)
	}
	// 等待期间原消费者的处理权未被撤回。
	if err := b.Ack("orders", "workers", "c1", ds[0].Token); err != nil {
		t.Fatalf("in-flight ack during wait: %v", err)
	}
	// 原消费者完成后交接确认。
	if _, err := b.Handover("orders", "workers", "c2", HandoverOptions{Wait: true, Reason: "轮班"}); err != nil {
		t.Fatalf("handover after drain: %v", err)
	}
	if owner, _ := b.Owner("orders", "workers"); owner != "c2" {
		t.Fatalf("owner = %q, want c2", owner)
	}
	ds2, err := b.Pull("orders", "workers", "c2", 10, time.Hour)
	if err != nil || len(ds2) != 1 || ds2[0].Message.Offset != 1 {
		t.Fatalf("c2 pull: %v %+v", err, ds2)
	}
}

// 交接不抹平隔离形成的空洞：顺序与连续确认位置保持不变。
func TestHandoverPreservesOrderAndQuarantineGap(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	publishN(t, b, 4)
	if err := b.SetRetryPolicy("orders", "workers", 1, time.Hour); err != nil {
		t.Fatal(err)
	}

	// 隔离 offset 0，确认 1，留下 2,3 未处理。
	ds, _ := b.Pull("orders", "workers", "c1", 2, time.Hour)
	if err := b.Nack("orders", "workers", "c1", ds[0].Token); err != nil {
		t.Fatal(err)
	}
	if err := b.Ack("orders", "workers", "c1", ds[1].Token); err != nil {
		t.Fatal(err)
	}
	posBefore, _ := b.Position("orders", "workers")
	dposBefore, _ := b.DeliverablePosition("orders", "workers")
	if posBefore != 0 || dposBefore != 2 {
		t.Fatalf("before: pos=%d dpos=%d, want 0/2", posBefore, dposBefore)
	}

	if _, err := b.Handover("orders", "workers", "c2", HandoverOptions{}); err != nil {
		t.Fatal(err)
	}
	posAfter, _ := b.Position("orders", "workers")
	dposAfter, _ := b.DeliverablePosition("orders", "workers")
	if posAfter != posBefore || dposAfter != dposBefore {
		t.Fatalf("handover moved positions: %d/%d -> %d/%d", posBefore, dposBefore, posAfter, dposAfter)
	}
	if q, _ := b.Quarantined("orders", "workers"); len(q) != 1 || q[0] != 0 {
		t.Fatalf("quarantine gap erased: %v", q)
	}
	// 新消费者按序接手，隔离消息不被自动重投。
	ds2, _ := b.Pull("orders", "workers", "c2", 10, time.Hour)
	if len(ds2) != 2 || ds2[0].Message.Offset != 2 || ds2[1].Message.Offset != 3 {
		t.Fatalf("c2 got %+v, want offsets 2,3 in order", ds2)
	}
}

// 只交接所选分区，未选分区继续由原消费者处理。
func TestHandoverOnlySelectedPartition(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	if err := b.CreateSubscription("orders", "auditors"); err != nil {
		t.Fatal(err)
	}
	publishN(t, b, 2)

	if _, err := b.Handover("orders", "workers", "c2", HandoverOptions{}); err != nil {
		t.Fatal(err)
	}
	// 未交接的分区不受影响。
	if owner, _ := b.Owner("orders", "auditors"); owner != "" {
		t.Fatalf("auditors owner = %q, want unassigned", owner)
	}
	ds, err := b.Pull("orders", "auditors", "c1", 10, time.Hour)
	if err != nil || len(ds) != 2 {
		t.Fatalf("auditors pull: %v len=%d", err, len(ds))
	}
	if _, err := b.Pull("orders", "workers", "c1", 1, time.Hour); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("workers pull by c1: want ErrNotOwner, got %v", err)
	}
}

// 重复交接幂等；交接后重启，归属与处理记录保持一致，旧令牌依然被拒。
func TestHandoverIdempotentAcrossRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	publishN(t, b, 2)

	ds, _ := b.Pull("orders", "workers", "c1", 1, time.Hour)
	oldToken := ds[0].Token
	if _, err := b.Handover("orders", "workers", "c2", HandoverOptions{Reason: "扩容"}); err != nil {
		t.Fatal(err)
	}
	h1, _ := b.History("orders", "workers", -1)
	// 重复交接同一目标：幂等，不重复记录历史。
	if _, err := b.Handover("orders", "workers", "c2", HandoverOptions{Reason: "扩容"}); err != nil {
		t.Fatal(err)
	}
	h2, _ := b.History("orders", "workers", -1)
	if len(h1) != len(h2) {
		t.Fatalf("duplicate handover recorded history: %d -> %d", len(h1), len(h2))
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	// 重新打开后归属一致，旧消费者令牌与拉取仍被拒绝。
	b2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if owner, _ := b2.Owner("orders", "workers"); owner != "c2" {
		t.Fatalf("after restart owner = %q, want c2", owner)
	}
	if err := b2.Ack("orders", "workers", "c1", oldToken); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("late ack after restart: want ErrStaleToken, got %v", err)
	}
	if _, err := b2.Pull("orders", "workers", "c1", 1, time.Hour); !errors.Is(err, ErrNotOwner) {
		t.Fatalf("old owner pull after restart: want ErrNotOwner, got %v", err)
	}
	ds2, err := b2.Pull("orders", "workers", "c2", 10, time.Hour)
	if err != nil || len(ds2) != 2 {
		t.Fatalf("c2 pull after restart: %v len=%d", err, len(ds2))
	}
}

// 沿一条消息可查看每次处理所属消费者、交接原因与最终处置。
func TestHandoverHistoryPerMessage(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	publishN(t, b, 1)

	if _, err := b.Pull("orders", "workers", "c1", 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Handover("orders", "workers", "c2", HandoverOptions{Reason: "c1 维护"}); err != nil {
		t.Fatal(err)
	}
	ds2, _ := b.Pull("orders", "workers", "c2", 1, time.Hour)
	if err := b.Ack("orders", "workers", "c2", ds2[0].Token); err != nil {
		t.Fatal(err)
	}
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
	var handoverBy, handoverDetail string
	for _, e := range h {
		kinds = append(kinds, e.Event+"@"+e.Consumer)
		if e.Event == EventHandover {
			handoverBy = e.Consumer
			handoverDetail = e.Detail
		}
	}
	want := []string{"deliver@c1", "handover@c2", "deliver@c2", "ack@c2"}
	if fmt.Sprint(kinds) != fmt.Sprint(want) {
		t.Fatalf("history = %v, want %v", kinds, want)
	}
	if handoverBy != "c2" || handoverDetail == "" {
		t.Fatalf("handover event = %q %q", handoverBy, handoverDetail)
	}
	// 最终处置：由 c2 确认。
	dest, _ := b2.AckedBy("orders", "workers")
	if dest[0] != "c2" {
		t.Fatalf("final disposition = %q, want c2", dest[0])
	}
}
