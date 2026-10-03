package broker

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func publishPartition(t *testing.T, b *Broker, partition string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, err := b.Publish("orders", partition, []byte(partition)); err != nil {
			t.Fatalf("publish %s/%d: %v", partition, i, err)
		}
	}
}

func eventKinds(events []HistoryEvent) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, e.Event)
	}
	return out
}

func TestPartitionGrantWaitExpireAndAdjacentGrant(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	if err := b.SetRetryPolicy("orders", "workers", 10, time.Minute); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0)
	b.now = func() time.Time { return now }
	publishPartition(t, b, "p-a", 1)
	publishPartition(t, b, "p-b", 1)

	old, err := b.Pull("orders", "workers", "old", 10, time.Hour)
	if err != nil || len(old) != 2 {
		t.Fatalf("old pull: %v len=%d", err, len(old))
	}
	pA := old[0]
	pB := old[1]
	if pA.Message.Key != "p-a" || pB.Message.Key != "p-b" {
		t.Fatalf("unexpected deliveries: %+v %+v", pA, pB)
	}

	work, err := b.PartitionWorkView("orders", "workers", []string{"p-a"})
	if err != nil {
		t.Fatal(err)
	}
	if got := work["p-a"].InFlight; len(got) != 1 || got[0] != 0 {
		t.Fatalf("work in-flight = %v", got)
	}

	g1, err := b.CreatePartitionGrant("orders", "workers", PartitionGrantRequest{
		Partitions:   []string{"p-a"},
		FromConsumer: "old",
		ToConsumer:   "temp",
		Reason:       "backlog handover",
		Mode:         HandoverWait,
		StartsAt:     now,
		ExpiresAt:    now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if g1.Status != grantActive {
		t.Fatalf("g1 status = %s", g1.Status)
	}
	if blocked, err := b.Pull("orders", "workers", "old", 10, time.Hour); err != nil || len(blocked) != 0 {
		t.Fatalf("old pull during grant = %+v, err=%v", blocked, err)
	}
	if allowed, err := b.Pull("orders", "workers", "temp", 10, time.Hour); err != nil || len(allowed) != 0 {
		t.Fatalf("temp should not steal old held message: %+v, err=%v", allowed, err)
	}
	// 未选分区继续由旧消费者完成。
	if err := b.Ack("orders", "workers", "old", pB.Token); err != nil {
		t.Fatalf("ack untouched partition: %v", err)
	}

	// 授权到期与旧处理确认同刻：旧令牌按交接时有效的 wait 策略仍然有效。
	now = g1.ExpiresAt
	if err := b.Ack("orders", "workers", "old", pA.Token); err != nil {
		t.Fatalf("old late ack at expiry: %v", err)
	}
	grants, _ := b.PartitionGrants("orders", "workers")
	if grants[0].Status != grantCompleted {
		t.Fatalf("g1 after old ack = %s", grants[0].Status)
	}

	// 第二次临时授权从 g1 到期同刻开始，验证相邻授权不被误判重叠。
	if err := b.Nack("orders", "workers", "old", ""); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("stale token nack = %v", err)
	}
	g2, err := b.CreatePartitionGrant("orders", "workers", PartitionGrantRequest{
		Partitions:   []string{"p-a"},
		FromConsumer: "old",
		ToConsumer:   "temp2",
		Reason:       "second temporary ownership",
		Mode:         HandoverRevoke,
		StartsAt:     now,
		ExpiresAt:    now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if g2.Status != grantActive {
		t.Fatalf("g2 status = %s", g2.Status)
	}

	h, err := b.History("orders", "workers", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(h) < 5 {
		t.Fatalf("history too short: %+v", h)
	}
	for _, e := range h {
		if e.Partition != "p-a" {
			t.Fatalf("partition lost from history: %+v", e)
		}
	}
	if h[0].GrantID != "" || h[len(h)-1].GrantID != g2.ID {
		t.Fatalf("grant history not traceable: %+v", h)
	}
}

func TestPartitionGrantRevokeDrainsAndCancelBeforeStart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	if err := b.SetRetryPolicy("orders", "workers", 2, time.Minute); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(200, 0)
	b.now = func() time.Time { return now }
	publishPartition(t, b, "p-x", 3)

	first, err := b.Pull("orders", "workers", "old", 1, time.Hour)
	if err != nil || len(first) != 1 {
		t.Fatalf("pull: %v len=%d", err, len(first))
	}
	oldToken := first[0].Token

	// 尚未开始的安排可取消。
	future, err := b.CreatePartitionGrant("orders", "workers", PartitionGrantRequest{
		Partitions:   []string{"p-x"},
		FromConsumer: "old",
		ToConsumer:   "future",
		Reason:       "future",
		Mode:         HandoverWait,
		StartsAt:     now.Add(time.Minute),
		ExpiresAt:    now.Add(2 * time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CancelPartitionGrant("orders", "workers", future.ID, "ops"); err != nil {
		t.Fatal(err)
	}
	if err := b.CancelPartitionGrant("orders", "workers", future.ID, "ops"); !errors.Is(err, ErrGrantStarted) {
		t.Fatalf("cancel finished = %v", err)
	}

	g, err := b.CreatePartitionGrant("orders", "workers", PartitionGrantRequest{
		Partitions:   []string{"p-x"},
		FromConsumer: "old",
		ToConsumer:   "temp",
		Reason:       "immediate revoke",
		Mode:         HandoverRevoke,
		StartsAt:     now,
		ExpiresAt:    now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Ack("orders", "workers", "old", oldToken); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("revoked old ack = %v", err)
	}
	ds, err := b.Pull("orders", "workers", "temp", 1, time.Hour)
	if err != nil || len(ds) != 1 || ds[0].Owner != "temp" || ds[0].GrantID != g.ID {
		t.Fatalf("temp pull = %+v err=%v", ds, err)
	}
	tempToken := ds[0].Token

	// 到期时选择等待：draining 中 temp 不能再拉新消息，但已有在途确认有效。
	now = g.ExpiresAt
	if blocked, err := b.Pull("orders", "workers", "temp", 10, time.Hour); err != nil || len(blocked) != 0 {
		t.Fatalf("temp pull while draining = %+v err=%v", blocked, err)
	}
	if err := b.Ack("orders", "workers", "temp", tempToken); err != nil {
		t.Fatalf("temp ack while draining: %v", err)
	}
	if old, err := b.Pull("orders", "workers", "old", 10, time.Hour); err != nil || len(old) != 2 {
		t.Fatalf("old resumes remaining work: %+v err=%v", old, err)
	}
}

func TestPartitionGrantPreservesQuarantineHoleAndRestartHistory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	if err := b.SetRetryPolicy("orders", "workers", 1, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(300, 0)
	b.now = func() time.Time { return now }
	publishPartition(t, b, "p-q", 2)

	ds, _ := b.Pull("orders", "workers", "old", 1, time.Hour)
	if err := b.Nack("orders", "workers", "old", ds[0].Token); err != nil {
		t.Fatal(err)
	}
	rest, _ := b.Pull("orders", "workers", "old", 10, time.Hour)
	if err := b.Ack("orders", "workers", "old", rest[0].Token); err != nil {
		t.Fatal(err)
	}

	g, err := b.CreatePartitionGrant("orders", "workers", PartitionGrantRequest{
		Partitions:   []string{"p-q"},
		FromConsumer: "old",
		ToConsumer:   "temp",
		Reason:       "quarantine handover",
		Mode:         HandoverWait,
		StartsAt:     now,
		ExpiresAt:    now.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	work, _ := b.PartitionWorkView("orders", "workers", []string{"p-q"})
	if got := work["p-q"].Quarantined; len(got) != 1 || got[0] != 0 {
		t.Fatalf("quarantined work = %v", got)
	}
	if pos, _ := b.Position("orders", "workers"); pos != 0 {
		t.Fatalf("position = %d, quarantine hole must remain", pos)
	}
	if dpos, _ := b.DeliverablePosition("orders", "workers"); dpos != 2 {
		t.Fatalf("deliverable = %d, want 2", dpos)
	}
	if err := b.Redeliver("orders", "workers", 0, "ops"); err != nil {
		t.Fatal(err)
	}
	redelivered, _ := b.Pull("orders", "workers", "temp", 1, time.Hour)
	if len(redelivered) != 1 || redelivered[0].Owner != "temp" {
		t.Fatalf("redelivery during grant: %+v", redelivered)
	}
	if err := b.ReturnPartitionGrant("orders", "workers", g.ID, "ops", HandoverRevoke); err != nil {
		t.Fatal(err)
	}
	if err := b.Ack("orders", "workers", "temp", redelivered[0].Token); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("ack after force return = %v", err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	b2.now = func() time.Time { return now }
	grants, err := b2.PartitionGrants("orders", "workers")
	if err != nil || len(grants) != 1 || grants[0].Reason != "quarantine handover" {
		t.Fatalf("grants after restart = %+v err=%v", grants, err)
	}
	h, _ := b2.History("orders", "workers", 0)
	kinds := eventKinds(h)
	want := []string{EventDeliver, EventNack, EventQuarantine, EventGrant, EventGrantStart, EventRedeliver, EventDeliver, EventGrantRevoke}
	if len(kinds) != len(want) {
		t.Fatalf("history = %v, want %v", kinds, want)
	}
	for i := range want {
		if kinds[i] != want[i] {
			t.Fatalf("history = %v, want %v", kinds, want)
		}
	}
	if pos, _ := b2.Position("orders", "workers"); pos != 0 {
		t.Fatalf("position after restart = %d", pos)
	}
	dests, _ := b2.AckedBy("orders", "workers")
	if _, ok := dests[0]; ok {
		t.Fatalf("revoked work must not produce result: %v", dests)
	}
}
