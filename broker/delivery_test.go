package broker

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// 延迟投递：可见时间之前拉不到，到期后才能拉到。
func TestDelayedDelivery(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	now := time.Now()
	b.now = func() time.Time { return now }

	if _, err := b.PublishDelayed("orders", "k0", []byte("later"), time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish("orders", "k1", []byte("now")); err != nil {
		t.Fatal(err)
	}
	ds, err := b.Pull("orders", "workers", "c1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 1 || ds[0].Message.Offset != 1 {
		t.Fatalf("before visible: want only offset 1, got %+v", ds)
	}
	if err := b.Ack("orders", "workers", "c1", ds[0].Token); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	ds, err = b.Pull("orders", "workers", "c1", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 1 || ds[0].Message.Offset != 0 || string(ds[0].Message.Payload) != "later" {
		t.Fatalf("after visible: want delayed offset 0, got %+v", ds)
	}
}

// 失败退避：Nack 后退避期内不可再投递，到期后重新投递。
func TestNackBackoff(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	now := time.Now()
	b.now = func() time.Time { return now }
	publishN(t, b, 1)

	ds, _ := b.Pull("orders", "workers", "c1", 1, time.Hour)
	if err := b.Nack("orders", "workers", "c1", ds[0].Token, time.Minute); err != nil {
		t.Fatal(err)
	}
	// 退避期内拉不到。
	if ds2, _ := b.Pull("orders", "workers", "c2", 1, time.Hour); len(ds2) != 0 {
		t.Fatalf("during backoff want 0, got %d", len(ds2))
	}
	// 退避结束后可被其他消费者拉到。
	now = now.Add(2 * time.Minute)
	ds2, err := b.Pull("orders", "workers", "c2", 1, time.Hour)
	if err != nil || len(ds2) != 1 {
		t.Fatalf("after backoff: %v len=%d", err, len(ds2))
	}
	// 旧 token 已失效。
	if err := b.Ack("orders", "workers", "c1", ds[0].Token); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("old token after nack: want ErrStaleToken, got %v", err)
	}
	if err := b.Ack("orders", "workers", "c2", ds2[0].Token); err != nil {
		t.Fatal(err)
	}
}

// 多次失败被隔离；隔离不阻塞后续消息；消费位置不能因跳过失败消息而误认前序完成。
func TestQuarantineDoesNotBlockOrAdvancePosition(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	b.MaxAttempts = 2
	now := time.Now()
	b.now = func() time.Time { return now }
	publishN(t, b, 4)

	// offset 0 连续失败两次 -> 隔离。
	for i := 0; i < 2; i++ {
		ds, err := b.Pull("orders", "workers", "c1", 1, time.Hour)
		if err != nil || len(ds) != 1 || ds[0].Message.Offset != 0 {
			t.Fatalf("round %d pull: %v %+v", i, err, ds)
		}
		if err := b.Nack("orders", "workers", "c1", ds[0].Token, 0); err != nil {
			t.Fatal(err)
		}
	}
	q, err := b.Quarantined("orders", "workers")
	if err != nil || len(q) != 1 || q[0] != 0 {
		t.Fatalf("quarantined: %v %v", err, q)
	}
	// 后续消息仍可被拉取并确认。
	ds, err := b.Pull("orders", "workers", "c2", 10, time.Hour)
	if err != nil || len(ds) != 3 {
		t.Fatalf("want 3 remaining, got %v %d", err, len(ds))
	}
	for _, d := range ds {
		if d.Message.Offset == 0 {
			t.Fatal("quarantined message must not be delivered")
		}
		if err := b.Ack("orders", "workers", "c2", d.Token); err != nil {
			t.Fatal(err)
		}
	}
	// 连续消费位置仍停在 0：跳过失败消息不代表前序已完成。
	pos, _ := b.Position("orders", "workers")
	if pos != 0 {
		t.Fatalf("position = %d, want 0 (offset 0 not acked)", pos)
	}
}

// 人工重投：保留原偏移身份，开启新尝试，旧 token 依然无效。
func TestManualRedeliver(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	b.MaxAttempts = 1
	now := time.Now()
	b.now = func() time.Time { return now }
	publishN(t, b, 1)

	ds, _ := b.Pull("orders", "workers", "c1", 1, time.Hour)
	oldToken := ds[0].Token
	if err := b.Nack("orders", "workers", "c1", oldToken, 0); err != nil {
		t.Fatal(err)
	}
	// 未隔离的消息不能重投。
	if err := b.Redeliver("orders", "workers", 5, "ops"); !errors.Is(err, ErrNoMessage) {
		t.Fatalf("want ErrNoMessage, got %v", err)
	}
	if err := b.Redeliver("orders", "workers", 0, "ops"); err != nil {
		t.Fatal(err)
	}
	ds2, err := b.Pull("orders", "workers", "c2", 1, time.Hour)
	if err != nil || len(ds2) != 1 {
		t.Fatalf("redelivered pull: %v len=%d", err, len(ds2))
	}
	if ds2[0].Message.Offset != 0 || string(ds2[0].Message.Payload) != "payload-0" {
		t.Fatalf("identity must be preserved: %+v", ds2[0].Message)
	}
	if err := b.Ack("orders", "workers", "c1", oldToken); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("old token: want ErrStaleToken, got %v", err)
	}
	if err := b.Ack("orders", "workers", "c2", ds2[0].Token); err != nil {
		t.Fatal(err)
	}
	// 历史包含完整链路：deliver -> reject -> quarantine -> redeliver -> deliver -> ack。
	h := b.History("orders", "workers", 0)
	want := []string{EventDeliver, EventReject, EventQuarantine, EventRedeliver, EventDeliver, EventAck}
	if len(h) != len(want) {
		t.Fatalf("history len = %d, want %d: %+v", len(h), len(want), h)
	}
	for i, e := range want {
		if h[i].Event != e {
			t.Fatalf("history[%d] = %s, want %s", i, h[i].Event, e)
		}
	}
	if h[3].Consumer != "ops" {
		t.Fatalf("redeliver operator = %q, want ops", h[3].Consumer)
	}
}

// 历史留痕：领取、超时、拒绝、确认全部记录；最终确认者不代替尝试记录；重启后仍可核对。
func TestHistorySurvivesRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	now := time.Now()
	b.now = func() time.Time { return now }
	publishN(t, b, 1)

	if _, err := b.Pull("orders", "workers", "c1", 1, 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	ds2, _ := b.Pull("orders", "workers", "c2", 1, time.Hour) // 触发超时回收
	if err := b.Nack("orders", "workers", "c2", ds2[0].Token, 0); err != nil {
		t.Fatal(err)
	}
	ds3, _ := b.Pull("orders", "workers", "c3", 1, time.Hour)
	if err := b.Ack("orders", "workers", "c3", ds3[0].Token); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	h := b2.History("orders", "workers", 0)
	want := []struct {
		event    string
		consumer string
	}{
		{EventDeliver, "c1"},
		{EventTimeout, "c1"},
		{EventDeliver, "c2"},
		{EventReject, "c2"},
		{EventDeliver, "c3"},
		{EventAck, "c3"},
	}
	if len(h) != len(want) {
		t.Fatalf("history len = %d, want %d: %+v", len(h), len(want), h)
	}
	for i, w := range want {
		if h[i].Event != w.event || h[i].Consumer != w.consumer {
			t.Fatalf("history[%d] = %s/%s, want %s/%s", i, h[i].Event, h[i].Consumer, w.event, w.consumer)
		}
	}
	// 最终去向只有 c3，但历史保留全部尝试。
	dest, _ := b2.AckedBy("orders", "workers")
	if dest[0] != "c3" {
		t.Fatalf("final destination = %q, want c3", dest[0])
	}
}

// 重启发生在确认边界：已确认的不重投，未确认的（含在途）允许重投，位置连续。
func TestRestartAtAckBoundary(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	publishN(t, b, 4)

	ds, _ := b.Pull("orders", "workers", "c1", 2, time.Hour)
	for _, d := range ds {
		if err := b.Ack("orders", "workers", "c1", d.Token); err != nil {
			t.Fatal(err)
		}
	}
	// 第三条在途未确认时直接重启（模拟崩溃）。
	if _, err := b.Pull("orders", "workers", "c1", 1, time.Hour); err != nil {
		t.Fatal(err)
	}
	b2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	pos, _ := b2.Position("orders", "workers")
	if pos != 2 {
		t.Fatalf("position = %d, want 2", pos)
	}
	ds2, err := b2.Pull("orders", "workers", "c2", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds2) != 2 || ds2[0].Message.Offset != 2 || ds2[1].Message.Offset != 3 {
		t.Fatalf("want offsets 2,3 redelivered in order, got %+v", ds2)
	}
	// 历史里仍有崩溃前两次领取与确认。
	h := b2.History("orders", "workers", 0)
	if len(h) != 2 || h[0].Event != EventDeliver || h[1].Event != EventAck {
		t.Fatalf("history for offset 0: %+v", h)
	}
}

// 隔离与退避状态跨重启保留。
func TestQuarantinePersistsAcrossRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "broker")
	b := setup(t, dir)
	b.MaxAttempts = 1
	publishN(t, b, 2)

	ds, _ := b.Pull("orders", "workers", "c1", 1, time.Hour)
	if err := b.Nack("orders", "workers", "c1", ds[0].Token, 0); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	q, _ := b2.Quarantined("orders", "workers")
	if len(q) != 1 || q[0] != 0 {
		t.Fatalf("quarantined after restart: %v", q)
	}
	ds2, _ := b2.Pull("orders", "workers", "c2", 10, time.Minute)
	if len(ds2) != 1 || ds2[0].Message.Offset != 1 {
		t.Fatalf("only offset 1 should be deliverable, got %+v", ds2)
	}
	// 人工重投后恢复可投递。
	if err := b2.Redeliver("orders", "workers", 0, "ops"); err != nil {
		t.Fatal(err)
	}
	if err := b2.Ack("orders", "workers", "c2", ds2[0].Token); err != nil {
		t.Fatal(err)
	}
	ds3, _ := b2.Pull("orders", "workers", "c3", 10, time.Minute)
	if len(ds3) != 1 || ds3[0].Message.Offset != 0 {
		t.Fatalf("redelivered offset 0 expected, got %+v", ds3)
	}
}
