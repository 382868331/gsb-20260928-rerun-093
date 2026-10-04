package broker

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// keyForPartition 找一个落入指定分区的 key。
func keyForPartition(partitions, want int) string {
	for i := 0; ; i++ {
		k := fmt.Sprintf("key-%d", i)
		if partitionOf(k, partitions) == want {
			return k
		}
	}
}

func setupGrantBroker(t *testing.T, now *time.Time) (*Broker, string) {
	t.Helper()
	b, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	*now = time.Now()
	b.now = func() time.Time { return *now }
	if err := b.CreateTopic("orders"); err != nil {
		t.Fatal(err)
	}
	if err := b.CreateSubscription("orders", "sub"); err != nil {
		t.Fatal(err)
	}
	if err := b.SetPartitions("orders", "sub", 2); err != nil {
		t.Fatal(err)
	}
	return b, "orders"
}

func TestGrantHandoverBacklogAndReturn(t *testing.T) {
	var now time.Time
	b, _ := setupGrantBroker(t, &now)
	defer b.Close()

	k0 := keyForPartition(2, 0)
	k1 := keyForPartition(2, 1)
	// 分区 0：一条在途、一条退避等待重投、一条隔离、一条待投递。
	if _, err := b.Publish("orders", k0, []byte("p0-inflight")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish("orders", k0, []byte("p0-retry")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish("orders", k0, []byte("p0-quar")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish("orders", k0, []byte("p0-pending")); err != nil {
		t.Fatal(err)
	}
	// 分区 1：一条普通消息，交接期间应继续由原消费者处理。
	if _, err := b.Publish("orders", k1, []byte("p1-normal")); err != nil {
		t.Fatal(err)
	}
	if err := b.SetRetryPolicy("orders", "sub", 2, time.Minute); err != nil {
		t.Fatal(err)
	}

	// 分区 0 造出四种状态：在途、隔离、退避等待重投、待投递。
	pull1 := func() []Delivery {
		ds, err := b.Pull("orders", "sub", "c-a", 1, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if len(ds) != 1 {
			t.Fatalf("expected 1 delivery, got %d", len(ds))
		}
		return ds
	}
	inflightTok := pull1()[0].Token // p0-inflight：一直在途
	quarTok := pull1()[0].Token     // p0-quar：两次拒绝后隔离
	if err := b.Nack("orders", "sub", "c-a", quarTok); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Minute) // 退避到期
	quarTok = pull1()[0].Token
	if err := b.Nack("orders", "sub", "c-a", quarTok); err != nil {
		t.Fatal(err)
	}
	quar, err := b.Quarantined("orders", "sub")
	if err != nil || len(quar) != 1 {
		t.Fatalf("expected 1 quarantined, got %v %v", quar, err)
	}
	retryTok := pull1()[0].Token // p0-retry：退避等待重投
	if err := b.Nack("orders", "sub", "c-a", retryTok); err != nil {
		t.Fatal(err)
	}
	// p0-pending 从未领取，p1-normal 留给交接期间的原处理者。
	// 交接前查看积压：在途 1（p0-inflight）、等待重投 1（p0-retry）、隔离 1、待投递 1。
	view, err := b.Backlog("orders", "sub", []int{0})
	if err != nil {
		t.Fatal(err)
	}
	if len(view.InFlight) != 1 || view.InFlight[0].Owner != "c-a" {
		t.Fatalf("backlog inflight = %+v", view.InFlight)
	}
	if len(view.Retry) != 1 || len(view.Quarantined) != 1 || len(view.Pending) != 1 {
		t.Fatalf("backlog = %+v", view)
	}

	// 把分区 0 临时交给 c-b，选择撤回旧处理权（不等旧在途完成）。
	start := now
	expiry := now.Add(30 * time.Minute)
	gid, err := b.GrantPartitions("orders", "sub", []int{0}, "c-a", "c-b", "backlog spillover", start, expiry, true)
	if err != nil {
		t.Fatal(err)
	}
	// 旧在途被撤回：c-a 的迟到确认按当时有效归属判断，必须失败。
	if err := b.Ack("orders", "sub", "c-a", inflightTok); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("late ack after revoke should be stale, got %v", err)
	}
	// 授权期内 c-a 拉不到分区 0 的消息，但分区 1 继续原处理。
	ds, err := b.Pull("orders", "sub", "c-a", 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range ds {
		if partitionOf(d.Message.Key, 2) == 0 {
			t.Fatalf("c-a pulled granted partition message %s", d.Message.Payload)
		}
	}
	if len(ds) == 0 {
		t.Fatal("c-a should still pull partition 1 messages")
	}
	// c-b 接管分区 0：撤回的消息与待投递消息都可拉取，投递记录带授权信息。
	ds, err = b.Pull("orders", "sub", "c-b", 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) == 0 {
		t.Fatal("c-b should pull granted partition messages")
	}
	for _, d := range ds {
		if partitionOf(d.Message.Key, 2) != 0 {
			t.Fatalf("c-b pulled non-granted partition message %s", d.Message.Payload)
		}
	}
	var bTok string
	for _, d := range ds {
		if string(d.Message.Payload) == "p0-inflight" {
			bTok = d.Token
		}
	}
	if bTok == "" {
		t.Fatal("revoked message not redelivered to c-b")
	}
	if err := b.Ack("orders", "sub", "c-b", bTok); err != nil {
		t.Fatal(err)
	}
	// 同一条消息不会因换人而重复生效。
	if err := b.Ack("orders", "sub", "c-b", bTok); !errors.Is(err, ErrAlreadyAcked) {
		t.Fatalf("duplicate ack should fail, got %v", err)
	}

	// 隔离消息形成的确认缺口不能被交接抹平。
	pos, err := b.Position("orders", "sub")
	if err != nil {
		t.Fatal(err)
	}
	dpos, err := b.DeliverablePosition("orders", "sub")
	if err != nil {
		t.Fatal(err)
	}
	if pos != 1 || dpos != 2 {
		t.Fatalf("quarantine gap must remain: position=%d deliverable=%d", pos, dpos)
	}

	// 重复交接：同一分区时间重叠的授权被拒绝。
	if _, err := b.GrantPartitions("orders", "sub", []int{0}, "c-a", "c-c", "overlap", now, expiry, false); !errors.Is(err, ErrGrantConflict) {
		t.Fatalf("overlapping grant should be rejected, got %v", err)
	}

	// 到期归还，等待收尾（不终止在途）。
	now = expiry
	if err := b.ReturnGrant("orders", "sub", gid, false); err != nil {
		t.Fatal(err)
	}
	// 归还后 c-a 重新拉到分区 0 的消息。
	ds, err = b.Pull("orders", "sub", "c-a", 10, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	seenP0 := false
	for _, d := range ds {
		if partitionOf(d.Message.Key, 2) == 0 {
			seenP0 = true
		}
	}
	if !seenP0 {
		t.Fatal("after return c-a should pull partition 0 again")
	}

	// 连续两次临时授权：归还后可再次交接同一分区。
	gid2, err := b.GrantPartitions("orders", "sub", []int{0}, "c-a", "c-b", "second spillover", now, now.Add(time.Hour), false)
	if err != nil {
		t.Fatalf("second grant after return should succeed, got %v", err)
	}
	if gid2 == gid {
		t.Fatal("grant IDs must differ")
	}

	// 沿消息看历史：处理者、授权范围、交接原因与最终处置。
	hist, err := b.History("orders", "sub", -1)
	if err != nil {
		t.Fatal(err)
	}
	var grantEv, returnEv, revokeEv, deliverWithGrant bool
	for _, e := range hist {
		switch e.Event {
		case EventGrant:
			grantEv = grantEv || (strings.Contains(e.Detail, "reason=backlog spillover") && strings.Contains(e.Detail, "partitions=[0]"))
		case EventGrantReturn:
			returnEv = true
		case EventRevoke:
			revokeEv = true
		case EventDeliver:
			if strings.Contains(e.Detail, "grant="+gid) {
				deliverWithGrant = true
			}
		}
	}
	if !grantEv || !returnEv || !revokeEv || !deliverWithGrant {
		t.Fatalf("history missing grant facts: grant=%v return=%v revoke=%v deliver=%v", grantEv, returnEv, revokeEv, deliverWithGrant)
	}
	ackedBy, err := b.AckedBy("orders", "sub")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for off, who := range ackedBy {
		if who == "c-b" && partitionOf(b.topics["orders"][off].Key, 2) == 0 {
			found = true
		}
	}
	if !found {
		t.Fatal("expected c-b as final acker of a partition 0 message")
	}
}

func TestGrantReturnTerminateVsWaitSameInstant(t *testing.T) {
	var now time.Time
	b, _ := setupGrantBroker(t, &now)
	defer b.Close()

	k0 := keyForPartition(2, 0)
	if _, err := b.Publish("orders", k0, []byte("m1")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish("orders", k0, []byte("m2")); err != nil {
		t.Fatal(err)
	}

	// 授权给 c-b（等待旧处理完成模式），c-b 领取 m1。
	gid, err := b.GrantPartitions("orders", "sub", []int{0}, "c-a", "c-b", "spill", now, now.Add(time.Hour), false)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := b.Pull("orders", "sub", "c-b", 1, time.Hour)
	if err != nil || len(ds) != 1 {
		t.Fatalf("pull: %v %d", err, len(ds))
	}
	tok := ds[0].Token

	// 归还与确认同刻发生：等待收尾模式下，当时有效归属仍认可该确认。
	returnAt := now.Add(30 * time.Minute)
	now = returnAt
	if err := b.ReturnGrant("orders", "sub", gid, false); err != nil {
		t.Fatal(err)
	}
	if err := b.Ack("orders", "sub", "c-b", tok); err != nil {
		t.Fatalf("same-instant ack after graceful return should succeed, got %v", err)
	}

	// 第二次授权，归还时立即终止旧授权：同刻迟到确认必须按新归属拒绝。
	gid2, err := b.GrantPartitions("orders", "sub", []int{0}, "c-a", "c-b", "spill2", now, now.Add(time.Hour), false)
	if err != nil {
		t.Fatal(err)
	}
	ds, err = b.Pull("orders", "sub", "c-b", 1, time.Hour)
	if err != nil || len(ds) != 1 {
		t.Fatalf("pull2: %v %d", err, len(ds))
	}
	tok2 := ds[0].Token
	now = now.Add(30 * time.Minute)
	if err := b.ReturnGrant("orders", "sub", gid2, true); err != nil {
		t.Fatal(err)
	}
	if err := b.Ack("orders", "sub", "c-b", tok2); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("terminate return should stale the in-flight token, got %v", err)
	}
	// 被终止的消息重新投递给原处理者，最终只确认一次。
	ds, err = b.Pull("orders", "sub", "c-a", 1, time.Hour)
	if err != nil || len(ds) != 1 {
		t.Fatalf("pull3: %v %d", err, len(ds))
	}
	if err := b.Ack("orders", "sub", "c-a", ds[0].Token); err != nil {
		t.Fatal(err)
	}
	counts, err := b.Counts("orders", "sub")
	if err != nil {
		t.Fatal(err)
	}
	if counts.Acked != 2 {
		t.Fatalf("acked = %d, want 2 (no duplicate effect)", counts.Acked)
	}
}

func TestGrantCancelAndRestart(t *testing.T) {
	var now time.Time
	b, _ := setupGrantBroker(t, &now)
	k0 := keyForPartition(2, 0)
	if _, err := b.Publish("orders", k0, []byte("m1")); err != nil {
		t.Fatal(err)
	}

	// 尚未开始的临时安排可以取消。
	future := now.Add(time.Hour)
	gid, err := b.GrantPartitions("orders", "sub", []int{0}, "c-a", "c-b", "planned", future, future.Add(time.Hour), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CancelGrant("orders", "sub", gid); err != nil {
		t.Fatal(err)
	}
	// 已取消的授权不生效：到达原开始时间后 c-a 照常拉取。
	now = future
	ds, err := b.Pull("orders", "sub", "c-a", 1, time.Hour)
	if err != nil || len(ds) != 1 {
		t.Fatalf("cancelled grant must not take effect: %v %d", err, len(ds))
	}
	// 已经开始的授权不能取消，事实保留。
	gid2, err := b.GrantPartitions("orders", "sub", []int{0}, "c-a", "c-b", "active", now, now.Add(time.Hour), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.CancelGrant("orders", "sub", gid2); !errors.Is(err, ErrGrantStarted) {
		t.Fatalf("cancel started grant should fail, got %v", err)
	}

	// 退出后重开：授权、分区数与积压视图都恢复。
	dir := b.dir
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	b2.now = func() time.Time { return now }
	grants, err := b2.Grants("orders", "sub")
	if err != nil {
		t.Fatal(err)
	}
	if len(grants) != 2 {
		t.Fatalf("grants after restart = %+v", grants)
	}
	if !grants[0].Cancelled || grants[1].Cancelled {
		t.Fatalf("grant states after restart = %+v", grants)
	}
	// 有效授权仍然拦截原处理者。
	ds, err = b2.Pull("orders", "sub", "c-a", 1, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 0 {
		t.Fatalf("active grant should still block c-a after restart, got %d", len(ds))
	}
	ds, err = b2.Pull("orders", "sub", "c-b", 1, time.Hour)
	if err != nil || len(ds) != 1 {
		t.Fatalf("delegate should pull after restart: %v %d", err, len(ds))
	}
	// 历史跨重启保留授权范围与交接原因。
	hist, err := b2.History("orders", "sub", -1)
	if err != nil {
		t.Fatal(err)
	}
	var cancelEv bool
	for _, e := range hist {
		if e.Event == EventGrantCancel && strings.Contains(e.Detail, "reason=planned") {
			cancelEv = true
		}
	}
	if !cancelEv {
		t.Fatal("cancel fact must survive restart")
	}
}

func TestGrantExpiryPreviewRevisionAndRestart(t *testing.T) {
	var now time.Time
	b, _ := setupGrantBroker(t, &now)

	k0 := keyForPartition(2, 0)
	k1 := keyForPartition(2, 1)
	if _, err := b.Publish("orders", k0, []byte("p0")); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Publish("orders", k1, []byte("p1")); err != nil {
		t.Fatal(err)
	}
	start := now
	oldExpiry := now.Add(time.Hour)
	gid, err := b.GrantPartitions("orders", "sub", []int{0, 1}, "c-a", "c-b", "active spillover", start, oldExpiry, false)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := b.Pull("orders", "sub", "c-b", 10, 2*time.Hour)
	if err != nil || len(ds) != 2 {
		t.Fatalf("initial pull = %d, %v", len(ds), err)
	}
	tokens := map[int]string{}
	for _, d := range ds {
		tokens[partitionOf(d.Message.Key, 2)] = d.Token
	}

	// 整次延长：提交前列出在途消息、原期限、新期限和处理方式。
	extendedExpiry := oldExpiry.Add(time.Hour)
	extendPreview, err := b.PrepareGrantExpiryPreview("orders", "sub", gid, nil, extendedExpiry, "")
	if err != nil {
		t.Fatal(err)
	}
	if extendPreview.Revision != 1 || len(extendPreview.Partitions) != 2 || extendPreview.Shortened {
		t.Fatalf("bad extend preview: %+v", extendPreview)
	}
	if len(extendPreview.InFlight) != 2 {
		t.Fatalf("extend inflight preview = %+v", extendPreview.InFlight)
	}
	for _, item := range extendPreview.InFlight {
		if !item.OldGrantExpiry.Equal(oldExpiry) || !item.NewGrantExpiry.Equal(extendedExpiry) || item.Action != GrantExpiryKeep {
			t.Fatalf("bad extend inflight item: %+v", item)
		}
	}
	if err := b.ConfirmGrantExpiry("orders", "sub", extendPreview); err != nil {
		t.Fatal(err)
	}
	if err := b.ConfirmGrantExpiry("orders", "sub", extendPreview); !errors.Is(err, ErrGrantRevisionStale) {
		t.Fatalf("old revision confirmation = %v, want stale", err)
	}

	// 只缩短分区 0：必须明确等待或撤销，未选分区 1 保持延长后的期限。
	shortP0Expiry := now.Add(30 * time.Minute)
	if _, err := b.PrepareGrantExpiryPreview("orders", "sub", gid, []int{0}, shortP0Expiry, ""); !errors.Is(err, ErrGrantExpiryChoice) {
		t.Fatalf("shorten without explicit choice = %v", err)
	}
	waitPreview, err := b.PrepareGrantExpiryPreview("orders", "sub", gid, []int{0}, shortP0Expiry, GrantExpiryWait)
	if err != nil {
		t.Fatal(err)
	}
	if waitPreview.Revision != 2 || !waitPreview.Shortened || len(waitPreview.InFlight) != 1 {
		t.Fatalf("bad wait preview: %+v", waitPreview)
	}
	item := waitPreview.InFlight[0]
	if item.Partition != 0 || item.Action != GrantExpiryWait || !item.OldGrantExpiry.Equal(extendedExpiry) || !item.NewGrantExpiry.Equal(shortP0Expiry) {
		t.Fatalf("bad shorten preview item: %+v", item)
	}
	if err := b.ConfirmGrantExpiry("orders", "sub", waitPreview); err != nil {
		t.Fatal(err)
	}
	now = shortP0Expiry
	if err := b.Ack("orders", "sub", "c-b", tokens[0]); err != nil {
		t.Fatalf("waiting policy must preserve current in-flight processing: %v", err)
	}
	if _, err := b.Pull("orders", "sub", "c-b", 10, time.Hour); err != nil {
		t.Fatal(err)
	}

	// 分区 1 再缩短并撤销当前处理权：旧 Token 立即失效，但不视为成功。
	shortP1Expiry := now.Add(30 * time.Minute)
	revokePreview, err := b.PrepareGrantExpiryPreview("orders", "sub", gid, []int{1}, shortP1Expiry, GrantExpiryRevoke)
	if err != nil {
		t.Fatal(err)
	}
	if len(revokePreview.InFlight) != 1 || revokePreview.InFlight[0].Action != GrantExpiryRevoke {
		t.Fatalf("bad revoke preview: %+v", revokePreview.InFlight)
	}
	if err := b.ConfirmGrantExpiry("orders", "sub", revokePreview); err != nil {
		t.Fatal(err)
	}
	if err := b.Ack("orders", "sub", "c-b", tokens[1]); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("revoked in-flight ack = %v, want stale", err)
	}
	now = shortP1Expiry
	ds, err = b.Pull("orders", "sub", "c-a", 10, time.Hour)
	if err != nil || len(ds) != 1 || partitionOf(ds[0].Message.Key, 2) != 1 {
		t.Fatalf("after shortened expiry original owner should receive p1: %+v %v", ds, err)
	}
	if err := b.Ack("orders", "sub", "c-a", ds[0].Token); err != nil {
		t.Fatal(err)
	}
	counts, err := b.Counts("orders", "sub")
	if err != nil || counts.Acked != 2 {
		t.Fatalf("counts = %+v, %v", counts, err)
	}

	// 保存重开后可追溯每次修订、实际影响分区与各分区当前期限。
	dir := b.dir
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b2.Close()
	b2.now = func() time.Time { return now }
	grants, err := b2.Grants("orders", "sub")
	if err != nil || len(grants) != 1 {
		t.Fatalf("grants after restart = %+v, %v", grants, err)
	}
	g := grants[0]
	if g.Revision != 4 || len(g.Revisions) != 3 || len(g.Terms) != 2 {
		t.Fatalf("revision history after restart = %+v", g)
	}
	terms := map[int]time.Time{}
	for _, term := range g.Terms {
		terms[term.Partition] = term.ExpiresAt
	}
	if !terms[0].Equal(shortP0Expiry) || !terms[1].Equal(shortP1Expiry) {
		t.Fatalf("actual partition expiry scope = %+v", terms)
	}
	if len(g.Revisions[0].Partitions) != 2 || len(g.Revisions[1].Partitions) != 1 || g.Revisions[1].Partitions[0] != 0 || g.Revisions[2].Partitions[0] != 1 {
		t.Fatalf("revision scopes = %+v", g.Revisions)
	}
	hist, err := b2.History("orders", "sub", -1)
	if err != nil {
		t.Fatal(err)
	}
	var expiryEvents int
	for _, e := range hist {
		if e.Event == EventGrantExpiry && strings.Contains(e.Detail, "grant="+gid) {
			expiryEvents++
		}
	}
	if expiryEvents != 3 {
		t.Fatalf("grant expiry history events = %d, want 3", expiryEvents)
	}
}

func TestGrantExpiryShortenToNowRevokesWithoutSuccess(t *testing.T) {
	var now time.Time
	b, _ := setupGrantBroker(t, &now)
	defer b.Close()

	k0 := keyForPartition(2, 0)
	if _, err := b.Publish("orders", k0, []byte("p0")); err != nil {
		t.Fatal(err)
	}
	gid, err := b.GrantPartitions("orders", "sub", []int{0}, "c-a", "c-b", "active", now, now.Add(time.Hour), false)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := b.Pull("orders", "sub", "c-b", 1, time.Hour)
	if err != nil || len(ds) != 1 {
		t.Fatalf("pull = %+v, %v", ds, err)
	}
	tok := ds[0].Token
	preview, err := b.PrepareGrantExpiryPreview("orders", "sub", gid, nil, now, GrantExpiryRevoke)
	if err != nil {
		t.Fatal(err)
	}
	if len(preview.InFlight) != 1 || preview.InFlight[0].Action != GrantExpiryRevoke {
		t.Fatalf("preview = %+v", preview.InFlight)
	}
	if err := b.ConfirmGrantExpiry("orders", "sub", preview); err != nil {
		t.Fatal(err)
	}
	if err := b.Ack("orders", "sub", "c-b", tok); !errors.Is(err, ErrStaleToken) {
		t.Fatalf("revoked ack = %v, want stale", err)
	}
	counts, err := b.Counts("orders", "sub")
	if err != nil || counts.Acked != 0 {
		t.Fatalf("revoked work must not count as success: %+v %v", counts, err)
	}
	ds, err = b.Pull("orders", "sub", "c-a", 1, time.Hour)
	if err != nil || len(ds) != 1 {
		t.Fatalf("original owner redelivery = %+v, %v", ds, err)
	}
}
