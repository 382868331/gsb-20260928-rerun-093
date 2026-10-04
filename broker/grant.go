package broker

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"
)

// 临时授权（交接）相关历史事件类型。
const (
	EventGrant       = "grant"        // 建立临时授权（交接）
	EventGrantCancel = "grant_cancel" // 取消尚未开始的临时授权
	EventGrantReturn = "grant_return" // 授权到期归还
	EventGrantExpiry = "grant_expiry" // 临时授权期限修订
	EventRevoke      = "revoke"       // 在途处理权被撤回（交接或归还时立即终止）
)

var (
	ErrNoGrant            = errors.New("broker: no such grant")
	ErrGrantConflict      = errors.New("broker: partition already covered by an overlapping grant")
	ErrGrantStarted       = errors.New("broker: grant already started, cannot cancel")
	ErrGrantClosed        = errors.New("broker: grant already cancelled or returned")
	ErrInvalidPartition   = errors.New("broker: invalid partition")
	ErrGrantRevisionStale = errors.New("broker: grant revision is stale; review the latest expiry preview")
	ErrGrantExpiryChoice  = errors.New("broker: shortening a grant requires wait or revoke for current processing")
	ErrGrantExpirySame    = errors.New("broker: grant expiry is unchanged for selected partitions")
)

const (
	GrantExpiryWait   = "wait"   // 缩短后等待已有在途处理自然完成或超时
	GrantExpiryRevoke = "revoke" // 缩短时撤销受托者当前在途处理权
	GrantExpiryKeep   = "keep"   // 不撤销在途处理（延长或无需变动时）
)

// Grant 是一次临时授权：把指定分区在 [StartsAt, ExpiresAt) 内
// 从原处理者 From 临时交给 To 处理，到期归还，不永久改变归属。
// 授权范围、期限与交接原因随消息历史保留。
type Grant struct {
	ID         string          `json:"id"`
	Topic      string          `json:"topic"`
	Sub        string          `json:"sub"`
	Partitions []int           `json:"partitions"`
	From       string          `json:"from"`
	To         string          `json:"to"`
	Reason     string          `json:"reason"`
	StartsAt   time.Time       `json:"starts_at"`
	ExpiresAt  time.Time       `json:"expires_at"`
	CreatedAt  time.Time       `json:"created_at"`
	Revision   int             `json:"revision,omitempty"`
	Terms      []GrantTerm     `json:"terms,omitempty"`
	Revisions  []GrantRevision `json:"revisions,omitempty"`
	Cancelled  bool            `json:"cancelled,omitempty"`
	Returned   bool            `json:"returned,omitempty"`
	ReturnedAt time.Time       `json:"returned_at,omitempty"`
	Terminated bool            `json:"terminated,omitempty"` // 归还时立即终止在途处理
}

// GrantTerm 是单个分区当前实际生效的授权期限。
type GrantTerm struct {
	Partition int       `json:"partition"`
	ExpiresAt time.Time `json:"expires_at"`
}

// GrantRevision 记录一次授权期限修订及其实际影响分区。
type GrantRevision struct {
	Revision        int               `json:"revision"`
	At              time.Time         `json:"at"`
	Partitions      []int             `json:"partitions"`
	Changes         []GrantTermChange `json:"changes,omitempty"`
	NewExpiresAt    time.Time         `json:"new_expires_at"`
	ShortenedPolicy string            `json:"shortened_policy,omitempty"`
}

// GrantTermChange 记录单个分区在一次期限修订前后的实际期限。
type GrantTermChange struct {
	Partition int       `json:"partition"`
	OldExpiry time.Time `json:"old_expires_at"`
	NewExpiry time.Time `json:"new_expires_at"`
}

// GrantExpiryInFlight 是期限变更预演中的一条受影响在途消息。
type GrantExpiryInFlight struct {
	Offset           int64     `json:"offset"`
	Owner            string    `json:"owner"`
	Epoch            uint64    `json:"epoch"`
	Partition        int       `json:"partition"`
	OldGrantExpiry   time.Time `json:"old_grant_expiry"`
	NewGrantExpiry   time.Time `json:"new_grant_expiry"`
	DeliveryDeadline time.Time `json:"delivery_deadline"`
	Action           string    `json:"action"`
}

// GrantExpiryPreview 是提交前的期限变更预演。它不会落盘；
// Revision 是预演所依据的授权修订，确认时必须仍为最新修订。
type GrantExpiryPreview struct {
	GrantID         string                `json:"grant_id"`
	Revision        int                   `json:"revision"`
	Partitions      []int                 `json:"partitions"`
	NewExpiresAt    time.Time             `json:"new_expires_at"`
	ShortenedPolicy string                `json:"shortened_policy,omitempty"`
	Shortened       bool                  `json:"shortened"`
	InFlight        []GrantExpiryInFlight `json:"in_flight"`
}

// active 判断授权在 t 时刻是否有效（已生效、未到期、未取消、未归还）。
func (g *Grant) active(t time.Time) bool {
	if g.Cancelled || g.Returned || g.StartsAt.After(t) {
		return false
	}
	for _, term := range g.Terms {
		if t.Before(term.ExpiresAt) {
			return true
		}
	}
	return len(g.Terms) == 0 && t.Before(g.ExpiresAt)
}

func (g *Grant) partitionActive(partition int, t time.Time) bool {
	return g.active(t) && g.covers(partition) && t.Before(g.effectiveExpiry(partition))
}

func (g *Grant) effectiveExpiry(partition int) time.Time {
	for _, term := range g.Terms {
		if term.Partition == partition {
			return term.ExpiresAt
		}
	}
	return g.ExpiresAt
}

func (g *Grant) covers(partition int) bool {
	for _, p := range g.Partitions {
		if p == partition {
			return true
		}
	}
	return false
}

// partitionOf 返回 key 在订阅分区数下所属的分区。
func partitionOf(key string, partitions int) int {
	if partitions < 1 {
		partitions = 1
	}
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(partitions))
}

// SetPartitions 设置订阅的分区数（默认 1，即全部消息同属一个分区）。
// 分区按消息 Key 哈希归属，分区数改变只影响后续判断。
func (b *Broker) SetPartitions(topic, sub string, partitions int) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return err
	}
	if partitions < 1 {
		return ErrInvalidPartition
	}
	s.partitions = partitions
	return b.flushLocked()
}

// InFlightInfo 描述一条尚未完成的在途处理。
type InFlightInfo struct {
	Offset   int64
	Owner    string
	Epoch    uint64
	Deadline time.Time
	Attempt  int
}

// RetryInfo 描述一条失败退避中、等待重投的消息。
type RetryInfo struct {
	Offset   int64
	Failures int
	RetryAt  time.Time
}

// BacklogView 是交接前的积压视图：尚未完成（在途）、等待重投（退避中）、
// 已隔离与仍待投递的消息，便于在交接前明确范围。
type BacklogView struct {
	InFlight    []InFlightInfo // 尚未完成：已领取未确认
	Retry       []RetryInfo    // 等待重投：失败退避中
	Quarantined []int64        // 已隔离：不再自动投递
	Pending     []int64        // 仍待投递：未确认、未隔离、未放弃、未在途
}

// Backlog 返回指定分区的积压视图；partitions 为空表示全部分区。
func (b *Broker) Backlog(topic, sub string, partitions []int) (BacklogView, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return BacklogView{}, err
	}
	sel := map[int]bool{}
	for _, p := range partitions {
		if p < 0 || p >= s.partitions {
			return BacklogView{}, ErrInvalidPartition
		}
		sel[p] = true
	}
	want := func(off int64) bool {
		if len(sel) == 0 {
			return true
		}
		return sel[partitionOf(b.topics[topic][off].Key, s.partitions)]
	}
	var view BacklogView
	now := b.now()
	for off, f := range s.inFlight {
		if want(off) {
			view.InFlight = append(view.InFlight, InFlightInfo{
				Offset:   off,
				Owner:    f.owner,
				Epoch:    f.epoch,
				Deadline: f.deadline,
				Attempt:  s.failures[off] + 1,
			})
		}
	}
	for off, t := range s.retryAt {
		if t.After(now) && want(off) {
			view.Retry = append(view.Retry, RetryInfo{Offset: off, Failures: s.failures[off], RetryAt: t})
		}
	}
	for off := range s.quarantined {
		if want(off) {
			view.Quarantined = append(view.Quarantined, off)
		}
	}
	for off := int64(0); off < int64(len(b.topics[topic])); off++ {
		if s.acked[off] || s.quarantined[off] || s.abandoned[off] {
			continue
		}
		if _, held := s.inFlight[off]; held {
			continue
		}
		if t, backing := s.retryAt[off]; backing && t.After(now) {
			continue
		}
		if want(off) {
			view.Pending = append(view.Pending, off)
		}
	}
	sort.Slice(view.InFlight, func(i, j int) bool { return view.InFlight[i].Offset < view.InFlight[j].Offset })
	sort.Slice(view.Retry, func(i, j int) bool { return view.Retry[i].Offset < view.Retry[j].Offset })
	sort.Slice(view.Quarantined, func(i, j int) bool { return view.Quarantined[i] < view.Quarantined[j] })
	sort.Slice(view.Pending, func(i, j int) bool { return view.Pending[i] < view.Pending[j] })
	return view, nil
}

// GrantPartitions 建立一次临时授权：把 partitions 在 [start, expiry) 内
// 从 from 临时交给 to 处理，到期归还，不永久改变归属。
// revokeInFlight 为 false 表示等待旧处理完成（旧在途保留至其截止时间），
// 为 true 表示立即撤回旧处理权（旧在途被回收，旧 Token 即刻失效）。
// 同一分区上时间重叠的授权会被拒绝（ErrGrantConflict）。
// 返回授权 ID；授权范围、期限与交接原因记入消息历史。
func (b *Broker) GrantPartitions(topic, sub string, partitions []int, from, to, reason string, start, expiry time.Time, revokeInFlight bool) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return "", err
	}
	if len(partitions) == 0 || !expiry.After(start) {
		return "", ErrInvalidPartition
	}
	parts := append([]int(nil), partitions...)
	sort.Ints(parts)
	parts = dedupeInts(parts)
	for _, p := range parts {
		if p < 0 || p >= s.partitions {
			return "", ErrInvalidPartition
		}
	}
	// 重复交接检查：同分区且时间重叠的未关闭授权视为冲突。
	for _, g := range s.grants {
		if g.Cancelled || g.Returned {
			continue
		}
		for _, p := range parts {
			if g.covers(p) && g.StartsAt.Before(expiry) && start.Before(g.effectiveExpiry(p)) {
				return "", ErrGrantConflict
			}
		}
	}
	s.grantSeq++
	g := &Grant{
		ID:         fmt.Sprintf("g%d", s.grantSeq),
		Topic:      topic,
		Sub:        sub,
		Partitions: parts,
		From:       from,
		To:         to,
		Reason:     reason,
		StartsAt:   start,
		ExpiresAt:  expiry,
		CreatedAt:  b.now(),
		Revision:   1,
		Terms:      initialGrantTerms(parts, expiry),
	}
	s.grants[g.ID] = g
	if err := b.recordHistoryLocked(s, -1, EventGrant, to, 0, 0, grantDetail(g)); err != nil {
		return "", err
	}
	// 立即撤回旧处理权：回收这些分区上不属于新持有人的在途处理。
	if revokeInFlight && !g.StartsAt.After(b.now()) {
		if err := b.revokeInFlightLocked(s, g, g.To, "grant "+g.ID+" revoke"); err != nil {
			return "", err
		}
	}
	return g.ID, b.flushLocked()
}

func initialGrantTerms(parts []int, expiry time.Time) []GrantTerm {
	terms := make([]GrantTerm, 0, len(parts))
	for _, p := range parts {
		terms = append(terms, GrantTerm{Partition: p, ExpiresAt: expiry})
	}
	return terms
}

// PrepareGrantExpiryPreview 预演一次已生效临时授权的期限变更。
// partitions 为空表示调整整次授权的所有分区；未选择分区保留原期限。
// 缩短任何分区时，shortenedPolicy 必须显式选择 wait 或 revoke。
func (b *Broker) PrepareGrantExpiryPreview(topic, sub, grantID string, partitions []int, newExpiry time.Time, shortenedPolicy string) (GrantExpiryPreview, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, g, err := b.grantForExpiryChangeLocked(topic, sub, grantID)
	if err != nil {
		return GrantExpiryPreview{}, err
	}
	selected, err := b.selectedGrantPartitionsLocked(s, g, partitions)
	if err != nil {
		return GrantExpiryPreview{}, err
	}
	now := b.now()
	if newExpiry.Before(now) || newExpiry.Before(g.StartsAt) {
		return GrantExpiryPreview{}, ErrInvalidPartition
	}
	shortened := false
	for _, p := range selected {
		if !g.effectiveExpiry(p).After(now) {
			return GrantExpiryPreview{}, ErrInvalidPartition
		}
		if newExpiry.Before(g.effectiveExpiry(p)) {
			shortened = true
		}
	}
	if shortened {
		switch shortenedPolicy {
		case GrantExpiryWait, GrantExpiryRevoke:
		default:
			return GrantExpiryPreview{}, ErrGrantExpiryChoice
		}
	} else {
		shortenedPolicy = GrantExpiryKeep
	}
	action := GrantExpiryWait
	if !shortened {
		action = GrantExpiryKeep
	} else if shortenedPolicy == GrantExpiryRevoke {
		action = GrantExpiryRevoke
	}
	preview := GrantExpiryPreview{
		GrantID:         g.ID,
		Revision:        g.Revision,
		Partitions:      append([]int(nil), selected...),
		NewExpiresAt:    newExpiry,
		ShortenedPolicy: shortenedPolicy,
		Shortened:       shortened,
	}
	selectedSet := map[int]bool{}
	changes := make([]GrantTermChange, 0, len(selected))
	shortenedPartitions := map[int]bool{}
	for _, p := range selected {
		selectedSet[p] = true
		old := g.effectiveExpiry(p)
		changes = append(changes, GrantTermChange{Partition: p, OldExpiry: old, NewExpiry: preview.NewExpiresAt})
		if preview.NewExpiresAt.Before(old) {
			shortenedPartitions[p] = true
		}
	}
	msgs := b.topics[s.topic]
	for off, f := range s.inFlight {
		p := partitionOf(msgs[off].Key, s.partitions)
		if !selectedSet[p] {
			continue
		}
		oldExpiry := g.effectiveExpiry(p)
		if !shortened {
			action = GrantExpiryKeep
		} else if f.owner != g.To || !newExpiry.Before(oldExpiry) {
			action = GrantExpiryWait
		} else {
			action = shortenedPolicy
		}
		preview.InFlight = append(preview.InFlight, GrantExpiryInFlight{
			Offset:           off,
			Owner:            f.owner,
			Epoch:            f.epoch,
			Partition:        p,
			OldGrantExpiry:   oldExpiry,
			NewGrantExpiry:   newExpiry,
			DeliveryDeadline: f.deadline,
			Action:           action,
		})
	}
	sort.Slice(preview.InFlight, func(i, j int) bool { return preview.InFlight[i].Offset < preview.InFlight[j].Offset })
	return preview, nil
}

// ConfirmGrantExpiry 按预演提交期限变更。每次确认产生新的授权修订；
// 基于旧修订的确认会被拒绝，需要重新调用 PrepareGrantExpiryPreview。
func (b *Broker) ConfirmGrantExpiry(topic, sub string, preview GrantExpiryPreview) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, g, err := b.grantForExpiryChangeLocked(topic, sub, preview.GrantID)
	if err != nil {
		return err
	}
	if preview.Revision != g.Revision {
		return ErrGrantRevisionStale
	}
	selected, err := b.selectedGrantPartitionsLocked(s, g, preview.Partitions)
	if err != nil {
		return err
	}
	now := b.now()
	if preview.NewExpiresAt.Before(now) || preview.NewExpiresAt.Before(g.StartsAt) {
		return ErrInvalidPartition
	}
	shortened := false
	unchanged := true
	for _, p := range selected {
		old := g.effectiveExpiry(p)
		if !old.After(now) {
			return ErrInvalidPartition
		}
		if preview.NewExpiresAt.Before(old) {
			shortened = true
		}
		if !preview.NewExpiresAt.Equal(old) {
			unchanged = false
		}
	}
	if unchanged {
		return ErrGrantExpirySame
	}
	if shortened {
		switch preview.ShortenedPolicy {
		case GrantExpiryWait, GrantExpiryRevoke:
		default:
			return ErrGrantExpiryChoice
		}
	}
	selectedSet := map[int]bool{}
	changes := make([]GrantTermChange, 0, len(selected))
	shortenedPartitions := map[int]bool{}
	for _, p := range selected {
		selectedSet[p] = true
		old := g.effectiveExpiry(p)
		changes = append(changes, GrantTermChange{Partition: p, OldExpiry: old, NewExpiry: preview.NewExpiresAt})
		if preview.NewExpiresAt.Before(old) {
			shortenedPartitions[p] = true
		}
	}
	for _, other := range s.grants {
		if other == g || other.Cancelled || other.Returned {
			continue
		}
		for _, p := range selected {
			if other.covers(p) && other.StartsAt.Before(preview.NewExpiresAt) && g.StartsAt.Before(other.effectiveExpiry(p)) {
				return ErrGrantConflict
			}
		}
	}
	for i := range g.Terms {
		if selectedSet[g.Terms[i].Partition] {
			g.Terms[i].ExpiresAt = preview.NewExpiresAt
		}
	}
	latest := latestExpiry(g.Terms)
	if latest.After(g.ExpiresAt) {
		g.ExpiresAt = latest
	}
	g.Revision++
	rev := GrantRevision{
		Revision:        g.Revision,
		At:              now,
		Partitions:      append([]int(nil), selected...),
		Changes:         changes,
		NewExpiresAt:    preview.NewExpiresAt,
		ShortenedPolicy: preview.ShortenedPolicy,
	}
	g.Revisions = append(g.Revisions, rev)
	detail := grantExpiryDetail(g, rev, shortened)
	if err := b.recordHistoryLocked(s, -1, EventGrantExpiry, g.To, 0, 0, detail); err != nil {
		return err
	}
	if shortened && preview.ShortenedPolicy == GrantExpiryRevoke {
		revoke := *g
		revoke.Partitions = revoke.Partitions[:0]
		for p := range shortenedPartitions {
			revoke.Partitions = append(revoke.Partitions, p)
		}
		sort.Ints(revoke.Partitions)
		if err := b.revokeInFlightLocked(s, &revoke, "", "grant "+g.ID+" expiry revision "+fmt.Sprint(g.Revision)+" revoke"); err != nil {
			return err
		}
	}
	return b.flushLocked()
}

func (b *Broker) grantForExpiryChangeLocked(topic, sub, grantID string) (*subState, *Grant, error) {
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return nil, nil, err
	}
	g, ok := s.grants[grantID]
	if !ok {
		return nil, nil, ErrNoGrant
	}
	now := b.now()
	if g.Cancelled || g.Returned {
		return nil, nil, ErrGrantClosed
	}
	if g.StartsAt.After(now) || !g.active(now) {
		return nil, nil, ErrGrantStarted
	}
	return s, g, nil
}

func (b *Broker) selectedGrantPartitionsLocked(s *subState, g *Grant, partitions []int) ([]int, error) {
	if len(partitions) == 0 {
		return append([]int(nil), g.Partitions...), nil
	}
	parts := append([]int(nil), partitions...)
	sort.Ints(parts)
	parts = dedupeInts(parts)
	for _, p := range parts {
		if p < 0 || p >= s.partitions || !g.covers(p) {
			return nil, ErrInvalidPartition
		}
	}
	return parts, nil
}

func latestExpiry(terms []GrantTerm) time.Time {
	var latest time.Time
	for _, term := range terms {
		if latest.IsZero() || term.ExpiresAt.After(latest) {
			latest = term.ExpiresAt
		}
	}
	return latest
}

// revokeInFlightLocked 回收授权分区上不属于 keepOwner 的在途处理，
// 旧 Token 即刻失效（ErrStaleToken），不会误伤新持有人。调用方须持有锁。
func (b *Broker) revokeInFlightLocked(s *subState, g *Grant, keepOwner, detail string) error {
	msgs := b.topics[s.topic]
	for off, f := range s.inFlight {
		if f.owner == keepOwner {
			continue
		}
		if !g.covers(partitionOf(msgs[off].Key, s.partitions)) {
			continue
		}
		if err := b.recordHistoryLocked(s, off, EventRevoke, f.owner, f.epoch, s.failures[off]+1, detail); err != nil {
			return err
		}
		delete(s.inFlight, off)
	}
	return nil
}

// CancelGrant 取消一个尚未开始的临时授权；已经开始的部分保留事实，
// 已开始或已关闭的授权不能取消（ErrGrantStarted / ErrGrantClosed）。
func (b *Broker) CancelGrant(topic, sub, grantID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return err
	}
	g, ok := s.grants[grantID]
	if !ok {
		return ErrNoGrant
	}
	if g.Cancelled || g.Returned {
		return ErrGrantClosed
	}
	if !g.StartsAt.After(b.now()) {
		return ErrGrantStarted
	}
	g.Cancelled = true
	if err := b.recordHistoryLocked(s, -1, EventGrantCancel, g.To, 0, 0, grantDetail(g)); err != nil {
		return err
	}
	return b.flushLocked()
}

// ReturnGrant 归还一个临时授权：授权不再有效，分区归还原处理者。
// 归还时可能仍有在途处理：terminate 为 false 表示等待收尾
// （在途保留至其截止时间，迟到确认仍按当时有效归属判断），
// 为 true 表示立即终止旧授权（在途回收，旧 Token 即刻失效）。
func (b *Broker) ReturnGrant(topic, sub, grantID string, terminate bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return err
	}
	g, ok := s.grants[grantID]
	if !ok {
		return ErrNoGrant
	}
	if g.Cancelled || g.Returned {
		return ErrGrantClosed
	}
	g.Returned = true
	g.ReturnedAt = b.now()
	g.Terminated = terminate
	detail := grantDetail(g)
	if terminate {
		detail += " terminate=true"
	}
	if err := b.recordHistoryLocked(s, -1, EventGrantReturn, g.To, 0, 0, detail); err != nil {
		return err
	}
	if terminate {
		if err := b.revokeInFlightLocked(s, g, "", "grant "+g.ID+" return terminate"); err != nil {
			return err
		}
	}
	return b.flushLocked()
}

// Grants 返回订阅的全部临时授权（含已取消、已归还），按建立顺序。
func (b *Broker) Grants(topic, sub string) ([]Grant, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return nil, err
	}
	var out []Grant
	for _, g := range s.grants {
		out = append(out, *g)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// activeGrantLocked 返回 partition 在 now 时刻的有效授权，无则 nil。调用方须持有锁。
func (b *Broker) activeGrantLocked(s *subState, partition int, now time.Time) *Grant {
	var best *Grant
	for _, g := range s.grants {
		if !g.partitionActive(partition, now) {
			continue
		}
		if best == nil || g.ID < best.ID {
			best = g
		}
	}
	return best
}

func grantDetail(g *Grant) string {
	parts := make([]string, len(g.Partitions))
	for i, p := range g.Partitions {
		parts[i] = fmt.Sprint(p)
	}
	return fmt.Sprintf("partitions=[%s] from=%s to=%s reason=%s start=%s expiry=%s",
		strings.Join(parts, ","), g.From, g.To, g.Reason,
		g.StartsAt.Format(time.RFC3339Nano), g.ExpiresAt.Format(time.RFC3339Nano))
}

func grantExpiryDetail(g *Grant, rev GrantRevision, shortened bool) string {
	parts := make([]string, len(rev.Partitions))
	for i, p := range rev.Partitions {
		parts[i] = fmt.Sprint(p)
	}
	direction := "extend"
	if shortened {
		direction = "shorten"
	}
	policy := rev.ShortenedPolicy
	if policy == "" {
		policy = GrantExpiryKeep
	}
	old := make([]string, 0, len(rev.Changes))
	newParts := make([]string, 0, len(rev.Changes))
	for _, change := range rev.Changes {
		old = append(old, fmt.Sprintf("%d:%s", change.Partition, change.OldExpiry.Format(time.RFC3339Nano)))
		newParts = append(newParts, fmt.Sprintf("%d:%s", change.Partition, change.NewExpiry.Format(time.RFC3339Nano)))
	}
	return fmt.Sprintf("grant=%s revision=%d partitions=[%s] direction=%s old_partition_expiry=[%s] new_partition_expiry=[%s] policy=%s",
		g.ID, rev.Revision, strings.Join(parts, ","), direction,
		strings.Join(old, ","), strings.Join(newParts, ","), policy)
}

func dedupeInts(in []int) []int {
	out := in[:0]
	for i, v := range in {
		if i == 0 || v != in[i-1] {
			out = append(out, v)
		}
	}
	return out
}
