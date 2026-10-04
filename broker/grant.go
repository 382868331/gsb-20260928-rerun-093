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
	EventGrant             = "grant"               // 建立临时授权（交接）
	EventGrantCancel       = "grant_cancel"        // 取消尚未开始的临时授权
	EventGrantReturn       = "grant_return"        // 授权到期归还
	EventGrantExpiryChange = "grant_expiry_change" // 授权期限变更
	EventRevoke            = "revoke"              // 在途处理权被撤回（交接、归还或缩短期限时终止）
)

var (
	ErrNoGrant                  = errors.New("broker: no such grant")
	ErrGrantConflict            = errors.New("broker: partition already covered by an overlapping grant")
	ErrGrantStarted             = errors.New("broker: grant already started, cannot cancel")
	ErrGrantClosed              = errors.New("broker: grant already cancelled or returned")
	ErrGrantNotActive           = errors.New("broker: grant is not active")
	ErrInvalidPartition         = errors.New("broker: invalid partition")
	ErrInvalidExpiryChange      = errors.New("broker: invalid grant expiry change")
	ErrGrantExpiryPreviewStale  = errors.New("broker: grant expiry preview is stale, review the latest preview")
	ErrGrantExpiryPreviewClosed = errors.New("broker: grant expiry preview was already confirmed or cancelled")
)

type GrantExpiryShortenPolicy string

const (
	GrantExpiryWaitCurrentCompletion GrantExpiryShortenPolicy = "wait_current_completion"
	GrantExpiryRevokeProcessing      GrantExpiryShortenPolicy = "revoke_processing"
)

const (
	grantExpiryExtend = "extend_authorization"
	grantExpiryWait   = "wait_current_completion"
	grantExpiryRevoke = "revoke_processing"
)

// Grant 是一次临时授权：把指定分区在 [StartsAt, ExpiresAt) 内
// 从原处理者 From 临时交给 To 处理，到期归还，不永久改变归属。
// 授权范围、期限与交接原因随消息历史保留。
type Grant struct {
	ID                string            `json:"id"`
	Topic             string            `json:"topic"`
	Sub               string            `json:"sub"`
	Partitions        []int             `json:"partitions"`
	From              string            `json:"from"`
	To                string            `json:"to"`
	Reason            string            `json:"reason"`
	StartsAt          time.Time         `json:"starts_at"`
	ExpiresAt         time.Time         `json:"expires_at"`
	CreatedAt         time.Time         `json:"created_at"`
	Revision          uint64            `json:"revision"`
	PartitionExpiries map[int]time.Time `json:"partition_expiries,omitempty"`
	PartitionRevokeAt map[int]time.Time `json:"partition_revoke_at,omitempty"`
	Revisions         []GrantRevision   `json:"revisions,omitempty"`
	Cancelled         bool              `json:"cancelled,omitempty"`
	Returned          bool              `json:"returned,omitempty"`
	ReturnedAt        time.Time         `json:"returned_at,omitempty"`
	Terminated        bool              `json:"terminated,omitempty"` // 归还时立即终止在途处理
}

// GrantRevision 记录一次授权期限修订及其实际生效分区。
type GrantRevision struct {
	Revision      uint64            `json:"revision"`
	At            time.Time         `json:"at"`
	Operator      string            `json:"operator,omitempty"`
	Reason        string            `json:"reason,omitempty"`
	Partitions    []int             `json:"partitions"`
	OldExpiries   map[int]time.Time `json:"old_expiries,omitempty"`
	NewExpiries   map[int]time.Time `json:"new_expiries"`
	ShortenPolicy string            `json:"shorten_policy,omitempty"`
}

// GrantExpiryImpact 是预演中一条受影响的在途消息。
type GrantExpiryImpact struct {
	Offset           int64
	Partition        int
	Owner            string
	Epoch            uint64
	DeliveryDeadline time.Time
	Attempt          int
	OldExpiresAt     time.Time
	NewExpiresAt     time.Time
	Action           string
}

// GrantExpiryPreview 是提交前的期限变更预演。
type GrantExpiryPreview struct {
	ID            string
	GrantID       string
	BaseRevision  uint64
	Operator      string
	Reason        string
	Partitions    []int
	NewExpiresAt  time.Time
	ShortenPolicy GrantExpiryShortenPolicy
	Impacts       []GrantExpiryImpact
	CreatedAt     time.Time
}

type grantExpiryPreviewState struct {
	p          GrantExpiryPreview
	signature  string
	cancelled  bool
	superseded bool
}

// active 判断授权在 t 时刻是否有效（已生效、未到期、未取消、未归还）。
func (g *Grant) active(t time.Time) bool {
	return !g.Cancelled && !g.Returned && !g.StartsAt.After(t) && t.Before(g.expiry())
}

func (g *Grant) activePartition(partition int, t time.Time) bool {
	return g.active(t) && g.covers(partition) && t.Before(g.partitionExpiry(partition))
}

func (g *Grant) expiry() time.Time {
	var out time.Time
	for _, p := range g.Partitions {
		t := g.partitionExpiry(p)
		if out.IsZero() || t.After(out) {
			out = t
		}
	}
	if out.IsZero() {
		return g.ExpiresAt
	}
	return out
}

func (g *Grant) partitionExpiry(partition int) time.Time {
	if g.PartitionExpiries != nil {
		if t, ok := g.PartitionExpiries[partition]; ok {
			return t
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
			partitionExpiry := g.partitionExpiry(p)
			if !g.StartsAt.Before(expiry) || !start.Before(partitionExpiry) {
				continue
			}
			if g.covers(p) {
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
	}
	initialExpiries := map[int]time.Time{}
	for _, p := range parts {
		initialExpiries[p] = expiry
	}
	g.Revisions = []GrantRevision{{
		Revision:    1,
		At:          b.now(),
		Partitions:  append([]int(nil), parts...),
		NewExpiries: initialExpiries,
	}}
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

// PreviewGrantExpiryChange 预演一次已生效授权的期限变更。partitions 为空表示整次授权；
// 未选择的分区保持原期限。缩短期限时必须明确选择等待当前处理完成或撤销处理权。
func (b *Broker) PreviewGrantExpiryChange(topic, sub, grantID, operator, reason string, partitions []int, newExpiry time.Time, shortenPolicy GrantExpiryShortenPolicy) (GrantExpiryPreview, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return GrantExpiryPreview{}, err
	}
	g, parts, err := b.prepareGrantExpiryChangeLocked(s, grantID, partitions, newExpiry, shortenPolicy)
	if err != nil {
		return GrantExpiryPreview{}, err
	}
	impacts, err := b.grantExpiryImpactsLocked(s, g, parts, newExpiry, shortenPolicy)
	if err != nil {
		return GrantExpiryPreview{}, err
	}
	s.grantPreviewSeq++
	p := GrantExpiryPreview{
		ID:            fmt.Sprintf("gep%d", s.grantPreviewSeq),
		GrantID:       grantID,
		BaseRevision:  g.Revision,
		Operator:      operator,
		Reason:        reason,
		Partitions:    parts,
		NewExpiresAt:  newExpiry,
		ShortenPolicy: shortenPolicy,
		Impacts:       impacts,
		CreatedAt:     b.now(),
	}
	s.grantPreviews[p.ID] = &grantExpiryPreviewState{p: p, signature: grantExpirySignature(p)}
	return p, nil
}

// ConfirmGrantExpiryChange 提交期限变更预演。每次成功提交都会生成新的授权修订；
// 基于旧修订、已撤销或在途状态已变化的预演会被拒绝，需要重新查看最新预演。
func (b *Broker) ConfirmGrantExpiryChange(topic, sub, previewID string) (GrantRevision, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return GrantRevision{}, err
	}
	state, ok := s.grantPreviews[previewID]
	if !ok {
		return GrantRevision{}, ErrGrantExpiryPreviewStale
	}
	p := state.p
	g, ok := s.grants[p.GrantID]
	if !ok {
		return GrantRevision{}, ErrNoGrant
	}
	if state.cancelled || state.superseded {
		return GrantRevision{}, ErrGrantExpiryPreviewClosed
	}
	if g.Revision != p.BaseRevision {
		return GrantRevision{}, ErrGrantExpiryPreviewStale
	}
	if err := b.enforceGrantExpiriesLocked(s, b.now()); err != nil {
		return GrantRevision{}, err
	}
	currentImpacts, err := b.grantExpiryImpactsLocked(s, g, p.Partitions, p.NewExpiresAt, p.ShortenPolicy)
	if err != nil {
		return GrantRevision{}, err
	}
	current := p
	current.Impacts = currentImpacts
	if grantExpirySignature(current) != state.signature {
		return GrantRevision{}, ErrGrantExpiryPreviewStale
	}
	parts, err := b.validateGrantExpiryPartitionsLocked(s, g, p.Partitions, p.NewExpiresAt, p.ShortenPolicy)
	if err != nil {
		return GrantRevision{}, err
	}
	oldExpiries := map[int]time.Time{}
	newExpiries := map[int]time.Time{}
	shortened := false
	for _, part := range parts {
		old := g.partitionExpiry(part)
		oldExpiries[part] = old
		newExpiries[part] = p.NewExpiresAt
		if p.NewExpiresAt.Before(old) {
			shortened = true
		}
		if g.PartitionExpiries == nil {
			g.PartitionExpiries = map[int]time.Time{}
		}
		g.PartitionExpiries[part] = p.NewExpiresAt
	}
	g.ExpiresAt = g.expiry()
	g.Revision++
	rev := GrantRevision{
		Revision:    g.Revision,
		At:          b.now(),
		Operator:    p.Operator,
		Reason:      p.Reason,
		Partitions:  parts,
		OldExpiries: oldExpiries,
		NewExpiries: newExpiries,
	}
	if shortened && p.ShortenPolicy == GrantExpiryRevokeProcessing {
		rev.ShortenPolicy = grantExpiryRevoke
		if g.PartitionRevokeAt == nil {
			g.PartitionRevokeAt = map[int]time.Time{}
		}
		for _, part := range parts {
			if p.NewExpiresAt.Before(oldExpiries[part]) {
				g.PartitionRevokeAt[part] = p.NewExpiresAt
			}
		}
	} else if shortened {
		rev.ShortenPolicy = grantExpiryWait
	}
	g.Revisions = append(g.Revisions, rev)
	delete(s.grantPreviews, previewID)
	if err := b.recordHistoryLocked(s, -1, EventGrantExpiryChange, p.Operator, 0, 0, grantExpiryChangeDetail(g, rev)); err != nil {
		return GrantRevision{}, err
	}
	if err := b.enforceGrantExpiriesLocked(s, b.now()); err != nil {
		return GrantRevision{}, err
	}
	if err := b.flushLocked(); err != nil {
		return GrantRevision{}, err
	}
	return rev, nil
}

// CancelGrantExpiryPreview 撤销尚未提交的期限变更预演，不改变授权。
func (b *Broker) CancelGrantExpiryPreview(topic, sub, previewID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	s, err := b.subLocked(topic, sub)
	if err != nil {
		return err
	}
	state, ok := s.grantPreviews[previewID]
	if !ok {
		return ErrGrantExpiryPreviewStale
	}
	state.cancelled = true
	delete(s.grantPreviews, previewID)
	return b.flushLocked()
}

func (b *Broker) prepareGrantExpiryChangeLocked(s *subState, grantID string, partitions []int, newExpiry time.Time, shortenPolicy GrantExpiryShortenPolicy) (*Grant, []int, error) {
	g, ok := s.grants[grantID]
	if !ok {
		return nil, nil, ErrNoGrant
	}
	parts, err := b.validateGrantExpiryPartitionsLocked(s, g, partitions, newExpiry, shortenPolicy)
	return g, parts, err
}

func (b *Broker) validateGrantExpiryPartitionsLocked(s *subState, g *Grant, partitions []int, newExpiry time.Time, shortenPolicy GrantExpiryShortenPolicy) ([]int, error) {
	if g.Cancelled || g.Returned || !g.active(b.now()) {
		return nil, ErrGrantNotActive
	}
	if !newExpiry.After(g.StartsAt) {
		return nil, ErrInvalidExpiryChange
	}
	parts := append([]int(nil), g.Partitions...)
	if len(partitions) > 0 {
		parts = append([]int(nil), partitions...)
		sort.Ints(parts)
		parts = dedupeInts(parts)
	}
	shortened := false
	for _, part := range parts {
		if part < 0 || part >= s.partitions || !g.covers(part) {
			return nil, ErrInvalidPartition
		}
		if !g.activePartition(part, b.now()) {
			return nil, ErrGrantNotActive
		}
		old := g.partitionExpiry(part)
		if newExpiry.Equal(old) {
			return nil, ErrInvalidExpiryChange
		}
		if newExpiry.Before(old) {
			shortened = true
		}
	}
	if shortened && shortenPolicy != GrantExpiryWaitCurrentCompletion && shortenPolicy != GrantExpiryRevokeProcessing {
		return nil, ErrInvalidExpiryChange
	}
	return parts, nil
}

func (b *Broker) grantExpiryImpactsLocked(s *subState, g *Grant, parts []int, newExpiry time.Time, shortenPolicy GrantExpiryShortenPolicy) ([]GrantExpiryImpact, error) {
	selected := map[int]bool{}
	for _, part := range parts {
		selected[part] = true
	}
	msgs := b.topics[s.topic]
	var impacts []GrantExpiryImpact
	for off, f := range s.inFlight {
		part := partitionOf(msgs[off].Key, s.partitions)
		if !selected[part] {
			continue
		}
		old := g.partitionExpiry(part)
		action := grantExpiryExtend
		if newExpiry.Before(old) {
			if shortenPolicy == GrantExpiryRevokeProcessing {
				action = grantExpiryRevoke
			} else {
				action = grantExpiryWait
			}
		}
		impacts = append(impacts, GrantExpiryImpact{
			Offset:           off,
			Partition:        part,
			Owner:            f.owner,
			Epoch:            f.epoch,
			DeliveryDeadline: f.deadline,
			Attempt:          s.failures[off] + 1,
			OldExpiresAt:     old,
			NewExpiresAt:     newExpiry,
			Action:           action,
		})
	}
	sort.Slice(impacts, func(i, j int) bool { return impacts[i].Offset < impacts[j].Offset })
	return impacts, nil
}

func (b *Broker) enforceGrantExpiriesLocked(s *subState, now time.Time) error {
	msgs := b.topics[s.topic]
	for _, g := range s.grants {
		if g.Cancelled || g.Returned || g.PartitionRevokeAt == nil {
			continue
		}
		for off, f := range s.inFlight {
			part := partitionOf(msgs[off].Key, s.partitions)
			dueAt, due := g.PartitionRevokeAt[part]
			if !due || dueAt.After(now) || !g.covers(part) {
				continue
			}
			if err := b.recordHistoryLocked(s, off, EventRevoke, f.owner, f.epoch, s.failures[off]+1, "grant "+g.ID+" expiry shorten revoke"); err != nil {
				return err
			}
			delete(s.inFlight, off)
		}
	}
	return nil
}

func grantExpirySignature(p GrantExpiryPreview) string {
	parts := make([]string, 0, len(p.Impacts))
	for _, impact := range p.Impacts {
		parts = append(parts, fmt.Sprintf("%d:%d:%s:%d:%s", impact.Offset, impact.Partition, impact.Owner, impact.Epoch, impact.DeliveryDeadline.Format(time.RFC3339Nano)))
	}
	return fmt.Sprintf("%d|%v|%s|%s|%s", p.BaseRevision, p.Partitions, p.NewExpiresAt.Format(time.RFC3339Nano), p.ShortenPolicy, strings.Join(parts, ","))
}

func grantExpiryChangeDetail(g *Grant, rev GrantRevision) string {
	return fmt.Sprintf("grant=%s revision=%d partitions=%v old=%v new=%v policy=%s operator=%s reason=%s",
		g.ID, rev.Revision, rev.Partitions, rev.OldExpiries, rev.NewExpiries, rev.ShortenPolicy, rev.Operator, rev.Reason)
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
		if !g.activePartition(partition, now) {
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

func dedupeInts(in []int) []int {
	out := in[:0]
	for i, v := range in {
		if i == 0 || v != in[i-1] {
			out = append(out, v)
		}
	}
	return out
}
