# 消息投递运行系统（第二阶段）

本地持久化消息投递：主题、订阅、消费位置、独占投递与确认，以及延迟投递、失败退避、隔离与人工重新投递、全量消息历史；支持分区级临时授权（交接与归还）。生产与消费均由项目内测试端（broker/ 下 *_test.go）完成，不接中间件。路由能力留待后续阶段。

## 结构

- broker/broker.go — 主题/订阅/消费位置、Pull/Ack、超时回收与纪元 fencing
- broker/grant.go — 分区与临时授权：积压视图、交接、期限变更预演、取消、归还
- broker/store.go — 磁盘持久化与重启恢复
- broker/broker_test.go — 测试端（生产者 + 多消费者场景）

## 持久化布局

- topics/<topic>.jsonl — 消息日志（追加写 + fsync）
- subs.json — 订阅、已确认偏移、失败计数、退避截止、隔离与放弃集合（临时文件 + rename 原子重写）
- acks.jsonl — 每条消息的实际去向记录（谁、何时、以何纪元确认）
- history.jsonl — 消息历史：每一次领取、超时、拒绝、确认、隔离、人工重投各一条（追加写 + fsync）

## 语义

- 每条消息同一时刻只有一个当前持有人（可见期内其他消费者拉不到）。
- 超时未确认即回收转交，纪元递增；旧持有人的 Token 立即失效（ErrStaleToken），不会误伤新持有人。
- 确认写盘后才算完成；普通重启后已确认消息不会重来，未确认消息允许重新投递。
- PublishDelayed 延迟投递：到期前对消费者不可见。
- Nack 拒绝投递：进入退避（backoffBase * 2^(n-1)，SetRetryPolicy 可调），退避结束自动重新投递，Attempt 递增。
- 失败次数达上限的消息被隔离：不再自动投递，但不阻塞同订阅后续可处理消息；隔离不等于业务确认完成。
- 消费位置分开表示：Position 是连续已确认前缀（隔离/放弃不推进它，未完成的空洞不会被掩盖）；DeliverablePosition 是连续已处理前缀（已确认、已隔离或已放弃，投递可越过）。两者之差即尚未确认完成的空洞，可用 Quarantined / Abandoned / History 解释。
- Redeliver 人工重投被隔离消息：保留原消息身份（主题/偏移/内容），清除失败计数，下一次 Pull 以新纪元、新尝试投递，操作者记入历史。
- Abandon 人工放弃被隔离消息：终态，不再投递也不算确认；操作者记入历史，DeliverablePosition 据此推进，Position 不推进。
- 每一次领取、超时、拒绝、确认、隔离、人工重投都留在消息历史（History）里；最终确认者（AckedBy）不代替全部尝试记录。
- 旧领取令牌迟到、重启后再次投递、重投失败都不会重复确认（ErrStaleToken / ErrAlreadyAcked）。
- Counts 分别核对已交付（按历史去重）、已确认、隔离、放弃与仍待处理的消息数量；重启后可用 AckedBy / History / Quarantined / Abandoned / Counts 核对每条消息的实际去向与全部尝试。
- 分区按消息 Key 哈希归属（SetPartitions 设置分区数，默认 1）。GrantPartitions 把指定分区在 [start, expiry) 内临时交给另一名逻辑消费者，到期归还，不永久改变归属；未选分区继续原处理。
- 已生效授权可用 PreviewGrantExpiryChange 预演期限变更：partitions 为空表示整次授权，传部分分区时仅这些分区改变，未选分区保持原期限。预演返回受影响在途消息的原期限、新期限、持有人、尝试、投递截止时间及处理方式。
- 延长授权的在途处理继续使用原投递；缩短授权必须明确选择 GrantExpiryWaitCurrentCompletion（等待当前处理完成，不把已有处理自动算作成功）或 GrantExpiryRevokeProcessing（到期撤销处理权，旧 Token 失效并重新投递）。
- ConfirmGrantExpiryChange 只能提交仍匹配当前授权修订和在途快照的预演；提交成功生成新的 GrantRevision。旧修订、预演后在途状态变化、重复提交都会收到 ErrGrantExpiryPreviewStale，需要重新查看最新预演。CancelGrantExpiryPreview 只撤销预演，不改变授权。
- 每次期限变更的操作者、原因、实际生效分区、原期限、新期限与缩短处理方式写入 Grant.Revisions 和 history；关闭重开后仍可追溯。
- 交接前可用 Backlog 查看指定分区尚未完成（在途）、等待重投（退避中）、已隔离与仍待投递的消息；交接时可明确选择等待旧处理完成（revokeInFlight=false）或立即撤回旧处理权（revokeInFlight=true，旧 Token 即刻失效）。
- 归还（ReturnGrant）时可能仍有在途处理：terminate=false 等待收尾（在途保留至其截止时间，归还与确认同刻发生时当时有效归属仍认可该确认），terminate=true 立即终止旧授权（在途回收，旧 Token 即刻失效）。新旧消费者的迟到确认与重投均按当时有效归属判断（纪元 fencing），同一条消息不会重复生效。
- 尚未开始的临时授权可取消（CancelGrant），已经开始的部分保留事实；同一分区上时间重叠的重复交接被拒绝（ErrGrantConflict）。
- 授权范围、期限与交接原因随消息历史保留（grant / grant_cancel / grant_return / revoke 事件，授权期内的投递记录带 grant 与 reason），隔离消息形成的连续确认缺口不被交接或归还抹平；重启后授权与积压视图照常恢复。

## 测试

    go test ./...

覆盖：确认后重启不重投、崩溃后重投、独占持有、超时转交与旧确认拒绝、重启核对去向、消息内容持久化、延迟投递、失败退避与重试、隔离不阻塞后续消息、人工重投保留原身份、历史跨重启完整、确认边界重启 + 消费者换人 + 旧令牌迟到时的顺序与消费位置、确认位置与可投递位置分离、人工放弃的进度推进、重投失败与旧令牌不重复确认、数量核对跨重启一致、交接前积压视图、撤回旧处理权与迟到确认、未选分区继续原处理、连续两次临时授权、归还与确认同刻发生（等待收尾 / 立即终止）、重复交接拒绝、取消未开始的授权、退出后重开授权恢复、期限延长/缩短与部分分区预演、等待当前处理/撤销处理权、旧修订和在途快照失效、撤销预演不改授权、期限修订跨重启追溯。

## 后续

- 路由能力（按 key/规则分发到不同订阅）。
