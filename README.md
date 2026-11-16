# 展品体验事件湖

本项目使用 FastAPI 和 SQLite 管理场馆、展品、来源契约与事件批次。原始事件保留接收证据，正式报表只读取已发布的日期快照。

## 发布前质量门禁

每个场馆可配置质量规则（`PUT /venues/{venue_id}/quality-policy`），例如要求某个来源或某种事件类型必须出现在候选快照中（`require_sources` / `require_kinds`）、要求所有启用来源都有数据（`require_active_source_coverage`）、限制未发布事件数、拒收行数、拒收比例或按拒收代码设限。默认策略为空，行为与旧版本一致。

- `GET /snapshots/{snapshot_id}/quality` 评估确切候选快照：来源覆盖、当日已接收批次与拒收分布、命中规则。
- 评估结论绑定 `state_digest`（快照成员摘要、当日事件与批次、来源与契约状态、分区 head、策略修订）。发布时若质量不通过会被 409 `quality_gate_failed` 拒绝；新批次、head 前进或策略调整后，旧评估和旧审批自动失效。
- 场馆授权人员（`POST /venues/{venue_id}/quality-approvers`）可用 `POST /snapshots/{snapshot_id}/quality/approval` 提交理由批准例外；也可在发布请求中同时带上 `approver_id` 与 `reason`。审批只对同一 `state_digest` 生效，发布时会记录所用评估与审批。
- `GET /venues/{venue_id}/days/{local_day}/gate` 汇总发布前视图（已发布 head 与暂存候选的覆盖、拒收、门禁结论）。
- 报表返回 `quality` 字段，说明该快照是否覆盖反馈来源、是否带例外审批，避免把“没有反馈数据”误读成“没有人愿意评分”。

## 运行

在预装依赖的环境中，从项目根目录执行：

    uvicorn exhibit_lake.api:app --host 0.0.0.0 --port 8000

默认数据库为本地 SQLite 文件；用 EXHIBIT_LAKE_DB 可指定其他路径。

## 测试与构建

    python -m unittest discover -s tests -v
    python -m compileall -q exhibit_lake
    python -m exhibit_lake.demo

以上三条命令依次运行业务测试、编译检查和本地合成数据验收。依赖版本记录在 requirements.txt。
