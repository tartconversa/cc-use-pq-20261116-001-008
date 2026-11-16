"""候选快照的发布前质量门禁、例外审批与状态绑定。

质量评估始终针对一个确切的候选快照。快照成员、该日期已接收的事件与批次、
拒收分布、来源登记与契约、分区 head，以及质量策略修订共同构成
``state_digest``。任一输入变化（新批次、新事件、head 前进、来源启停、
契约修订、策略调整）都会让旧评估与旧审批失效，审批不能套用到新内容上。
"""
from __future__ import annotations

import json
import uuid
from datetime import date, datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

from exhibit_lake.catalog.registry import KINDS, iso_day, now, require_id
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import (Conflict, InvalidInput, NotFound, NotAuthorized,
                                 QualityGateFailed, StateError)
from exhibit_lake.ingest.validation import digest


POLICY_KEYS = {
    "require_sources", "require_kinds", "require_active_source_coverage",
    "require_contract_coverage", "min_events", "max_pending_events",
    "max_rejected_rows", "max_rejected_ratio", "max_rejected_by_code",
}


def _default_rules() -> dict:
    return {"require_sources": [], "require_kinds": [],
            "require_active_source_coverage": False, "require_contract_coverage": False,
            "min_events": 0, "max_pending_events": None, "max_rejected_rows": None,
            "max_rejected_ratio": None, "max_rejected_by_code": {}}


def _normalize_rules(rules: object) -> dict:
    if not isinstance(rules, dict):
        raise InvalidInput("quality policy rules must be an object")
    unknown = sorted(set(rules) - POLICY_KEYS)
    if unknown:
        raise InvalidInput("unknown quality policy rules", details={"unknown": unknown})
    normalized = _default_rules()
    sources = rules.get("require_sources", [])
    if not isinstance(sources, list) or len(sources) != len(set(sources)):
        raise InvalidInput("require_sources must be a list of unique source identifiers")
    normalized["require_sources"] = sorted(require_id(x, "require_sources") for x in sources)
    kinds = rules.get("require_kinds", [])
    if not isinstance(kinds, list) or len(kinds) != len(set(kinds)):
        raise InvalidInput("require_kinds must be a list of unique kinds")
    if any(kind not in KINDS for kind in kinds):
        raise InvalidInput("require_kinds contains an unknown source kind")
    normalized["require_kinds"] = sorted(kinds)
    flag = rules.get("require_active_source_coverage", False)
    if not isinstance(flag, bool):
        raise InvalidInput("require_active_source_coverage must be a boolean")
    normalized["require_active_source_coverage"] = flag
    contract_flag = rules.get("require_contract_coverage", False)
    if not isinstance(contract_flag, bool):
        raise InvalidInput("require_contract_coverage must be a boolean")
    normalized["require_contract_coverage"] = contract_flag
    for key in ("min_events", "max_pending_events", "max_rejected_rows"):
        if key not in rules:
            continue
        value = rules[key]
        if value is None and key != "min_events":
            continue
        if type(value) is not int or value < 0:
            raise InvalidInput(f"{key} must be a nonnegative integer or null")
        normalized[key] = value
    ratio = rules.get("max_rejected_ratio")
    if ratio is not None:
        if isinstance(ratio, bool) or not isinstance(ratio, (int, float)) or not 0 <= ratio <= 1:
            raise InvalidInput("max_rejected_ratio must be between 0 and 1")
        normalized["max_rejected_ratio"] = float(ratio)
    by_code = rules.get("max_rejected_by_code", {})
    if not isinstance(by_code, dict):
        raise InvalidInput("max_rejected_by_code must be an object")
    checked = {}
    for code, limit in by_code.items():
        if not isinstance(code, str) or not 1 <= len(code) <= 80:
            raise InvalidInput("rejection code must be a short string")
        if type(limit) is not int or limit < 0:
            raise InvalidInput("rejection code limits must be nonnegative integers")
        checked[code] = limit
    normalized["max_rejected_by_code"] = dict(sorted(checked.items()))
    return normalized


def _utc_window(day: str, timezone_name: str) -> tuple[str, str]:
    zone = ZoneInfo(timezone_name)
    start = datetime.combine(date.fromisoformat(day), time.min, tzinfo=zone).astimezone(timezone.utc)
    return start.isoformat(), (start + timedelta(days=1)).isoformat()


class QualityGate:
    """场馆级可配置质量规则、候选评估和授权例外审批。"""

    def __init__(self, store: Store):
        self.store = store

    # ------------------------------------------------------------------ policy
    def policy(self, venue_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            return self._policy(conn, venue_id)

    def _policy(self, conn, venue_id: str) -> dict:
        if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
            raise NotFound("venue not found")
        row = Store.one(conn, "SELECT * FROM quality_policies WHERE venue_id=?", (venue_id,))
        if not row:
            revision, rules, updated_at = 0, _default_rules(), None
        else:
            revision = row["revision"]
            rules = _normalize_rules(json.loads(row["policy_json"]))
            updated_at = row["updated_at"]
        return {"venue_id": venue_id, "revision": revision, "rules": rules,
                "digest": digest({"revision": revision, "rules": rules}),
                "updated_at": updated_at}

    def set_policy(self, venue_id: str, rules: dict,
                   expected_revision: int | None = None) -> dict:
        normalized = _normalize_rules(rules)
        if expected_revision is not None and (type(expected_revision) is not int or expected_revision < 0):
            raise InvalidInput("expected_revision must be a nonnegative integer")
        with self.store.transaction() as conn:
            current = self._policy(conn, venue_id)
            if expected_revision is not None and expected_revision != current["revision"]:
                raise Conflict("quality policy revision changed",
                               details={"revision": current["revision"]})
            revision = current["revision"] + 1
            stamp = now()
            conn.execute("INSERT INTO quality_policies(venue_id,revision,policy_json,updated_at) "
                         "VALUES(?,?,?,?) ON CONFLICT(venue_id) DO UPDATE SET "
                         "revision=excluded.revision,policy_json=excluded.policy_json,"
                         "updated_at=excluded.updated_at",
                         (venue_id, revision, json.dumps(normalized, sort_keys=True), stamp))
            policy = {"venue_id": venue_id, "revision": revision, "rules": normalized,
                      "digest": digest({"revision": revision, "rules": normalized}),
                      "updated_at": stamp}
            Store.audit(conn, "quality.policy_updated", venue_id,
                        {"revision": revision, "digest": policy["digest"]}, stamp)
            return policy

    # --------------------------------------------------------------- approvers
    def grant_approver(self, venue_id: str, approver_id: str) -> dict:
        require_id(approver_id, "approver_id")
        with self.store.transaction() as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            existing = Store.one(conn, "SELECT * FROM quality_approvers "
                                 "WHERE venue_id=? AND approver_id=?", (venue_id, approver_id))
            stamp = now()
            if existing and existing["revoked_at"] is None:
                raise Conflict("approver already authorized")
            if existing:
                conn.execute("UPDATE quality_approvers SET granted_at=?,revoked_at=NULL "
                             "WHERE venue_id=? AND approver_id=?", (stamp, venue_id, approver_id))
            else:
                conn.execute("INSERT INTO quality_approvers(venue_id,approver_id,granted_at,revoked_at) "
                             "VALUES(?,?,?,NULL)", (venue_id, approver_id, stamp))
            Store.audit(conn, "quality.approver_granted", approver_id,
                        {"venue_id": venue_id}, stamp)
            return {"venue_id": venue_id, "approver_id": approver_id, "active": True,
                    "granted_at": stamp}

    def revoke_approver(self, venue_id: str, approver_id: str) -> dict:
        require_id(approver_id, "approver_id")
        with self.store.transaction() as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            existing = Store.one(conn, "SELECT * FROM quality_approvers "
                                 "WHERE venue_id=? AND approver_id=?", (venue_id, approver_id))
            if not existing:
                raise NotFound("approver not found")
            if existing["revoked_at"] is not None:
                raise Conflict("approver already revoked")
            stamp = now()
            conn.execute("UPDATE quality_approvers SET revoked_at=? WHERE venue_id=? AND approver_id=?",
                         (stamp, venue_id, approver_id))
            affected = Store.many(conn, "SELECT a.approval_id FROM quality_approvals a JOIN snapshots s "
                                  "ON s.snapshot_id=a.snapshot_id WHERE s.venue_id=? AND a.approver_id=? "
                                  "AND a.revoked_at IS NULL AND s.status='staged'",
                                  (venue_id, approver_id))
            for row in affected:
                conn.execute("UPDATE quality_approvals SET revoked_at=? WHERE approval_id=?",
                             (stamp, row["approval_id"]))
                Store.audit(conn, "quality.approval_invalidated", row["approval_id"],
                            {"approver_id": approver_id, "reason": "approver_revoked"}, stamp)
            Store.audit(conn, "quality.approver_revoked", approver_id,
                        {"venue_id": venue_id,
                         "invalidated_approvals": sorted(x["approval_id"] for x in affected)}, stamp)
            return {"venue_id": venue_id, "approver_id": approver_id, "active": False,
                    "revoked_at": stamp,
                    "invalidated_approval_ids": sorted(x["approval_id"] for x in affected)}

    def approvers(self, venue_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            rows = Store.many(conn, "SELECT * FROM quality_approvers WHERE venue_id=? ORDER BY approver_id",
                              (venue_id,))
            return {"venue_id": venue_id,
                    "approvers": [{"approver_id": x["approver_id"],
                                   "active": x["revoked_at"] is None,
                                   "granted_at": x["granted_at"],
                                   "revoked_at": x["revoked_at"]} for x in rows]}

    # -------------------------------------------------------------- evaluation
    def evaluate(self, snapshot_id: str, *, persist: bool = False) -> dict:
        with self.store.transaction(write=persist) as conn:
            snapshot = self._snapshot(conn, snapshot_id)
            if snapshot["status"] == "published":
                return self._published_report(conn, snapshot)
            part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?",
                             (snapshot["venue_id"], snapshot["local_day"]))
            report = self._report(conn, snapshot, part)
            if persist:
                report["evaluation_id"] = self._store_evaluation(conn, report)
                Store.audit(conn, "quality.evaluated", snapshot_id,
                            {"verdict": report["verdict"],
                             "state_digest": report["state_digest"]}, now())
            report["approval"] = self._active_approval(conn, snapshot_id, report["state_digest"])
            report["stale_approvals"] = self._stale_approvals(conn, snapshot_id, report["state_digest"])
            report["blocking"] = report["verdict"] != "pass" and report["approval"] is None
            return report

    def _stale_approvals(self, conn, snapshot_id: str, state_digest: str) -> list[dict]:
        rows = Store.many(conn, "SELECT * FROM quality_approvals WHERE snapshot_id=? "
                          "AND revoked_at IS NULL ORDER BY created_at,approval_id", (snapshot_id,))
        snapshot = Store.one(conn, "SELECT venue_id FROM snapshots WHERE snapshot_id=?", (snapshot_id,))
        stale = []
        for row in rows:
            if row["state_digest"] == state_digest:
                allowed = Store.one(conn, "SELECT 1 AS ok FROM quality_approvers WHERE venue_id=? "
                                    "AND approver_id=? AND revoked_at IS NULL",
                                    (snapshot["venue_id"], row["approver_id"]))
                if allowed:
                    continue
                reason = "approver_unauthorized"
            else:
                reason = "state_changed"
            stale.append({"approval_id": row["approval_id"],
                          "approval_state_digest": row["state_digest"],
                          "approver_id": row["approver_id"], "reason": reason})
        return stale

    def evaluations(self, snapshot_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            self._snapshot(conn, snapshot_id)
            rows = Store.many(conn, "SELECT * FROM quality_evaluations WHERE snapshot_id=? "
                              "ORDER BY created_at,evaluation_id", (snapshot_id,))
            return {"snapshot_id": snapshot_id, "evaluations": [
                {"evaluation_id": x["evaluation_id"], "state_digest": x["state_digest"],
                 "verdict": x["verdict"], "policy_revision": x["policy_revision"],
                 "created_at": x["created_at"]} for x in rows]}

    def day(self, venue_id: str, day: str) -> dict:
        """发布前总览：当前已发布 head、暂存候选的覆盖/拒收与门禁结论。"""
        iso_day(day, "local_day")
        with self.store.transaction(write=False) as conn:
            policy = self._policy(conn, venue_id)
            part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?",
                             (venue_id, day))
            head_id = part["head_id"] if part else None
            generation = part["generation"] if part else 0
            staged = Store.many(conn, "SELECT snapshot_id FROM snapshots WHERE venue_id=? "
                                "AND local_day=? AND status='staged' ORDER BY created_at,snapshot_id",
                                (venue_id, day))
            candidates = []
            for item in staged:
                snapshot = self._snapshot(conn, item["snapshot_id"])
                report = self._report(conn, snapshot, part)
                approval = self._active_approval(conn, snapshot["snapshot_id"],
                                                 report["state_digest"])
                candidates.append({
                    "snapshot_id": snapshot["snapshot_id"], "status": "staged",
                    "generation": snapshot["generation"], "parent_id": snapshot["parent_id"],
                    "candidate_current": report["candidate_current"],
                    "verdict": report["verdict"], "state_digest": report["state_digest"],
                    "members_digest": report["members_digest"],
                    "source_digest": report["source_digest"],
                    "approval": approval, "blocking": report["verdict"] != "pass" and approval is None,
                    "stale_approvals": self._stale_approvals(conn, snapshot["snapshot_id"],
                                                             report["state_digest"]),
                    "findings": report["findings"], "coverage": report["coverage"],
                    "created_at": snapshot["created_at"]})
            head_quality = None
            if head_id:
                head_snapshot = self._snapshot(conn, head_id)
                head_quality = self._published_report(conn, head_snapshot)
            return {"venue_id": venue_id, "local_day": day, "head_id": head_id,
                    "generation": generation, "policy": policy,
                    "published": head_quality, "candidates": candidates}

    def _published_report(self, conn, snapshot: dict) -> dict:
        stored = Store.one(conn, "SELECT q.evaluation_id,q.approval_id,q.published_at,e.report_json "
                            "FROM snapshot_quality q JOIN quality_evaluations e "
                            "ON e.evaluation_id=q.evaluation_id WHERE q.snapshot_id=?",
                            (snapshot["snapshot_id"],))
        if not stored:
            return {"snapshot_id": snapshot["snapshot_id"], "venue_id": snapshot["venue_id"],
                    "local_day": snapshot["local_day"], "status": "published",
                    "as_published": True, "quality_recorded": False,
                    "verdict": "unrecorded", "blocking": False, "approval": None,
                    "policy": None, "findings": [],
                    "message": "this publication predates recorded quality decisions"}
        report = json.loads(stored["report_json"])
        report.update({"status": "published", "as_published": True, "quality_recorded": True,
                       "evaluation_id": stored["evaluation_id"],
                       "published_quality_at": stored["published_at"],
                       "approval": self._approval_by_id(conn, stored["approval_id"]),
                       "blocking": False})
        return report

    def _active_approval(self, conn, snapshot_id: str, state_digest: str) -> dict | None:
        row = Store.one(conn, "SELECT * FROM quality_approvals WHERE snapshot_id=? "
                        "AND state_digest=? AND revoked_at IS NULL "
                        "ORDER BY created_at DESC LIMIT 1", (snapshot_id, state_digest))
        if not row:
            return None
        snapshot = Store.one(conn, "SELECT venue_id FROM snapshots WHERE snapshot_id=?", (snapshot_id,))
        allowed = Store.one(conn, "SELECT 1 AS ok FROM quality_approvers WHERE venue_id=? "
                            "AND approver_id=? AND revoked_at IS NULL",
                            (snapshot["venue_id"], row["approver_id"]))
        if not allowed:
            return None
        return self._approval_view(row)

    def _approval_by_id(self, conn, approval_id: str | None) -> dict | None:
        if not approval_id:
            return None
        row = Store.one(conn, "SELECT * FROM quality_approvals WHERE approval_id=?", (approval_id,))
        return self._approval_view(row) if row else None

    def _approval_view(self, row: dict) -> dict:
        return {"approval_id": row["approval_id"], "snapshot_id": row["snapshot_id"],
                "evaluation_id": row["evaluation_id"], "state_digest": row["state_digest"],
                "approver_id": row["approver_id"], "reason": row["reason"],
                "created_at": row["created_at"], "revoked_at": row["revoked_at"]}

    def _snapshot(self, conn, snapshot_id: str) -> dict:
        snapshot = Store.one(conn, "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,))
        if not snapshot:
            raise NotFound("snapshot not found")
        return snapshot

    def _store_evaluation(self, conn, report: dict) -> str:
        existing = Store.one(conn, "SELECT evaluation_id FROM quality_evaluations "
                             "WHERE snapshot_id=? AND state_digest=?",
                             (report["snapshot_id"], report["state_digest"]))
        if existing:
            return existing["evaluation_id"]
        evaluation_id = uuid.uuid4().hex
        conn.execute("INSERT INTO quality_evaluations(evaluation_id,snapshot_id,venue_id,local_day,"
                     "state_digest,policy_revision,policy_digest,members_digest,source_digest,"
                     "verdict,report_json,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                     (evaluation_id, report["snapshot_id"], report["venue_id"],
                      report["local_day"], report["state_digest"], report["policy"]["revision"],
                      report["policy"]["digest"], report["members_digest"],
                      report["source_digest"], report["verdict"],
                      json.dumps(report, sort_keys=True), now()))
        return evaluation_id

    def _report(self, conn, snapshot: dict, part: dict | None) -> dict:
        venue = Store.one(conn, "SELECT * FROM venues WHERE venue_id=?", (snapshot["venue_id"],))
        policy = self._policy(conn, snapshot["venue_id"])
        day = snapshot["local_day"]
        head_id = part["head_id"] if part else None
        generation = part["generation"] if part else 0

        sources = Store.many(conn, "SELECT source_id,kind,active FROM sources WHERE venue_id=? "
                             "ORDER BY source_id", (snapshot["venue_id"],))
        contracts = Store.many(conn, "SELECT c.contract_id,c.source_id,c.revision,c.kind,c.fields_json,"
                              "c.effective_from,c.effective_to FROM contracts c JOIN sources s "
                              "ON s.source_id=c.source_id WHERE s.venue_id=? ORDER BY c.contract_id",
                              (snapshot["venue_id"],))
        events = Store.many(conn, "SELECT event_id,source_id,batch_id,exhibit_id,kind,payload_hash "
                            "FROM accepted_events WHERE venue_id=? AND local_day=? "
                            "ORDER BY source_id,event_id", (snapshot["venue_id"], day))
        members = Store.many(conn, "SELECT source_id,event_id,payload_hash FROM snapshot_members "
                             "WHERE snapshot_id=? ORDER BY source_id,event_id",
                             (snapshot["snapshot_id"],))
        event_index = {(x["source_id"], x["event_id"]): x for x in events}
        member_keys = {(x["source_id"], x["event_id"]) for x in members}
        pending = [x for x in events if (x["source_id"], x["event_id"]) not in member_keys]

        window_start, window_end = _utc_window(day, venue["timezone"])
        received = Store.many(conn, "SELECT b.* FROM batches b JOIN sources s ON s.source_id=b.source_id "
                              "WHERE s.venue_id=? AND b.received_at>=? AND b.received_at<? "
                              "ORDER BY b.batch_id",
                              (snapshot["venue_id"], window_start, window_end))
        contributing_ids = sorted({x["batch_id"] for x in events})
        contributing = []
        if contributing_ids:
            marks = ",".join("?" for _ in contributing_ids)
            contributing = Store.many(conn, "SELECT * FROM batches WHERE batch_id IN (" + marks + ") "
                                      "ORDER BY batch_id", tuple(contributing_ids))
        activity = {x["batch_id"]: x for x in [*received, *contributing]}
        rejections = []
        if activity:
            marks = ",".join("?" for _ in activity)
            rejections = Store.many(conn, "SELECT batch_id,row_no,code FROM rejected_rows "
                                    "WHERE batch_id IN (" + marks + ") ORDER BY batch_id,row_no",
                                    tuple(sorted(activity)))

        reject_by_code: dict[str, int] = {}
        reject_by_source: dict[str, int] = {}
        batch_source = {x["batch_id"]: x["source_id"] for x in activity.values()}
        for row in rejections:
            reject_by_code[row["code"]] = reject_by_code.get(row["code"], 0) + 1
            source_id = batch_source[row["batch_id"]]
            reject_by_source[source_id] = reject_by_source.get(source_id, 0) + 1
        raw_rows = sum(x["total_rows"] for x in activity.values())
        rejected_rows = len(rejections)

        snapshot_by_source: dict[str, int] = {}
        snapshot_by_kind: dict[str, int] = {}
        for item in members:
            snapshot_by_source[item["source_id"]] = snapshot_by_source.get(item["source_id"], 0) + 1
            record = event_index.get((item["source_id"], item["event_id"]))
            if record:
                snapshot_by_kind[record["kind"]] = snapshot_by_kind.get(record["kind"], 0) + 1
        accepted_by_source: dict[str, int] = {}
        pending_by_source: dict[str, int] = {}
        contributing_by_source: dict[str, int] = {}
        for item in events:
            accepted_by_source[item["source_id"]] = accepted_by_source.get(item["source_id"], 0) + 1
        for item in pending:
            pending_by_source[item["source_id"]] = pending_by_source.get(item["source_id"], 0) + 1
        for item in contributing:
            contributing_by_source[item["source_id"]] = contributing_by_source.get(item["source_id"], 0) + 1
        received_by_source: dict[str, int] = {}
        for item in received:
            received_by_source[item["source_id"]] = received_by_source.get(item["source_id"], 0) + 1

        required_sources = set(policy["rules"]["require_sources"])
        required_kinds = set(policy["rules"]["require_kinds"])
        coverage_sources = []
        for source in sources:
            sid = source["source_id"]
            snapshot_events = snapshot_by_source.get(sid, 0)
            accepted_events = accepted_by_source.get(sid, 0)
            if not source["active"]:
                status = "inactive"
            elif snapshot_events:
                status = "covered"
            elif accepted_events:
                status = "unpublished"
            elif received_by_source.get(sid, 0) or contributing_by_source.get(sid, 0):
                status = "rejected_only"
            else:
                status = "no_data"
            coverage_sources.append({
                "source_id": sid, "kind": source["kind"], "active": bool(source["active"]),
                "expected": sid in required_sources or source["kind"] in required_kinds,
                "status": status, "snapshot_events": snapshot_events,
                "accepted_events": accepted_events,
                "pending_events": pending_by_source.get(sid, 0),
                "accepted_batches": contributing_by_source.get(sid, 0),
                "received_batches": received_by_source.get(sid, 0),
                "rejected_rows": reject_by_source.get(sid, 0)})

        activity_batches = []
        for item in sorted(activity.values(), key=lambda x: (x["source_id"], x["batch_id"])):
            activity_batches.append({
                "batch_id": item["batch_id"], "source_id": item["source_id"],
                "request_key": item["request_key"], "contract_id": item["contract_id"],
                "received_at": item["received_at"], "total_rows": item["total_rows"],
                "valid_rows": item["valid_rows"], "rejected_rows": item["rejected_rows"],
                "rejections_by_code": {
                    code: sum(1 for x in rejections if x["batch_id"] == item["batch_id"]
                              and x["code"] == code)
                    for code in sorted({x["code"] for x in rejections
                                        if x["batch_id"] == item["batch_id"]})}})

        state = {
            "snapshot_id": snapshot["snapshot_id"], "snapshot_digest": snapshot["digest"],
            "parent_id": snapshot["parent_id"], "generation": snapshot["generation"],
            "members": members, "head_id": head_id, "partition_generation": generation,
            "policy_revision": policy["revision"], "policy_digest": policy["digest"],
            "sources": sources, "contracts": contracts, "events": events,
            "batches": activity_batches, "rejections": rejections,
        }
        state_digest = digest(state)
        members_digest = digest(members)
        source_digest = digest({name: state[name] for name in
                                ("sources", "contracts", "head_id", "partition_generation",
                                 "policy_revision", "policy_digest")})

        findings = []
        source_index = {x["source_id"]: x for x in coverage_sources}
        for item in members:
            record = event_index.get((item["source_id"], item["event_id"]))
            if record is None:
                findings.append({"rule": "snapshot", "code": "snapshot_member_missing",
                                 "severity": "error", "source_id": item["source_id"],
                                 "event_id": item["event_id"],
                                 "message": "candidate member no longer exists as an accepted event"})
            elif record["payload_hash"] != item["payload_hash"]:
                findings.append({"rule": "snapshot", "code": "snapshot_member_changed",
                                 "severity": "error", "source_id": item["source_id"],
                                 "event_id": item["event_id"],
                                 "message": "accepted event differs from the staged snapshot member"})
        for required in policy["rules"]["require_sources"]:
            source = source_index.get(required)
            if source is None:
                findings.append({"rule": "require_sources",
                                 "code": "required_source_not_registered", "severity": "error",
                                 "source_id": required,
                                 "message": "required source is not registered at this venue"})
            elif not source["active"]:
                findings.append({"rule": "require_sources",
                                 "code": "required_source_inactive", "severity": "error",
                                 "source_id": required,
                                 "message": "required source is inactive"})
            elif source["snapshot_events"] == 0:
                findings.append({"rule": "require_sources", "code": "required_source_uncovered",
                                 "severity": "error", "source_id": required,
                                 "message": "required source has no events in the candidate snapshot",
                                 "actual": source["snapshot_events"], "expected": ">=1",
                                 "accepted_events": source["accepted_events"],
                                 "pending_events": source["pending_events"],
                                 "received_batches": source["received_batches"]})
        for kind in policy["rules"]["require_kinds"]:
            if snapshot_by_kind.get(kind, 0) == 0:
                findings.append({"rule": "require_kinds", "code": "required_kind_uncovered",
                                 "severity": "error", "kind": kind,
                                 "message": "candidate snapshot contains no events of the required kind",
                                 "actual": 0, "expected": ">=1"})
        if policy["rules"]["require_active_source_coverage"]:
            for source in coverage_sources:
                if source["active"] and source["snapshot_events"] == 0:
                    findings.append({"rule": "require_active_source_coverage",
                                     "code": "active_source_uncovered", "severity": "error",
                                     "source_id": source["source_id"], "status": source["status"],
                                     "message": "active source has no events in the candidate snapshot",
                                     "actual": 0, "expected": ">=1"})
        if policy["rules"]["require_contract_coverage"]:
            for source in sources:
                if not source["active"]:
                    continue
                active_contracts = [x for x in contracts if x["source_id"] == source["source_id"]
                                    and x["effective_from"] <= window_start
                                    and (x["effective_to"] is None or x["effective_to"] > window_start)]
                if not active_contracts:
                    findings.append({"rule": "require_contract_coverage",
                                     "code": "source_contract_uncovered", "severity": "error",
                                     "source_id": source["source_id"],
                                     "message": "active source has no contract covering the local day"})
        total_events = len(members)
        if total_events < policy["rules"]["min_events"]:
            findings.append({"rule": "min_events", "code": "min_events_not_met", "severity": "error",
                             "message": "candidate snapshot has fewer events than the policy requires",
                             "actual": total_events, "expected": policy["rules"]["min_events"]})
        pending_events = len(pending)
        if (policy["rules"]["max_pending_events"] is not None
                and pending_events > policy["rules"]["max_pending_events"]):
            findings.append({"rule": "max_pending_events", "code": "pending_events_exceeded",
                             "severity": "error",
                             "message": "accepted events for the day are missing from the candidate snapshot",
                             "actual": pending_events,
                             "expected": policy["rules"]["max_pending_events"]})
        if (policy["rules"]["max_rejected_rows"] is not None
                and rejected_rows > policy["rules"]["max_rejected_rows"]):
            findings.append({"rule": "max_rejected_rows", "code": "rejected_rows_exceeded",
                             "severity": "error",
                             "message": "rejected rows for the day exceed the policy limit",
                             "actual": rejected_rows,
                             "expected": policy["rules"]["max_rejected_rows"]})
        ratio = round(rejected_rows / raw_rows, 6) if raw_rows else 0.0
        if (policy["rules"]["max_rejected_ratio"] is not None
                and ratio > policy["rules"]["max_rejected_ratio"]):
            findings.append({"rule": "max_rejected_ratio", "code": "rejected_ratio_exceeded",
                             "severity": "error",
                             "message": "rejected row ratio for the day exceeds the policy limit",
                             "actual": ratio, "expected": policy["rules"]["max_rejected_ratio"]})
        for code, limit in policy["rules"]["max_rejected_by_code"].items():
            actual = reject_by_code.get(code, 0)
            if actual > limit:
                findings.append({"rule": "max_rejected_by_code", "code": "rejection_code_exceeded",
                                 "severity": "error", "rejection_code": code,
                                 "message": "rejection code count for the day exceeds the policy limit",
                                 "actual": actual, "expected": limit})
        current = bool(snapshot["status"] == "staged" and snapshot["parent_id"] == head_id
                       and snapshot["generation"] == generation + 1)
        if snapshot["status"] == "staged" and not current:
            findings.append({"rule": "candidate", "code": "candidate_not_current",
                             "severity": "error",
                             "message": "candidate is not the publishable generation for the current head",
                             "actual": {"parent_id": snapshot["parent_id"],
                                        "generation": snapshot["generation"]},
                             "expected": {"parent_id": head_id, "generation": generation + 1}})
        elif snapshot["status"] not in ("staged", "published"):
            findings.append({"rule": "candidate", "code": "snapshot_not_publishable",
                             "severity": "error", "status": snapshot["status"],
                             "message": "snapshot has been abandoned and can no longer be published"})
        verdict = "fail" if any(x["severity"] == "error" for x in findings) else "pass"

        return {"snapshot_id": snapshot["snapshot_id"], "venue_id": snapshot["venue_id"],
                "local_day": day, "status": snapshot["status"], "as_published": False,
                "generation": snapshot["generation"], "parent_id": snapshot["parent_id"],
                "head_id": head_id, "candidate_current": current,
                "snapshot_digest": snapshot["digest"], "members_digest": members_digest,
                "source_digest": source_digest, "state_digest": state_digest,
                "policy": policy,
                "coverage": {"event_count": total_events,
                             "accepted_events": len(events),
                             "pending_events": pending_events,
                             "sources": coverage_sources,
                             "snapshot_by_kind": dict(sorted(snapshot_by_kind.items())),
                             "required_sources": policy["rules"]["require_sources"],
                             "required_kinds": policy["rules"]["require_kinds"],
                             "missing_required_sources": [
                                 x["source_id"] for x in coverage_sources
                                 if x["expected"] and x["snapshot_events"] == 0],
                             "uncovered_active_sources": [
                                 x["source_id"] for x in coverage_sources
                                 if x["active"] and x["snapshot_events"] == 0],
                             "raw_rows": raw_rows, "rejected_rows": rejected_rows,
                             "rejected_ratio": ratio,
                             "rejections_by_code": dict(sorted(reject_by_code.items())),
                             "batches": activity_batches},
                "findings": findings, "verdict": verdict,
                "approval": None, "blocking": verdict != "pass",
                "evaluated_at": now()}

    # --------------------------------------------------------------- approvals
    def approve(self, snapshot_id: str, approver_id: str, reason: str) -> dict:
        require_id(approver_id, "approver_id")
        if not isinstance(reason, str) or not 1 <= len(reason.strip()) <= 500:
            raise InvalidInput("approval reason is required and must be at most 500 characters")
        reason = reason.strip()
        with self.store.transaction() as conn:
            snapshot = self._snapshot(conn, snapshot_id)
            if snapshot["status"] != "staged":
                raise StateError("only staged snapshots can be approved for publication")
            part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?",
                             (snapshot["venue_id"], snapshot["local_day"]))
            if (not part or snapshot["parent_id"] != part["head_id"]
                    or snapshot["generation"] != part["generation"] + 1):
                raise Conflict("candidate is not the publishable generation for the current head")
            allowed = Store.one(conn, "SELECT 1 AS ok FROM quality_approvers WHERE venue_id=? "
                                "AND approver_id=? AND revoked_at IS NULL",
                                (snapshot["venue_id"], approver_id))
            if not allowed:
                raise NotAuthorized("approver is not authorized for this venue")
            report = self._report(conn, snapshot, part)
            if report["verdict"] == "pass":
                raise Conflict("quality policy already passes; an exception approval is not applicable")
            evaluation_id = self._store_evaluation(conn, report)
            existing = Store.one(conn, "SELECT approval_id FROM quality_approvals "
                                 "WHERE snapshot_id=? AND state_digest=? AND revoked_at IS NULL",
                                 (snapshot_id, report["state_digest"]))
            if existing:
                raise Conflict("current state already has an active approval",
                               details={"approval_id": existing["approval_id"]})
            approval_id = uuid.uuid4().hex
            stamp = now()
            conn.execute("INSERT INTO quality_approvals(approval_id,snapshot_id,evaluation_id,"
                         "state_digest,approver_id,reason,created_at,revoked_at) "
                         "VALUES(?,?,?,?,?,?,?,NULL)",
                         (approval_id, snapshot_id, evaluation_id, report["state_digest"],
                          approver_id, reason, stamp))
            Store.audit(conn, "quality.approved", snapshot_id,
                        {"approval_id": approval_id, "evaluation_id": evaluation_id,
                         "approver_id": approver_id, "state_digest": report["state_digest"],
                         "reason": reason}, stamp)
            return {"snapshot_id": snapshot_id, "evaluation_id": evaluation_id,
                    "verdict": report["verdict"], "state_digest": report["state_digest"],
                    "findings": report["findings"],
                    "approval": {"approval_id": approval_id, "snapshot_id": snapshot_id,
                                 "evaluation_id": evaluation_id,
                                 "state_digest": report["state_digest"],
                                 "approver_id": approver_id, "reason": reason,
                                 "created_at": stamp, "revoked_at": None}}

    def approvals(self, snapshot_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            self._snapshot(conn, snapshot_id)
            rows = Store.many(conn, "SELECT * FROM quality_approvals WHERE snapshot_id=? "
                              "ORDER BY created_at,approval_id", (snapshot_id,))
            return {"snapshot_id": snapshot_id,
                    "approvals": [self._approval_view(x) for x in rows]}

    def revoke_approval(self, snapshot_id: str, approval_id: str, actor_id: str) -> dict:
        require_id(actor_id, "actor_id")
        with self.store.transaction() as conn:
            snapshot = self._snapshot(conn, snapshot_id)
            row = Store.one(conn, "SELECT * FROM quality_approvals "
                            "WHERE approval_id=? AND snapshot_id=?", (approval_id, snapshot_id))
            if not row:
                raise NotFound("approval not found")
            if row["revoked_at"] is not None:
                raise Conflict("approval already revoked")
            allowed = Store.one(conn, "SELECT 1 AS ok FROM quality_approvers WHERE venue_id=? "
                                "AND approver_id=? AND revoked_at IS NULL",
                                (snapshot["venue_id"], actor_id))
            if not allowed:
                raise NotAuthorized("actor is not authorized for this venue")
            stamp = now()
            conn.execute("UPDATE quality_approvals SET revoked_at=? WHERE approval_id=?",
                         (stamp, approval_id))
            Store.audit(conn, "quality.approval_revoked", snapshot_id,
                        {"approval_id": approval_id, "actor_id": actor_id}, stamp)
            return {"snapshot_id": snapshot_id, "approval_id": approval_id, "revoked_at": stamp}

    # ---------------------------------------------------------------- publish
    def authorize_publication(self, conn, snapshot: dict, part: dict) -> dict:
        """在发布事务内检查质量门禁；失败时抛出带完整诊断的冲突错误。"""
        report = self._report(conn, snapshot, part)
        approval = None
        if report["verdict"] != "pass":
            approval = self._active_approval(conn, snapshot["snapshot_id"], report["state_digest"])
            if approval is None:
                stale = Store.one(conn, "SELECT COUNT(*) AS n FROM quality_approvals "
                                  "WHERE snapshot_id=? AND revoked_at IS NULL",
                                  (snapshot["snapshot_id"],))["n"] > 0
                previous = Store.one(conn, "SELECT evaluation_id FROM quality_evaluations "
                                     "WHERE snapshot_id=? AND state_digest=?",
                                     (snapshot["snapshot_id"], report["state_digest"]))
                raise QualityGateFailed("quality policy blocks publication", details={
                    "snapshot_id": snapshot["snapshot_id"],
                    "evaluation_id": previous["evaluation_id"] if previous else None,
                    "state_digest": report["state_digest"],
                    "verdict": report["verdict"],
                    "findings": report["findings"],
                    "approval_required": True,
                    "stale_approval": stale,
                    "stale_approvals": self._stale_approvals(conn, snapshot["snapshot_id"],
                                                             report["state_digest"]),
                    "candidate_current": report["candidate_current"],
                    "policy": report["policy"],
                    "coverage": report["coverage"]})
        evaluation_id = self._store_evaluation(conn, report)
        return {"evaluation_id": evaluation_id,
                "approval_id": approval["approval_id"] if approval else None,
                "verdict": report["verdict"], "state_digest": report["state_digest"],
                "policy_revision": report["policy"]["revision"],
                "blocking": False, "approval": approval}

    def record_publication(self, conn, snapshot_id: str, decision: dict, stamp: str) -> None:
        conn.execute("INSERT INTO snapshot_quality(snapshot_id,evaluation_id,approval_id,published_at) "
                     "VALUES(?,?,?,?)", (snapshot_id, decision["evaluation_id"],
                                         decision["approval_id"], stamp))
