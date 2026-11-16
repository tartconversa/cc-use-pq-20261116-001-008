import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from exhibit_lake.api import create_app
from exhibit_lake.errors import (Conflict, InvalidInput, NotAuthorized,
                                 QualityGateFailed, StateError)
from exhibit_lake.service import ExhibitLake


class QualityGateTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.lake = ExhibitLake(Path(self.folder.name) / "quality.db")
        self.lake.register_venue("venue", "Venue", "Asia/Shanghai")
        self.lake.register_exhibit("robot", "venue", "Robot", "Maker", "2026-01-01")
        self.lake.register_source("counter", "venue", "visit")
        self.lake.register_source("kiosk", "venue", "feedback")
        self.lake.register_contract("count-v1", "counter",
                                    ["event_id", "exhibit_id", "event_at", "count"],
                                    "2026-01-01T00:00:00+00:00")
        self.lake.register_contract("rating-v1", "kiosk",
                                    ["event_id", "exhibit_id", "event_at", "rating", "visitor_id"],
                                    "2026-01-01T00:00:00+00:00")
        self.gate = self.lake.quality_gate

    def visit(self, event_id="v1", count=6):
        return {"event_id": event_id, "exhibit_id": "robot",
                "event_at": "2026-03-01T10:00:00+08:00", "count": count}

    def feedback(self, event_id="f1", rating=4):
        return {"event_id": event_id, "exhibit_id": "robot",
                "event_at": "2026-03-01T11:00:00+08:00", "rating": rating,
                "visitor_id": "visitor-1"}

    def stage(self, day="2026-03-01", head=None):
        return self.lake.snapshots.stage("venue", day, head)["snapshot_id"]

    def publish(self, snapshot_id, head=None):
        return self.lake.snapshots.publish(snapshot_id, head)

    def test_required_source_rejected_only_is_reported(self):
        bad = self.feedback()
        bad["rating"] = 9
        rejected = self.lake.ingest("kiosk", "rating-v1", "bad", [bad])
        self.lake.ingest("counter", "count-v1", "b1", [self.visit()])
        # Rejection evidence is attributed to the local receive day, as in the
        # existing coverage reports; simulate a batch received on 2026-03-01.
        with self.lake.store.transaction() as conn:
            conn.execute("UPDATE batches SET received_at=? WHERE batch_id=?",
                         ("2026-03-01T09:00:00+08:00", rejected["batch_id"]))
        self.gate.set_policy("venue", {"require_sources": ["kiosk"],
                                       "max_rejected_by_code": {"invalid_rating": 0}})
        report = self.gate.evaluate(self.stage())
        statuses = {x["source_id"]: x["status"] for x in report["coverage"]["sources"]}
        self.assertEqual(statuses["kiosk"], "rejected_only")
        codes = [x["code"] for x in report["findings"]]
        self.assertIn("required_source_uncovered", codes)
        self.assertIn("rejection_code_exceeded", codes)
        self.assertEqual(report["coverage"]["rejections_by_code"], {"invalid_rating": 1})

    def test_contract_coverage_rule(self):
        self.lake.ingest("counter", "count-v1", "b1", [self.visit()])
        candidate = self.stage()
        self.gate.set_policy("venue", {"require_contract_coverage": True})
        # The feedback contract starts 2026-01-01, so every active source is covered.
        self.assertEqual(self.gate.evaluate(candidate)["verdict"], "pass")
        with self.lake.store.transaction() as conn:
            conn.execute("UPDATE contracts SET effective_from=? WHERE contract_id='rating-v1'",
                         ("2026-04-01T00:00:00+00:00",))
        report = self.gate.evaluate(candidate)
        self.assertEqual([x["code"] for x in report["findings"]], ["source_contract_uncovered"])

    def test_input_feed_outage_blocks_publication(self):
        self.lake.ingest("counter", "count-v1", "b1", [self.visit()])
        self.gate.set_policy("venue", {"require_sources": ["kiosk"]})
        candidate = self.stage()
        report = self.gate.evaluate(candidate)
        self.assertEqual(report["verdict"], "fail")
        self.assertTrue(report["blocking"])
        self.assertEqual([x["code"] for x in report["findings"]], ["required_source_uncovered"])
        self.assertIn("kiosk", report["coverage"]["missing_required_sources"])
        with self.assertRaises(QualityGateFailed) as caught:
            self.publish(candidate)
        details = caught.exception.details
        self.assertTrue(details["approval_required"])
        self.assertFalse(details["stale_approval"])
        self.assertEqual(details["findings"][0]["source_id"], "kiosk")
        self.assertEqual(self.lake.snapshots.head("venue", "2026-03-01")["head_id"], None)

    def test_default_policy_keeps_legacy_behavior(self):
        self.lake.ingest("counter", "count-v1", "b1", [self.visit()])
        candidate = self.stage()
        self.assertEqual(self.gate.evaluate(candidate)["verdict"], "pass")
        self.publish(candidate)
        self.assertIsNotNone(self.lake.snapshots.head("venue", "2026-03-01")["head_id"])

    def test_required_kind_and_active_source_coverage(self):
        self.lake.ingest("counter", "count-v1", "b1", [self.visit()])
        candidate = self.stage()
        self.gate.set_policy("venue", {"require_kinds": ["feedback"]})
        report = self.gate.evaluate(candidate)
        self.assertEqual([x["code"] for x in report["findings"]], ["required_kind_uncovered"])
        self.gate.set_policy("venue", {"require_active_source_coverage": True})
        report = self.gate.evaluate(candidate)
        self.assertEqual([x["code"] for x in report["findings"]], ["active_source_uncovered"])
        self.lake.registry.deactivate_source("kiosk")
        self.assertEqual(self.gate.evaluate(candidate)["verdict"], "pass")

    def test_pending_events_and_rejections_are_gated(self):
        self.lake.ingest("counter", "count-v1", "b1", [self.visit(), self.visit("bad", 0)])
        candidate = self.stage()
        self.lake.ingest("kiosk", "rating-v1", "b2", [self.feedback()])
        self.gate.set_policy("venue", {"max_pending_events": 0, "max_rejected_rows": 0,
                                       "max_rejected_ratio": 0.0,
                                       "max_rejected_by_code": {"invalid_count": 0}})
        report = self.gate.evaluate(candidate)
        codes = sorted(x["code"] for x in report["findings"])
        self.assertEqual(codes, ["pending_events_exceeded", "rejected_ratio_exceeded",
                                 "rejected_rows_exceeded", "rejection_code_exceeded"])
        self.assertEqual(report["coverage"]["pending_events"], 1)
        self.assertEqual(report["coverage"]["rejections_by_code"], {"invalid_count": 1})

    def test_approval_requires_authorized_staff_and_reason(self):
        self.lake.ingest("counter", "count-v1", "b1", [self.visit()])
        self.gate.set_policy("venue", {"require_sources": ["kiosk"]})
        candidate = self.stage()
        with self.assertRaises(NotAuthorized):
            self.gate.approve(candidate, "stranger", "kiosk down")
        self.gate.grant_approver("venue", "ops-lead")
        with self.assertRaises(InvalidInput):
            self.gate.approve(candidate, "ops-lead", "   ")
        result = self.gate.approve(candidate, "ops-lead", "kiosk terminal offline")
        self.assertEqual(result["verdict"], "fail")
        self.assertEqual(result["approval"]["reason"], "kiosk terminal offline")
        published = self.publish(candidate)
        self.assertEqual(published["quality"]["approval_id"], result["approval"]["approval_id"])
        trail = self.lake.audit.trail(action="quality.approved")["audits"]
        self.assertEqual(trail[0]["detail"]["approver_id"], "ops-lead")
        self.assertEqual(trail[0]["detail"]["reason"], "kiosk terminal offline")

    def test_approval_not_applicable_when_policy_passes(self):
        self.lake.ingest("counter", "count-v1", "b1", [self.visit()])
        self.gate.grant_approver("venue", "ops-lead")
        with self.assertRaises(Conflict):
            self.gate.approve(self.stage(), "ops-lead", "unnecessary")

    def test_new_batch_after_approval_invalidates_it(self):
        self.lake.ingest("counter", "count-v1", "b1", [self.visit()])
        self.gate.set_policy("venue", {"require_sources": ["kiosk"]})
        self.gate.grant_approver("venue", "ops-lead")
        candidate = self.stage()
        approval = self.gate.approve(candidate, "ops-lead", "kiosk offline")
        before = self.gate.evaluate(candidate)
        self.assertEqual(before["approval"]["approval_id"], approval["approval"]["approval_id"])
        self.lake.ingest("kiosk", "rating-v1", "late", [self.feedback()])
        after = self.gate.evaluate(candidate)
        self.assertIsNone(after["approval"])
        self.assertTrue(after["blocking"])
        self.assertNotEqual(after["state_digest"], before["state_digest"])
        self.assertEqual([x["reason"] for x in after["stale_approvals"]], ["state_changed"])
        with self.assertRaises(QualityGateFailed) as caught:
            self.publish(candidate)
        self.assertTrue(caught.exception.details["stale_approval"])
        self.assertNotEqual(caught.exception.details["state_digest"], before["state_digest"])

    def test_head_change_invalidates_staged_approval(self):
        self.lake.ingest("counter", "count-v1", "b1", [self.visit()])
        self.gate.set_policy("venue", {"require_sources": ["kiosk"]})
        self.gate.grant_approver("venue", "ops-lead")
        first = self.stage()
        self.gate.approve(first, "ops-lead", "kiosk offline")
        # An approved candidate that misses a new batch cannot be published as-is.
        self.lake.ingest("counter", "count-v1", "b2", [self.visit("v2", 9)])
        self.lake.snapshots.abandon(first)
        second = self.stage()
        self.assertEqual(self.gate.approvals(second)["approvals"], [])
        self.assertIsNone(self.gate.evaluate(second)["approval"])
        with self.assertRaises(QualityGateFailed) as caught:
            self.publish(second)
        self.assertFalse(caught.exception.details["stale_approval"])
        self.gate.approve(second, "ops-lead", "kiosk still offline")
        self.publish(second)
        self.assertEqual(self.lake.analytics.day("venue", "2026-03-01")["total_visitors"], 15)

    def test_approval_does_not_carry_to_next_generation(self):
        self.lake.ingest("counter", "count-v1", "b1", [self.visit()])
        self.gate.set_policy("venue", {"require_sources": ["kiosk"]})
        self.gate.grant_approver("venue", "ops-lead")
        first = self.stage()
        approval = self.gate.approve(first, "ops-lead", "kiosk offline")
        self.publish(first)
        next_snapshot = self.stage(head=first)
        self.assertEqual(self.gate.approvals(next_snapshot)["approvals"], [])
        report = self.gate.evaluate(next_snapshot)
        self.assertIsNone(report["approval"])
        self.assertEqual(report["state_digest"] != approval["state_digest"], True)
        # The published generation keeps its recorded decision and visibility.
        published = self.gate.evaluate(first)
        self.assertEqual(published["approval"]["approval_id"], approval["approval"]["approval_id"])
        self.assertEqual(published["status"], "published")
        self.assertEqual(self.lake.analytics.day("venue", "2026-03-01")["snapshot_id"], first)

    def test_policy_change_invalidates_approval(self):
        self.lake.ingest("counter", "count-v1", "b1", [self.visit()])
        self.gate.set_policy("venue", {"min_events": 5})
        self.gate.grant_approver("venue", "ops-lead")
        candidate = self.stage()
        self.gate.approve(candidate, "ops-lead", "quiet day")
        self.gate.set_policy("venue", {"min_events": 1})
        report = self.gate.evaluate(candidate)
        self.assertEqual(report["verdict"], "pass")
        self.assertIsNone(report["approval"])
        self.publish(candidate)

    def test_revoked_approver_cannot_authorize(self):
        self.lake.ingest("counter", "count-v1", "b1", [self.visit()])
        self.gate.set_policy("venue", {"require_sources": ["kiosk"]})
        self.gate.grant_approver("venue", "ops-lead")
        candidate = self.stage()
        self.gate.approve(candidate, "ops-lead", "kiosk offline")
        revoked = self.gate.revoke_approver("venue", "ops-lead")
        self.assertEqual(len(revoked["invalidated_approval_ids"]), 1)
        self.assertIsNone(self.gate.evaluate(candidate)["approval"])
        with self.assertRaises(NotAuthorized):
            self.gate.approve(candidate, "ops-lead", "again")
        with self.assertRaises(QualityGateFailed):
            self.publish(candidate)

    def test_policy_revision_conflict(self):
        policy = self.gate.set_policy("venue", {"min_events": 1})
        self.assertEqual(policy["revision"], 1)
        with self.assertRaises(Conflict):
            self.gate.set_policy("venue", {"min_events": 2}, expected_revision=0)
        self.assertEqual(self.gate.set_policy("venue", {"min_events": 2}, expected_revision=1)["revision"], 2)

    def test_invalid_policy_rules(self):
        for rules in ({"min_events": -1}, {"max_rejected_ratio": 2}, {"unknown": 1},
                      {"require_sources": ["bad id"]}, {"require_kinds": ["unknown"]},
                      {"max_rejected_by_code": {"x": -1}}):
            with self.assertRaises(InvalidInput):
                self.gate.set_policy("venue", rules)

    def test_quality_survives_database_reopen(self):
        self.lake.ingest("counter", "count-v1", "b1", [self.visit()])
        self.gate.set_policy("venue", {"require_sources": ["kiosk"]})
        self.gate.grant_approver("venue", "ops-lead")
        candidate = self.stage()
        approval = self.gate.approve(candidate, "ops-lead", "kiosk offline")
        reopened = ExhibitLake(Path(self.folder.name) / "quality.db")
        self.assertEqual(reopened.quality_gate.policy("venue")["rules"]["require_sources"], ["kiosk"])
        self.assertEqual(reopened.quality_gate.evaluate(candidate)["approval"]["approval_id"],
                         approval["approval"]["approval_id"])
        reopened.snapshots.publish(candidate, None)

    def test_report_exposes_feedback_coverage(self):
        self.lake.ingest("counter", "count-v1", "b1", [self.visit()])
        self.gate.set_policy("venue", {"require_sources": ["kiosk"]})
        self.gate.grant_approver("venue", "ops-lead")
        candidate = self.stage()
        self.gate.approve(candidate, "ops-lead", "kiosk offline")
        self.publish(candidate)
        report = self.lake.analytics.day("venue", "2026-03-01")
        self.assertEqual(report["total_feedback"], 0)
        self.assertFalse(report["quality"]["feedback_covered"])
        self.assertTrue(report["quality"]["approved_at_publication"])
        self.assertEqual(report["quality"]["approval"]["reason"], "kiosk offline")
        self.assertEqual(report["quality"]["feedback_sources"][0]["status"], "no_data")

    def test_published_quality_is_not_recomputed(self):
        self.lake.ingest("counter", "count-v1", "b1", [self.visit()])
        candidate = self.stage()
        self.publish(candidate)
        self.lake.ingest("kiosk", "rating-v1", "late", [self.feedback()])
        as_published = self.gate.evaluate(candidate)
        self.assertEqual(as_published["status"], "published")
        self.assertTrue(as_published["as_published"])
        self.assertEqual(as_published["coverage"]["event_count"], 1)
        statuses = {x["source_id"]: x["status"] for x in as_published["coverage"]["sources"]}
        self.assertEqual(statuses, {"counter": "covered", "kiosk": "no_data"})
        self.assertEqual(self.lake.analytics.day("venue", "2026-03-01")["total_feedback"], 0)


class QualityGateApiTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.client = TestClient(create_app(str(Path(self.folder.name) / "api.db")))
        self.client.post("/venues", json={"venue_id": "venue", "name": "Venue", "timezone": "UTC"})
        self.client.post("/exhibits", json={"exhibit_id": "robot", "venue_id": "venue",
                                            "name": "Robot", "maker": "Maker",
                                            "launched_on": "2026-01-01"})
        self.client.post("/sources", json={"source_id": "counter", "venue_id": "venue", "kind": "visit"})
        self.client.post("/sources", json={"source_id": "kiosk", "venue_id": "venue", "kind": "feedback"})
        self.client.post("/contracts", json={"contract_id": "count-v1", "source_id": "counter",
                                             "fields": ["event_id", "exhibit_id", "event_at", "count"],
                                             "effective_from": "2026-01-01T00:00:00+00:00"})
        self.client.post("/contracts", json={"contract_id": "rating-v1", "source_id": "kiosk",
                                             "fields": ["event_id", "exhibit_id", "event_at", "rating", "visitor_id"],
                                             "effective_from": "2026-01-01T00:00:00+00:00"})
        self.client.post("/batches", json={"source_id": "counter", "contract_id": "count-v1",
                                           "request_key": "b1",
                                           "events": [{"event_id": "v1", "exhibit_id": "robot",
                                                       "event_at": "2026-03-01T10:00:00+08:00", "count": 5}]})

    def stage(self):
        result = self.client.post("/snapshots/stage", json={"venue_id": "venue",
                                                            "local_day": "2026-03-01"})
        self.assertEqual(result.status_code, 201, result.text)
        return result.json()["snapshot_id"]

    def test_gate_error_envelope_and_approval_flow(self):
        self.assertEqual(self.client.put("/venues/venue/quality-policy", json={
            "rules": {"require_sources": ["kiosk"]}}).status_code, 200)
        self.assertEqual(self.client.post("/venues/venue/quality-approvers",
                                          json={"approver_id": "ops"}).status_code, 201)
        candidate = self.stage()
        gate = self.client.get(f"/snapshots/{candidate}/quality").json()
        self.assertEqual(gate["verdict"], "fail")
        self.assertEqual(gate["findings"][0]["code"], "required_source_uncovered")
        day = self.client.get("/venues/venue/days/2026-03-01/quality-gate").json()
        self.assertEqual(day["candidates"][0]["blocking"], True)
        self.assertEqual(day["published"], None)
        blocked = self.client.post(f"/snapshots/{candidate}/publish", json={"expected_head": None})
        self.assertEqual(blocked.status_code, 409)
        self.assertEqual(blocked.json()["error"]["code"], "quality_gate_failed")
        self.assertFalse(blocked.json()["error"]["details"]["stale_approval"])
        unauthorized = self.client.post(f"/snapshots/{candidate}/quality/approval",
                                        json={"approver_id": "stranger", "reason": "kiosk offline"})
        self.assertEqual(unauthorized.status_code, 403)
        approved = self.client.post(f"/snapshots/{candidate}/quality/approval",
                                    json={"approver_id": "ops", "reason": "kiosk offline"})
        self.assertEqual(approved.status_code, 201)
        published = self.client.post(f"/snapshots/{candidate}/publish", json={"expected_head": None})
        self.assertEqual(published.status_code, 200, published.text)
        self.assertEqual(published.json()["quality"]["approval_id"],
                         approved.json()["approval"]["approval_id"])
        report = self.client.get("/venues/venue/days/2026-03-01/report").json()
        self.assertEqual(report["quality"]["approval"]["approver_id"], "ops")
        self.assertTrue(self.client.get("/venues/venue/days/2026-03-01/quality-gate").json()
                        ["published"]["quality_recorded"])

    def test_publish_accepts_inline_exception(self):
        self.client.put("/venues/venue/quality-policy", json={
            "rules": {"require_sources": ["kiosk"]}})
        self.client.post("/venues/venue/quality-approvers", json={"approver_id": "ops"})
        candidate = self.stage()
        missing_reason = self.client.post(f"/snapshots/{candidate}/publish",
                                          json={"expected_head": None, "approver_id": "ops"})
        self.assertEqual(missing_reason.status_code, 400)
        result = self.client.post(f"/snapshots/{candidate}/publish",
                                  json={"expected_head": None, "approver_id": "ops",
                                        "reason": "kiosk terminal offline"})
        self.assertEqual(result.status_code, 200, result.text)
        self.assertEqual(result.json()["quality"]["verdict"], "fail")

    def test_staged_candidate_remains_invisible(self):
        self.client.put("/venues/venue/quality-policy", json={"rules": {"min_events": 100}})
        candidate = self.stage()
        self.assertEqual(self.client.get("/venues/venue/days/2026-03-01/report").status_code, 404)
        self.assertEqual(self.client.get("/venues/venue/days/2026-03-01/snapshots").json(), [])
        self.assertEqual(self.client.get(f"/snapshots/{candidate}").json()["status"], "staged")


if __name__ == "__main__":
    unittest.main()
