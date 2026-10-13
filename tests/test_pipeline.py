"""End-to-end behaviour of the pipeline against a real (temp) SQLite database."""
import unittest
from unittest import mock

from schema_drift.contract import Contract
from schema_drift.util import ROOT

from .helpers import BASE, ExplodingSink, PipelineTestCase, good_row

RENAMED = ["order_id", "customer_id", "order_date", "total_amount", "status", "channel"]


class LoadAndIdempotencyTests(PipelineTestCase):
    def test_clean_file_loads_and_rerun_is_a_noop(self):
        path = self.write_csv("a.csv", [good_row(i) for i in range(1, 6)])
        first = self.pipe.ingest(path)
        self.assertEqual((first.status, first.inserted, first.quarantined), ("LOADED", 5, 0))
        self.assertEqual(self.sink.alerts, [])                       # clean data raises no alert

        again = self.pipe.ingest(path)                               # same bytes, same name
        copy = self.pipe.ingest(self.write_csv("renamed_copy.csv", [good_row(i) for i in range(1, 6)]))
        self.assertTrue(again.skipped and copy.skipped)              # same content under another name too
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM curated_orders"), 5)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM file_registry"), 1)

    def test_upsert_updates_changed_rows_and_ignores_identical_ones(self):
        self.pipe.ingest(self.write_csv("d1.csv", [good_row(1), good_row(2)]))
        res = self.pipe.ingest(self.write_csv("d2.csv", [good_row(1), good_row(2, amount="999.00")]))
        self.assertEqual((res.inserted, res.updated, res.unchanged), (0, 1, 1))
        self.assertEqual(self.scalar("SELECT amount FROM curated_orders WHERE order_id = 2"), 999.0)

    def test_exact_duplicate_dropped_but_conflicting_duplicate_quarantined(self):
        rows = [good_row(1), good_row(1), good_row(1, amount="5.00"), good_row(2)]
        res = self.pipe.ingest(self.write_csv("dup.csv", rows))
        self.assertEqual((res.inserted, res.duplicates_dropped, res.quarantined), (2, 1, 1))
        self.assertEqual(self.q("SELECT reason_code FROM quarantine_records"), [("DUPLICATE_KEY_CONFLICT",)])


class RowLevelQuarantineTests(PipelineTestCase):
    def test_bad_rows_are_quarantined_good_rows_still_load(self):
        rows = ([good_row(2, status="WEIRD"), good_row(3, amount="-4"), good_row(4, customer_id="")]
                + [good_row(i) for i in range(10, 70)])                                      # 3/63 bad = 4.8% < 5%
        res = self.pipe.ingest(self.write_csv("bad.csv", rows))
        self.assertEqual((res.status, res.inserted, res.quarantined), ("LOADED", 60, 3))   # noise stays below the alert threshold
        codes = {r[0] for r in self.q("SELECT reason_code FROM quarantine_records")}
        self.assertEqual(codes, {"INVALID_ENUM", "BELOW_MIN", "NULL_REQUIRED"})
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM raw_records"), 63)            # nothing lost from RAW

    def test_row_with_wrong_field_count_is_quarantined_as_malformed(self):
        text = ",".join(BASE) + "\n1,C0001,2026-10-01,10.00,PLACED,WEB\n2,C0002,2026-10-01,10.00\n"
        res = self.pipe.ingest(self.write_bytes("short.csv", text.encode()))
        self.assertEqual((res.inserted, res.quarantined), (1, 1))
        self.assertEqual(self.q("SELECT reason_code FROM quarantine_records"), [("MALFORMED_ROW",)])


class SchemaDriftTests(PipelineTestCase):
    def test_additive_drift_loads_with_warning_and_keeps_new_column(self):
        header = [c for c in BASE if c != "channel"] + ["coupon_code"]
        rows = [{**{k: v for k, v in good_row(i).items() if k != "channel"}, "coupon_code": "SAVE10"}
                for i in range(1, 4)]
        res = self.pipe.ingest(self.write_csv("add.csv", rows, header))
        self.assertEqual((res.status, res.inserted), ("LOADED_WITH_WARNINGS", 3))
        self.assertIn("SAVE10", self.scalar("SELECT extra_json FROM curated_orders WHERE order_id = 1"))
        self.assertEqual(len(self.sink.alerts), 1)
        self.assertEqual(self.sink.alerts[0].severity, "WARNING")

    def test_breaking_rename_quarantines_whole_file_and_alerts_critical(self):
        rows = [{("total_amount" if k == "amount" else k): v for k, v in good_row(i).items()} for i in range(1, 5)]
        res = self.pipe.ingest(self.write_csv("ren.csv", rows, RENAMED))
        self.assertEqual((res.status, res.inserted, res.quarantined), ("QUARANTINED_FILE", 0, 4))
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM curated_orders"), 0)
        self.assertEqual(self.q("SELECT DISTINCT reason_code FROM quarantine_records"), [("FILE_QUARANTINED",)])
        alert = self.sink.alerts[0]
        self.assertEqual(alert.severity, "CRITICAL")
        self.assertEqual(alert.explanation["status"], "FALLBACK")                 # no model configured -> still explained
        self.assertEqual(alert.explanation["suggested_mapping"], {"total_amount": "amount"})

    def test_approved_alias_then_replay_loads_quarantined_file_once(self):
        rows = [{("total_amount" if k == "amount" else k): v for k, v in good_row(i).items()} for i in range(1, 5)]
        self.pipe.ingest(self.write_csv("ren.csv", rows, RENAMED))

        untouched = self.pipe.replay()                                            # contract unchanged: must not retry
        self.assertFalse(untouched[0].replayed)

        self.pipe.set_contract(self.pipe.contract.with_alias("total_amount", "amount"))
        outcome = self.pipe.replay()[0]
        self.assertTrue(outcome.replayed)
        self.assertEqual(outcome.result.inserted, 4)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM curated_orders"), 4)
        self.assertEqual(self.q("SELECT status FROM file_registry ORDER BY file_id"), [("SUPERSEDED",), ("LOADED",)])
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM quarantine_records WHERE status = 'OPEN'"), 0)
        self.assertEqual(self.pipe.replay() and [o.replayed for o in self.pipe.replay()], [])  # nothing left to replay

    def test_type_drift_in_most_rows_quarantines_the_file(self):
        rows = [good_row(i, amount=f"Rs {i},299.00") for i in range(1, 6)]
        res = self.pipe.ingest(self.write_csv("rs.csv", rows))
        self.assertEqual((res.status, res.inserted), ("QUARANTINED_FILE", 0))
        self.assertIn("TYPE_DRIFT", {e.drift_type for e in res.events})

    def test_partial_type_drift_quarantines_only_bad_rows(self):
        rows = [good_row(i, order_date="05/10/2026" if i <= 2 else "2026-10-05") for i in range(1, 7)]
        res = self.pipe.ingest(self.write_csv("part.csv", rows))
        self.assertEqual((res.status, res.inserted, res.quarantined), ("LOADED_WITH_WARNINGS", 4, 2))
        self.assertIn("PARTIAL_TYPE_DRIFT", {e.drift_type for e in res.events})

    def test_absent_optional_column_does_not_wipe_existing_values(self):
        self.pipe.ingest(self.write_csv("d1.csv", [good_row(1)]))
        no_channel = [c for c in BASE if c != "channel"]
        row = {k: v for k, v in good_row(1, amount="150.00").items() if k != "channel"}
        self.pipe.ingest(self.write_csv("d2.csv", [row], no_channel))
        self.assertEqual(self.q("SELECT amount, channel FROM curated_orders"), [(150.0, "WEB")])


class FileLevelFailureTests(PipelineTestCase):
    def test_empty_and_undecodable_files_are_rejected_not_crashed(self):
        for name, data in (("empty.csv", b""), ("binary.csv", b"\xff\xfe\x00\x01\x02\x80\x81")):
            res = self.pipe.ingest(self.write_bytes(name, data))
            self.assertEqual(res.status, "REJECTED", name)
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM curated_orders"), 0)
        self.assertEqual(len(self.sink.alerts), 2)

    def test_crash_mid_load_rolls_back_everything_and_a_retry_succeeds(self):
        path = self.write_csv("a.csv", [good_row(i) for i in range(1, 4)])
        with mock.patch.object(type(self.pipe), "_load_rows", side_effect=RuntimeError("disk full")):
            with self.assertRaises(RuntimeError):
                self.pipe.ingest(path)
        for table in ("file_registry", "raw_records", "curated_orders", "quarantine_records", "drift_events"):
            self.assertEqual(self.scalar(f"SELECT COUNT(*) FROM {table}"), 0, table)   # atomic per file
        self.assertEqual(self.pipe.ingest(path).inserted, 3)                           # safe to re-run

    def test_alert_sink_failure_does_not_break_ingestion_or_lose_the_incident(self):
        pipe = self.make_pipeline(sinks=[ExplodingSink(), self.sink])
        row = {("total_amount" if k == "amount" else k): v for k, v in good_row(1).items()}
        path = self.write_csv("x.csv", [row], header=RENAMED)
        res = pipe.ingest(path)
        self.assertEqual(res.status, "QUARANTINED_FILE")
        self.assertEqual(self.scalar("SELECT COUNT(*) FROM incidents"), 1)             # the DB row is the record
        self.assertEqual(len(self.sink.alerts), 1)                                     # later sinks still fire


class ContractGuardTests(PipelineTestCase):
    def test_contract_column_without_a_curated_column_is_refused(self):
        raw = Contract.load(ROOT / "config" / "contract.json").to_dict()
        raw["columns"]["brand_new"] = {"type": "string", "required": False}
        with self.assertRaises(Exception) as ctx:
            self.pipe.set_contract(Contract.from_dict(raw))
        self.assertIn("brand_new", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
