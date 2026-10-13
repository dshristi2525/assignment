import unittest

from schema_drift.contract import Contract
from schema_drift.drift import detect_schema_drift, detect_type_drift, name_score, schema_fingerprint
from schema_drift.models import CRITICAL, INFO, WARNING
from schema_drift.util import ROOT

CONTRACT = Contract.load(ROOT / "config" / "contract.json")
BASE = ["order_id", "customer_id", "order_date", "amount", "status", "channel"]
VALUES = {"order_id": ["1", "2"], "customer_id": ["C0001", "C0002"], "order_date": ["2026-10-01"] * 2,
          "amount": ["10.00", "20.50"], "status": ["PLACED", "SHIPPED"], "channel": ["WEB", "APP"],
          "total_amount": ["10.00", "20.50"], "order_status": ["PLACED", "SHIPPED"],
          "coupon_code": ["SAVE10", "FEST25"]}


def diff(header, contract=CONTRACT):
    return detect_schema_drift(header, contract, lambda c: VALUES.get(c, []))


def types(d):
    return {(e.drift_type, e.severity) for e in d.events}


class SchemaDiffTests(unittest.TestCase):
    def test_identical_schema_has_no_events(self):
        self.assertEqual(diff(BASE).events, [])

    def test_column_reordering_is_not_drift(self):
        shuffled = ["status", "order_id", "channel", "amount", "order_date", "customer_id"]
        self.assertEqual(diff(shuffled).events, [])
        self.assertEqual(schema_fingerprint(shuffled), schema_fingerprint(BASE))

    def test_new_column_is_a_warning_not_a_breaking_change(self):
        d = diff(BASE + ["coupon_code"])
        self.assertEqual(types(d), {("UNEXPECTED_COLUMN", WARNING)})
        self.assertEqual(d.unexpected, ["coupon_code"])

    def test_missing_optional_column_is_a_warning(self):
        d = diff([c for c in BASE if c != "channel"])
        self.assertEqual(types(d), {("MISSING_OPTIONAL_COLUMN", WARNING)})

    def test_missing_required_column_is_critical(self):
        d = diff([c for c in BASE if c != "amount"])
        self.assertEqual(types(d), {("MISSING_REQUIRED_COLUMN", CRITICAL)})
        self.assertEqual(d.missing_required, ["amount"])

    def test_rename_is_inferred_from_name_and_values(self):
        header = ["order_id", "customer_id", "order_date", "total_amount", "order_status", "channel"]
        d = diff(header)
        self.assertEqual(types(d), {("RENAME_SUSPECTED", CRITICAL)})
        found = {e.column: e.evidence["incoming"] for e in d.events}
        self.assertEqual(found, {"amount": "total_amount", "status": "order_status"})
        self.assertEqual(d.candidate_matrix["total_amount"]["amount"], 1.0)
        self.assertEqual(d.candidate_matrix["total_amount"]["status"], 0.0)

    def test_unrelated_new_column_is_not_mistaken_for_a_rename(self):
        # 'channel' vanished and 'coupon_code' appeared: both strings, but names differ and values violate the enum
        d = diff(["order_id", "customer_id", "order_date", "amount", "status", "coupon_code"])
        self.assertEqual(types(d), {("MISSING_OPTIONAL_COLUMN", WARNING), ("UNEXPECTED_COLUMN", WARNING)})

    def test_duplicate_or_blank_header_names_are_critical(self):
        for header in (BASE + ["amount"], BASE + [""]):
            self.assertEqual(types(diff(header)), {("MALFORMED_HEADER", CRITICAL)})

    def test_approved_alias_maps_the_column_and_clears_the_drift(self):
        contract = CONTRACT.with_alias("total_amount", "amount")
        d = diff(["order_id", "customer_id", "order_date", "total_amount", "status", "channel"], contract)
        self.assertEqual(types(d), {("ALIAS_APPLIED", INFO)})
        self.assertEqual(d.alias_map, {"total_amount": "amount"})

    def test_name_score_orders_candidates_sensibly(self):
        self.assertEqual(name_score("amount", "AMOUNT"), 1.0)
        self.assertGreaterEqual(name_score("total_amount", "amount"), 0.9)
        self.assertLess(name_score("coupon_code", "channel"), 0.5)


class TypeDriftTests(unittest.TestCase):
    @staticmethod
    def rows(n_bad, n_good):
        return [(i, {"order_date": "05/10/2026" if i < n_bad else "2026-10-05"}) for i in range(n_bad + n_good)]

    def test_majority_bad_values_is_file_level_type_drift(self):
        events = detect_type_drift(self.rows(6, 4), CONTRACT, ["order_date"])
        self.assertEqual([(e.drift_type, e.severity) for e in events], [("TYPE_DRIFT", CRITICAL)])

    def test_minority_bad_values_is_left_to_row_validation(self):
        self.assertEqual(detect_type_drift(self.rows(3, 7), CONTRACT, ["order_date"]), [])

    def test_exactly_at_threshold_is_not_drift(self):
        self.assertEqual(detect_type_drift(self.rows(5, 5), CONTRACT, ["order_date"]), [])

    def test_empty_column_is_ignored(self):
        self.assertEqual(detect_type_drift([(1, {"order_date": ""})], CONTRACT, ["order_date"]), [])


if __name__ == "__main__":
    unittest.main()
