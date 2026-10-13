"""The AI component: untrusted-output handling. No network; the LLM is a scripted fake."""
import json
import unittest

from schema_drift.ai import IncidentExplainer

from .helpers import LLMError, PipelineTestCase, ScriptedClient, good_reply, good_row, make_evidence, RENAMED_HEADER


class ExplainerValidationTests(unittest.TestCase):
    def explain(self, *replies, **kw):
        client = ScriptedClient(*replies)
        return IncidentExplainer(client, **kw).explain(make_evidence()), client

    def test_valid_reply_is_accepted_with_its_mapping(self):
        exp, _ = self.explain(good_reply())
        self.assertEqual((exp.status, exp.attempts), ("OK", 1))
        self.assertEqual(exp.suggested_mapping, {"total_amount": "amount", "order_status": "status"})

    def test_no_model_configured_gives_deterministic_fallback(self):
        exp = IncidentExplainer(None).explain(make_evidence())
        self.assertEqual((exp.status, exp.fallback_reason), ("FALLBACK", "NO_LLM_CONFIGURED"))
        self.assertEqual(exp.suggested_mapping, {"total_amount": "amount", "order_status": "status"})

    def test_garbage_reply_is_retried_with_the_reason_then_falls_back(self):
        exp, client = self.explain("Sorry, I can't help with that.", "{not json")
        self.assertEqual((exp.status, exp.attempts), ("FALLBACK", 2))
        self.assertIn("LLM_REPLY_REJECTED", exp.fallback_reason)
        self.assertIn("rejected by an automatic validator", client.prompts[1])     # self-correction prompt

    def test_second_attempt_can_recover(self):
        exp, _ = self.explain("```nope```", good_reply())
        self.assertEqual((exp.status, exp.attempts), ("OK", 2))

    def test_json_wrapped_in_prose_and_code_fences_is_still_parsed(self):
        exp, _ = self.explain("Here you go:\n```json\n" + good_reply() + "\n```")
        self.assertEqual(exp.status, "OK")

    def test_hallucinated_column_is_rejected(self):
        bad = good_reply(columns_mentioned=["amount", "loyalty_tier"])
        exp, _ = self.explain(bad, bad)
        self.assertEqual(exp.status, "FALLBACK")
        self.assertIn("UNGROUNDED_COLUMNS", exp.fallback_reason)

    def test_hallucinated_mapping_is_rejected(self):
        # status is a real column, but total_amount's values do not conform to it
        bad = good_reply(suggested_mapping={"total_amount": "status"})
        exp, _ = self.explain(bad, bad)
        self.assertEqual(exp.status, "FALLBACK")
        self.assertIn("MAPPING_TYPE_MISMATCH", exp.fallback_reason)

    def test_two_columns_mapped_to_one_target_is_rejected(self):
        bad = good_reply(suggested_mapping={"total_amount": "amount", "order_status": "amount"})
        exp, _ = self.explain(bad, bad)
        self.assertIn("MAPPING_CONFLICT", exp.fallback_reason)

    def test_schema_violations_are_rejected(self):
        for bad in (json.dumps({"summary": "x"}),                                     # missing keys
                    good_reply(confidence=7), good_reply(confidence="high"),         # bad confidence
                    good_reply(recommended_actions=[]), good_reply(summary="  ")):
            exp, _ = self.explain(bad, bad)
            self.assertEqual(exp.status, "FALLBACK", bad[:60])

    def test_low_confidence_keeps_text_drops_mapping_and_warns_humans(self):
        exp, _ = self.explain(good_reply(confidence=0.3))
        self.assertEqual(exp.status, "LOW_CONFIDENCE")
        self.assertEqual(exp.suggested_mapping, {})
        self.assertTrue(exp.recommended_actions[0].startswith("LOW CONFIDENCE"))

    def test_provider_outage_falls_back_immediately_without_retry(self):
        exp, client = self.explain(LLMError("timeout"))
        self.assertEqual((exp.status, exp.attempts), ("FALLBACK", 1))
        self.assertTrue(exp.fallback_reason.startswith("LLM_ERROR"))
        self.assertEqual(len(client.prompts), 1)

    def test_buggy_client_cannot_break_the_explainer(self):
        exp, _ = self.explain(ZeroDivisionError())
        self.assertEqual(exp.status, "FALLBACK")
        self.assertTrue(exp.fallback_reason.startswith("LLM_CLIENT_CRASH"))

    def test_prompt_contains_evidence_not_the_dataset(self):
        _, client = self.explain(good_reply())
        self.assertIn("total_amount", client.prompts[0])
        self.assertNotIn("raw_json", client.prompts[0])


class ExplainerInPipelineTests(PipelineTestCase):
    def incident(self, client):
        pipe = self.make_pipeline(IncidentExplainer(client))
        rows = [{("total_amount" if k == "amount" else k): v for k, v in good_row(i).items()} for i in range(1, 4)]
        res = pipe.ingest(self.write_csv("ren.csv", rows, RENAMED_HEADER))
        return pipe, res

    def test_accepted_ai_explanation_is_stored_and_shown_in_the_alert(self):
        _, res = self.incident(ScriptedClient(good_reply(suggested_mapping={"total_amount": "amount"},
                                                         columns_mentioned=["amount", "total_amount"])))
        self.assertEqual(res.explanation.status, "OK")
        self.assertEqual(self.q("SELECT ai_status, ai_provider FROM incidents"), [("OK", "scripted")])
        self.assertEqual(self.sink.alerts[0].explanation["provider"], "scripted")

    def test_garbage_from_the_model_never_blocks_quarantine_or_alerting(self):
        _, res = self.incident(ScriptedClient("lol", "still not json"))
        self.assertEqual(res.status, "QUARANTINED_FILE")                              # decided before the model ran
        self.assertEqual(self.q("SELECT ai_status FROM incidents"), [("FALLBACK",)])
        self.assertEqual(len(self.sink.alerts), 1)


if __name__ == "__main__":
    unittest.main()


class OfflineStubTests(unittest.TestCase):
    """The demo stub must exercise the real validation path, including each failure mode."""

    def run_mode(self, mode):
        from schema_drift.ai.factory import make_client
        return IncidentExplainer(make_client(f"stub:{mode}")).explain(make_evidence())

    def test_each_stub_mode_lands_in_the_expected_state(self):
        expected = {"ok": "OK", "garbage": "FALLBACK", "hallucinate": "FALLBACK", "lowconf": "LOW_CONFIDENCE",
                    "down": "FALLBACK"}
        for mode, status in expected.items():
            self.assertEqual(self.run_mode(mode).status, status, mode)

    def test_unknown_stub_mode_is_a_configuration_error(self):
        from schema_drift.ai import LLMConfigError
        from schema_drift.ai.factory import make_client
        with self.assertRaises(LLMConfigError):
            make_client("stub:nonsense")
