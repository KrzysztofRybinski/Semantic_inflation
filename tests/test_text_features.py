import unittest
from pathlib import Path

from semantic_inflation.paths import repo_root
from semantic_inflation.text.features import compute_features_from_file, compute_features_from_text


class TestTextFeatures(unittest.TestCase):
    def test_fixture_has_expected_counts(self) -> None:
        fixture = repo_root() / "data" / "fixtures" / "sample_filing.html"
        feats = compute_features_from_file(fixture)

        self.assertGreaterEqual(feats["sentences_total"], 1)
        self.assertEqual(feats["sentences_env"], 5)
        self.assertEqual(feats["sentences_aspirational"], 2)
        self.assertEqual(feats["sentences_kpi"], 2)
        self.assertAlmostEqual(feats["A_share"], 2 / 5)
        self.assertAlmostEqual(feats["Q_share"], 2 / 5)

    def test_plain_text_path(self) -> None:
        text = "We aim to reduce emissions. In 2023, Scope 1 emissions were 10 tons CO2e."
        feats = compute_features_from_text(text)
        self.assertEqual(feats["sentences_env"], 2)
        self.assertEqual(feats["sentences_aspirational"], 1)
        self.assertEqual(feats["sentences_kpi"], 1)


if __name__ == "__main__":
    unittest.main()

