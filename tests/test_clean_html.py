import unittest

from semantic_inflation.text.clean_html import html_to_text
from semantic_inflation.text.features import compute_features_from_text


class TestCleanHtml(unittest.TestCase):
    def test_table_rows_keep_kpi_in_single_line(self) -> None:
        html = """
        <html>
          <body>
            <table>
              <tr>
                <th>Scope 1 emissions</th>
                <td>1,234</td>
                <td>metric tons CO2e</td>
              </tr>
            </table>
          </body>
        </html>
        """
        text = html_to_text(html, extractor="bs4", keep_tables=True)
        self.assertIn("Scope 1 emissions", text)
        self.assertIn("1,234", text)
        self.assertIn("metric tons CO2e", text)
        self.assertIn("Scope 1 emissions | 1,234 | metric tons CO2e", text)

        feats = compute_features_from_text(text)
        self.assertGreaterEqual(feats["sentences_kpi"], 1)


if __name__ == "__main__":
    unittest.main()
