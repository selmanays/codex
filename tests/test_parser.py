import json
import unittest

from incidecoder_scraper.scraper import ProductHTMLParser


class ProductHTMLParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = ProductHTMLParser("https://incidecoder.com")

    def test_parses_json_ld_and_ingredients(self) -> None:
        html = """
        <html>
          <head>
            <meta property="og:title" content="Brandless Magic Serum" />
            <meta property="og:description" content="A simple test serum." />
            <meta property="og:image" content="https://cdn.example.com/image.jpg" />
            <script type="application/ld+json">
              {
                "@context": "https://schema.org",
                "@type": "Product",
                "name": "Magic Serum",
                "brand": {"@type": "Brand", "name": "Brandless"},
                "description": "Hydrating serum",
                "aggregateRating": {"@type": "AggregateRating", "ratingValue": "4.6", "ratingCount": "18"},
                "category": ["Serum", "Treatment"],
                "hasIngredient": [
                  {"@type": "Thing", "name": "Water", "@id": "/ingredients/water"},
                  "Glycerin"
                ]
              }
            </script>
          </head>
          <body>
            <nav>
              <a href="/brands/brandless">Brandless</a>
            </nav>
            <h1>Magic Serum</h1>
            <section>
              <a href="/ingredients/water">Water</a>
              <a href="/ingredients/glycerin" data-name="Glycerin">Glycerin</a>
            </section>
          </body>
        </html>
        """
        product = self.parser.parse(html, "https://incidecoder.com/products/magic-serum")
        self.assertEqual(product.name, "Magic Serum")
        self.assertEqual(product.brand, "Brandless")
        self.assertEqual(product.description, "Hydrating serum")
        self.assertAlmostEqual(product.rating_value, 4.6)
        self.assertEqual(product.rating_count, 18)
        self.assertEqual(product.categories, ["Serum", "Treatment"])
        ingredient_names = sorted(ing.name for ing in product.ingredients)
        self.assertEqual(ingredient_names, ["Glycerin", "Water"])
        ingredient_urls = sorted(ing.url for ing in product.ingredients)
        self.assertEqual(
            ingredient_urls,
            [
                "https://incidecoder.com/ingredients/glycerin",
                "https://incidecoder.com/ingredients/water",
            ],
        )
        self.assertIsNotNone(product.json_ld)
        self.assertEqual(product.materials, [])
        self.assertEqual(product.functions, [])
        self.assertEqual(product.highlights, [])

    def test_parses_detailpage_sections(self) -> None:
        html = """
        <html>
          <head>
            <meta property=\"og:title\" content=\"Detail Serum\" />
            <meta name=\"description\" content=\"Fallback description\" />
          </head>
          <body>
            <div class=\"detailpage\">
              <h1>Detail Serum</h1>
              <section class=\"detailpage__description\">
                <p>Luxurious hydration booster.</p>
                <p>Leaves skin feeling soft.</p>
              </section>
              <section class=\"detailpage__materials\">
                <ul>
                  <li>Bio-Ceramide Complex</li>
                  <li>Peptide Blend</li>
                </ul>
              </section>
              <section class=\"detailpage__functions\">
                <div class=\"detailpage__function\" data-function=\"Soothing\">
                  <h3>Soothing</h3>
                  <ul>
                    <li>Calms irritation</li>
                    <li><a href=\"/function/hydration\">Adds moisture</a></li>
                  </ul>
                </div>
              </section>
              <section class=\"detailpage__highlights\">
                <div class=\"detailpage__highlight\">
                  <h4>Why we love it</h4>
                  <p>Lightweight texture</p>
                  <ul><li>Fragrance-free</li></ul>
                </div>
              </section>
              <section class=\"detailpage__ingredients\">
                <div class=\"detailpage__ingredient\" data-name=\"Betaine\" data-ingredient-url=\"/ingredients/betaine\">
                  <a href=\"/ingredients/betaine\" data-name=\"Betaine\">Betaine</a>
                  <div class=\"ingredient-tooltip\" data-title=\"Betaine (Trimethylglycine)\">
                    <p>Humectant</p>
                    <a href=\"/ingredient-function/humectant\">Learn more</a>
                  </div>
                </div>
              </section>
            </div>
          </body>
        </html>
        """
        product = self.parser.parse(html, "https://incidecoder.com/products/detail-serum")
        self.assertEqual(product.name, "Detail Serum")
        self.assertEqual(
            product.description,
            "Luxurious hydration booster. Leaves skin feeling soft.",
        )
        self.assertEqual(
            product.materials,
            ["Bio-Ceramide Complex", "Peptide Blend"],
        )
        self.assertEqual(len(product.functions), 1)
        function_entry = product.functions[0]
        self.assertEqual(function_entry.get("name"), "Soothing")
        self.assertEqual(
            function_entry.get("items"),
            ["Calms irritation", "Adds moisture"],
        )
        self.assertIn(
            "https://incidecoder.com/function/hydration",
            function_entry.get("links", []),
        )
        self.assertEqual(len(product.highlights), 1)
        highlight_entry = product.highlights[0]
        self.assertEqual(highlight_entry.get("title"), "Why we love it")
        self.assertEqual(highlight_entry.get("text"), "Lightweight texture")
        self.assertEqual(highlight_entry.get("items"), ["Fragrance-free"])
        self.assertEqual(len(product.ingredients), 1)
        ingredient = product.ingredients[0]
        self.assertEqual(ingredient.name, "Betaine")
        self.assertEqual(
            ingredient.extra.get("tooltip_title"),
            "Betaine (Trimethylglycine)",
        )
        self.assertEqual(ingredient.extra.get("tooltip_text"), "Humectant")
        self.assertEqual(
            ingredient.extra.get("tooltip_links"),
            ["https://incidecoder.com/ingredient-function/humectant"],
        )

    def test_parses_detailpage_variant_root_class(self) -> None:
        html = """
        <html>
          <body>
            <section class=\"product-detailpage\">
              <div class=\"detailpage__description\">
                <p>Balanced hydration.</p>
              </div>
              <div class=\"detailpage__materials\">
                <ul>
                  <li>Ceramide Complex</li>
                </ul>
              </div>
            </section>
          </body>
        </html>
        """
        product = self.parser.parse(html, "https://incidecoder.com/products/hydration-balance")
        self.assertEqual(product.description, "Balanced hydration.")
        self.assertEqual(product.materials, ["Ceramide Complex"])


if __name__ == "__main__":  # pragma: no cover - test runner hook
    unittest.main()
