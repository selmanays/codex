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


if __name__ == "__main__":  # pragma: no cover - test runner hook
    unittest.main()
