import os
import tempfile
import unittest

from incidecoder_scraper.scraper import Ingredient, Product
from incidecoder_scraper.storage import DataStore


class DataStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "scraper.db")
        self.store = DataStore(self.db_path)

    def tearDown(self) -> None:
        self.store.close()
        self.tmpdir.cleanup()

    def test_save_and_iter_products_roundtrip(self) -> None:
        product = Product(
            url="https://incidecoder.com/products/example",
            name="Example Product",
            brand="Example Brand",
            description="Demo",
            image_url="https://cdn.example.com/image.jpg",
            image_path="/images/example-product.jpg",
            rating_value=4.7,
            rating_count=42,
            categories=["Serum", "Hydration"],
            json_ld={"@type": "Product", "name": "Example Product"},
            ingredients=[
                Ingredient(
                    name="Water",
                    url="https://incidecoder.com/ingredients/water",
                    extra={"note": "solvent"},
                ),
                Ingredient(
                    name="Glycerin",
                    url="https://incidecoder.com/ingredients/glycerin",
                ),
            ],
        )

        self.assertFalse(self.store.has_product(product.url))

        self.store.save_product(product)

        self.assertTrue(self.store.has_product(product.url))
        products = list(self.store.iter_products())
        self.assertEqual(len(products), 1)
        loaded = products[0]
        self.assertEqual(loaded.url, product.url)
        self.assertEqual(loaded.name, product.name)
        self.assertEqual(loaded.brand, product.brand)
        self.assertEqual(loaded.description, product.description)
        self.assertEqual(loaded.image_url, product.image_url)
        self.assertEqual(loaded.image_path, product.image_path)
        self.assertEqual(loaded.rating_value, product.rating_value)
        self.assertEqual(loaded.rating_count, product.rating_count)
        self.assertEqual(loaded.categories, product.categories)
        self.assertEqual(loaded.json_ld, product.json_ld)
        loaded_ingredients = sorted(loaded.ingredients, key=lambda ingredient: ingredient.name)
        expected_ingredients = sorted(product.ingredients, key=lambda ingredient: ingredient.name)
        self.assertEqual(loaded_ingredients, expected_ingredients)

    def test_save_product_overwrites_existing_entries(self) -> None:
        original = Product(
            url="https://incidecoder.com/products/example",
            name="Example Product",
            brand="Example Brand",
            description="Demo",
            image_url="https://cdn.example.com/image.jpg",
            image_path="/images/example-product.jpg",
            rating_value=4.2,
            rating_count=10,
            categories=["Serum"],
            json_ld={"@type": "Product", "name": "Example Product"},
            ingredients=[
                Ingredient(
                    name="Water",
                    url="https://incidecoder.com/ingredients/water",
                ),
            ],
        )
        updated = Product(
            url=original.url,
            name=original.name,
            brand=original.brand,
            description=original.description,
            image_url=original.image_url,
            image_path=original.image_path,
            rating_value=4.9,
            rating_count=55,
            categories=["Serum", "Hydration"],
            json_ld=original.json_ld,
            ingredients=[
                Ingredient(
                    name="Niacinamide",
                    url="https://incidecoder.com/ingredients/niacinamide",
                ),
            ],
        )

        self.store.save_product(original)
        self.store.save_product(updated)

        products = list(self.store.iter_products())
        self.assertEqual(len(products), 1)
        loaded = products[0]
        self.assertEqual(loaded.rating_value, updated.rating_value)
        self.assertEqual(loaded.rating_count, updated.rating_count)
        self.assertEqual(loaded.categories, updated.categories)
        ingredient_names = [ingredient.name for ingredient in loaded.ingredients]
        self.assertEqual(ingredient_names, ["Niacinamide"])


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
