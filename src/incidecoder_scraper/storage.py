"""Persistence layer for scraped INCIDecoder data."""

from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import sqlite3
from typing import Iterable, Optional

try:  # pragma: no cover - optional dependency
    import duckdb  # type: ignore
except Exception:  # pragma: no cover - duckdb might be unavailable
    duckdb = None  # type: ignore

from .scraper import Ingredient, Product

LOGGER = logging.getLogger(__name__)


class DataStore:
    """Persist products and ingredients in a DuckDB (preferred) or SQLite database."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.backend = "duckdb" if duckdb else "sqlite"
        if self.backend == "duckdb":
            self.conn = duckdb.connect(path, read_only=False)  # type: ignore[call-arg]
        else:
            should_init_dir = os.path.dirname(path)
            if should_init_dir and not os.path.exists(should_init_dir):
                os.makedirs(should_init_dir, exist_ok=True)
            self.conn = sqlite3.connect(path)
        self._init_schema()

    def close(self) -> None:
        self.conn.close()

    def _init_schema(self) -> None:
        if self.backend == "duckdb":
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    url TEXT PRIMARY KEY,
                    name TEXT,
                    brand TEXT,
                    description TEXT,
                    image_url TEXT,
                    rating_value DOUBLE,
                    rating_count BIGINT,
                    categories TEXT,
                    json_ld TEXT,
                    last_scraped TIMESTAMP
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS product_ingredients (
                    product_url TEXT,
                    ingredient_url TEXT,
                    ingredient_name TEXT,
                    extra TEXT,
                    PRIMARY KEY (product_url, ingredient_url, ingredient_name)
                )
                """
            )
        else:
            cur = self.conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    url TEXT PRIMARY KEY,
                    name TEXT,
                    brand TEXT,
                    description TEXT,
                    image_url TEXT,
                    rating_value REAL,
                    rating_count INTEGER,
                    categories TEXT,
                    json_ld TEXT,
                    last_scraped TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS product_ingredients (
                    product_url TEXT,
                    ingredient_url TEXT,
                    ingredient_name TEXT,
                    extra TEXT,
                    PRIMARY KEY (product_url, ingredient_url, ingredient_name)
                )
                """
            )
            cur.close()
            self.conn.commit()

    def has_product(self, url: str) -> bool:
        query = "SELECT 1 FROM products WHERE url = ? LIMIT 1"
        cur = self.conn.execute(query, [url])
        row = cur.fetchone()
        cur.close()
        return row is not None

    def save_product(self, product: Product) -> None:
        timestamp = _dt.datetime.utcnow().replace(microsecond=0)
        categories = json.dumps(product.categories)
        json_ld = json.dumps(product.json_ld, ensure_ascii=False) if product.json_ld else None
        if self.backend == "duckdb":
            self.conn.execute("BEGIN TRANSACTION")
            self.conn.execute("DELETE FROM product_ingredients WHERE product_url = ?", [product.url])
            self.conn.execute("DELETE FROM products WHERE url = ?", [product.url])
            self.conn.execute(
                """
                INSERT INTO products (
                    url, name, brand, description, image_url,
                    rating_value, rating_count, categories, json_ld, last_scraped
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    product.url,
                    product.name,
                    product.brand,
                    product.description,
                    product.image_url,
                    product.rating_value,
                    product.rating_count,
                    categories,
                    json_ld,
                    timestamp,
                ],
            )
            if product.ingredients:
                ingredient_rows = [
                    (
                        product.url,
                        ingredient.url,
                        ingredient.name,
                        json.dumps(ingredient.extra, ensure_ascii=False) if ingredient.extra else None,
                    )
                    for ingredient in product.ingredients
                ]
                self.conn.executemany(
                    """
                    INSERT INTO product_ingredients (
                        product_url, ingredient_url, ingredient_name, extra
                    ) VALUES (?, ?, ?, ?)
                    """,
                    ingredient_rows,
                )
            self.conn.execute("COMMIT")
        else:
            cur = self.conn.cursor()
            cur.execute("DELETE FROM product_ingredients WHERE product_url = ?", [product.url])
            cur.execute("DELETE FROM products WHERE url = ?", [product.url])
            cur.execute(
                """
                INSERT INTO products (
                    url, name, brand, description, image_url,
                    rating_value, rating_count, categories, json_ld, last_scraped
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    product.url,
                    product.name,
                    product.brand,
                    product.description,
                    product.image_url,
                    product.rating_value,
                    product.rating_count,
                    categories,
                    json_ld,
                    timestamp.isoformat(),
                ],
            )
            if product.ingredients:
                ingredient_rows = [
                    (
                        product.url,
                        ingredient.url,
                        ingredient.name,
                        json.dumps(ingredient.extra, ensure_ascii=False) if ingredient.extra else None,
                    )
                    for ingredient in product.ingredients
                ]
                cur.executemany(
                    """
                    INSERT INTO product_ingredients (
                        product_url, ingredient_url, ingredient_name, extra
                    ) VALUES (?, ?, ?, ?)
                    """,
                    ingredient_rows,
                )
            self.conn.commit()
            cur.close()

    def iter_products(self) -> Iterable[Product]:
        cur = self.conn.execute(
            "SELECT url, name, brand, description, image_url, rating_value, rating_count, categories, json_ld FROM products"
        )
        rows = cur.fetchall()
        cur.close()
        for row in rows:
            url, name, brand, description, image_url, rating_value, rating_count, categories_json, json_ld_json = row
            categories = json.loads(categories_json) if categories_json else []
            json_ld = json.loads(json_ld_json) if json_ld_json else None
            ingredients = list(self._load_ingredients(url))
            yield Product(
                url=url,
                name=name,
                brand=brand,
                description=description,
                image_url=image_url,
                rating_value=rating_value,
                rating_count=rating_count,
                categories=categories,
                json_ld=json_ld,
                ingredients=ingredients,
            )

    def _load_ingredients(self, product_url: str):
        cur = self.conn.execute(
            "SELECT ingredient_url, ingredient_name, extra FROM product_ingredients WHERE product_url = ?",
            [product_url],
        )
        rows = cur.fetchall()
        cur.close()
        for url, name, extra in rows:
            extra_data = json.loads(extra) if extra else {}
            yield Ingredient(name=name, url=url, extra=extra_data)


__all__ = ["DataStore"]
