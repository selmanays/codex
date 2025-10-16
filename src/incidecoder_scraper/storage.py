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
                    materials TEXT,
                    functions TEXT,
                    highlights TEXT,
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
                    materials TEXT,
                    functions TEXT,
                    highlights TEXT,
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
        self._ensure_product_columns()

    def has_product(self, url: str) -> bool:
        query = "SELECT 1 FROM products WHERE url = ? LIMIT 1"
        cur = self.conn.execute(query, [url])
        row = cur.fetchone()
        cur.close()
        return row is not None

    def _ensure_product_columns(self) -> None:
        required_columns = [
            ("materials", "TEXT"),
            ("functions", "TEXT"),
            ("highlights", "TEXT"),
        ]
        for column, definition in required_columns:
            self._ensure_column("products", column, definition)

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        column_lower = column.lower()
        if self.backend == "duckdb":
            query = (
                "SELECT 1 FROM information_schema.columns WHERE lower(table_name) = ? AND lower(column_name) = ? LIMIT 1"
            )
            exists = self.conn.execute(query, [table.lower(), column_lower]).fetchone()
            if not exists:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        else:
            cur = self.conn.execute(f"PRAGMA table_info({table})")
            columns = {row[1].lower() for row in cur.fetchall()}
            cur.close()
            if column_lower not in columns:
                self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
                self.conn.commit()

    def save_product(self, product: Product) -> None:
        timestamp = _dt.datetime.utcnow().replace(microsecond=0)
        categories = json.dumps(product.categories)
        materials = json.dumps(product.materials, ensure_ascii=False)
        functions = json.dumps(product.functions, ensure_ascii=False)
        highlights = json.dumps(product.highlights, ensure_ascii=False)
        json_ld = json.dumps(product.json_ld, ensure_ascii=False) if product.json_ld else None
        if self.backend == "duckdb":
            self.conn.execute("BEGIN TRANSACTION")
            self.conn.execute("DELETE FROM product_ingredients WHERE product_url = ?", [product.url])
            self.conn.execute("DELETE FROM products WHERE url = ?", [product.url])
            self.conn.execute(
                """
                INSERT INTO products (
                    url, name, brand, description, image_url,
                    rating_value, rating_count, categories, materials, functions,
                    highlights, json_ld, last_scraped
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    materials,
                    functions,
                    highlights,
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
                    rating_value, rating_count, categories, materials, functions,
                    highlights, json_ld, last_scraped
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    materials,
                    functions,
                    highlights,
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
            "SELECT url, name, brand, description, image_url, rating_value, rating_count, categories, materials, functions, highlights, json_ld FROM products"
        )
        rows = cur.fetchall()
        cur.close()
        for row in rows:
            (
                url,
                name,
                brand,
                description,
                image_url,
                rating_value,
                rating_count,
                categories_json,
                materials_json,
                functions_json,
                highlights_json,
                json_ld_json,
            ) = row
            categories = json.loads(categories_json) if categories_json else []
            materials = json.loads(materials_json) if materials_json else []
            functions = json.loads(functions_json) if functions_json else []
            highlights = json.loads(highlights_json) if highlights_json else []
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
                materials=materials,
                functions=functions,
                highlights=highlights,
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
