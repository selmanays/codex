"""Persistence layer for scraped INCIDecoder data."""

from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import sqlite3
from typing import Iterable, Iterator, Optional, Sequence, Tuple

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
                CREATE TABLE IF NOT EXISTS brands (
                    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL UNIQUE,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed_at TIMESTAMP
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    id BIGINT PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
                    brand_id BIGINT,
                    url TEXT NOT NULL UNIQUE,
                    name TEXT,
                    brand_name TEXT,
                    description TEXT,
                    image_url TEXT,
                    rating_value DOUBLE,
                    rating_count BIGINT,
                    categories TEXT,
                    json_ld TEXT,
                    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    scraped_at TIMESTAMP,
                    FOREIGN KEY (brand_id) REFERENCES brands(id)
                )
                """
            )
            self.conn.execute(
                """
                CREATE TABLE IF NOT EXISTS product_ingredients (
                    product_id BIGINT,
                    ingredient_url TEXT,
                    ingredient_name TEXT,
                    extra TEXT,
                    PRIMARY KEY (product_id, ingredient_url, ingredient_name)
                )
                """
            )
        else:
            cur = self.conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS brands (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    url TEXT NOT NULL UNIQUE,
                    discovered_at TEXT,
                    processed_at TEXT
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    brand_id INTEGER,
                    url TEXT NOT NULL UNIQUE,
                    name TEXT,
                    brand_name TEXT,
                    description TEXT,
                    image_url TEXT,
                    rating_value REAL,
                    rating_count INTEGER,
                    categories TEXT,
                    json_ld TEXT,
                    discovered_at TEXT,
                    scraped_at TEXT,
                    FOREIGN KEY (brand_id) REFERENCES brands(id)
                )
                """
            )
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS product_ingredients (
                    product_id INTEGER,
                    ingredient_url TEXT,
                    ingredient_name TEXT,
                    extra TEXT,
                    PRIMARY KEY (product_id, ingredient_url, ingredient_name)
                )
                """
            )
            cur.close()
            self.conn.commit()

    def has_product(self, url: str) -> bool:
        query = "SELECT scraped_at FROM products WHERE url = ? AND scraped_at IS NOT NULL LIMIT 1"
        cur = self.conn.execute(query, [url])
        row = cur.fetchone()
        cur.close()
        return row is not None

    # ------------------------------------------------------------------
    # Brand helpers

    def add_brands(self, brands: Sequence[Tuple[str, str]]) -> int:
        """Insert newly discovered brands, returning the number of inserts."""

        if not brands:
            return 0
        timestamp = _dt.datetime.utcnow().replace(microsecond=0)
        inserted = 0
        if self.backend == "duckdb":
            self.conn.execute("BEGIN TRANSACTION")
            try:
                for name, url in brands:
                    row = self.conn.execute(
                        "SELECT id, name FROM brands WHERE url = ? LIMIT 1", [url]
                    ).fetchone()
                    if row:
                        brand_id, existing_name = row
                        if name and existing_name != name:
                            self.conn.execute(
                                "UPDATE brands SET name = ? WHERE id = ?",
                                [name, brand_id],
                            )
                        continue
                    self.conn.execute(
                        "INSERT INTO brands (name, url, discovered_at) VALUES (?, ?, ?)",
                        [name, url, timestamp],
                    )
                    inserted += 1
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        else:
            cur = self.conn.cursor()
            try:
                for name, url in brands:
                    cur.execute(
                        "SELECT id, name FROM brands WHERE url = ? LIMIT 1", [url]
                    )
                    row = cur.fetchone()
                    if row:
                        brand_id, existing_name = row
                        if name and existing_name != name:
                            cur.execute(
                                "UPDATE brands SET name = ? WHERE id = ?",
                                [name, brand_id],
                            )
                        continue
                    cur.execute(
                        "INSERT INTO brands (name, url, discovered_at) VALUES (?, ?, ?)",
                        [name, url, timestamp.isoformat()],
                    )
                    inserted += 1
                self.conn.commit()
            finally:
                cur.close()
        return inserted

    def iter_pending_brands(self) -> Iterator[Tuple[int, str, str]]:
        query = "SELECT id, name, url FROM brands WHERE processed_at IS NULL ORDER BY id"
        cur = self.conn.execute(query)
        rows = cur.fetchall()
        cur.close()
        for brand_id, name, url in rows:
            yield brand_id, name, url

    def reset_brand_processing(self) -> None:
        """Mark all brands as pending so they will be re-processed."""

        if self.backend == "duckdb":
            self.conn.execute("UPDATE brands SET processed_at = NULL")
        else:
            cur = self.conn.cursor()
            cur.execute("UPDATE brands SET processed_at = NULL")
            self.conn.commit()
            cur.close()

    def mark_brand_processed(self, brand_id: int) -> None:
        timestamp = _dt.datetime.utcnow().replace(microsecond=0)
        if self.backend == "duckdb":
            self.conn.execute(
                "UPDATE brands SET processed_at = ? WHERE id = ?", [timestamp, brand_id]
            )
        else:
            cur = self.conn.cursor()
            cur.execute(
                "UPDATE brands SET processed_at = ? WHERE id = ?",
                [timestamp.isoformat(), brand_id],
            )
            self.conn.commit()
            cur.close()

    # ------------------------------------------------------------------
    # Product helpers

    def add_products_for_brand(
        self, brand_id: int, products: Sequence[Tuple[str, Optional[str]]]
    ) -> int:
        """Queue products discovered for a brand, returning insert count."""

        if not products:
            return 0
        timestamp = _dt.datetime.utcnow().replace(microsecond=0)
        inserted = 0
        brand_name = self._get_brand_name(brand_id)
        if self.backend == "duckdb":
            self.conn.execute("BEGIN TRANSACTION")
            try:
                for url, name in products:
                    row = self.conn.execute(
                        "SELECT id, name FROM products WHERE url = ? LIMIT 1", [url]
                    ).fetchone()
                    if row:
                        product_id, existing_name = row
                        update_name = name if name and not existing_name else None
                        update_brand = brand_name if brand_name else None
                        if update_name or update_brand or brand_id:
                            self.conn.execute(
                                """
                                UPDATE products
                                SET name = COALESCE(?, name),
                                    brand_id = COALESCE(?, brand_id),
                                    brand_name = COALESCE(?, brand_name)
                                WHERE id = ?
                                """,
                                [update_name, brand_id, update_brand, product_id],
                            )
                        continue
                    self.conn.execute(
                        """
                        INSERT INTO products (brand_id, brand_name, url, name, discovered_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        [brand_id, brand_name, url, name, timestamp],
                    )
                    inserted += 1
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        else:
            cur = self.conn.cursor()
            try:
                for url, name in products:
                    cur.execute(
                        "SELECT id, name FROM products WHERE url = ? LIMIT 1", [url]
                    )
                    row = cur.fetchone()
                    if row:
                        product_id, existing_name = row
                        update_name = name if name and not existing_name else None
                        update_brand = brand_name if brand_name else None
                        if update_name or update_brand or brand_id:
                            cur.execute(
                                """
                                UPDATE products
                                SET name = COALESCE(?, name),
                                    brand_id = COALESCE(?, brand_id),
                                    brand_name = COALESCE(?, brand_name)
                                WHERE id = ?
                                """,
                                [update_name, brand_id, update_brand, product_id],
                            )
                        continue
                    cur.execute(
                        """
                        INSERT INTO products (brand_id, brand_name, url, name, discovered_at)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        [brand_id, brand_name, url, name, timestamp.isoformat()],
                    )
                    inserted += 1
                self.conn.commit()
            finally:
                cur.close()
        return inserted

    def iter_products_to_scrape(
        self, resume: bool = True
    ) -> Iterator[Tuple[int, str, Optional[str], Optional[str]]]:
        base_query = "SELECT id, url, name, brand_name FROM products"
        if resume:
            query = base_query + " WHERE scraped_at IS NULL ORDER BY discovered_at, id"
        else:
            query = base_query + " ORDER BY discovered_at, id"
        cur = self.conn.execute(query)
        rows = cur.fetchall()
        cur.close()
        for row in rows:
            yield row

    def mark_product_scraped(self, product_id: int) -> None:
        timestamp = _dt.datetime.utcnow().replace(microsecond=0)
        if self.backend == "duckdb":
            self.conn.execute(
                "UPDATE products SET scraped_at = ? WHERE id = ?", [timestamp, product_id]
            )
        else:
            cur = self.conn.cursor()
            cur.execute(
                "UPDATE products SET scraped_at = ? WHERE id = ?",
                [timestamp.isoformat(), product_id],
            )
            self.conn.commit()
            cur.close()

    def save_product(self, product: Product) -> None:
        timestamp = _dt.datetime.utcnow().replace(microsecond=0)
        categories = json.dumps(product.categories)
        json_ld = json.dumps(product.json_ld, ensure_ascii=False) if product.json_ld else None
        ingredients = [
            (
                ingredient.url,
                ingredient.name,
                json.dumps(ingredient.extra, ensure_ascii=False) if ingredient.extra else None,
            )
            for ingredient in product.ingredients
        ]
        if self.backend == "duckdb":
            self.conn.execute("BEGIN TRANSACTION")
            try:
                row = self.conn.execute(
                    "SELECT id FROM products WHERE url = ? LIMIT 1", [product.url]
                ).fetchone()
                if row:
                    product_id = row[0]
                    self.conn.execute(
                        """
                        UPDATE products
                        SET name = COALESCE(?, name),
                            brand_name = COALESCE(?, brand_name),
                            description = ?,
                            image_url = ?,
                            rating_value = ?,
                            rating_count = ?,
                            categories = ?,
                            json_ld = ?,
                            scraped_at = ?
                        WHERE id = ?
                        """,
                        [
                            product.name,
                            product.brand,
                            product.description,
                            product.image_url,
                            product.rating_value,
                            product.rating_count,
                            categories,
                            json_ld,
                            timestamp,
                            product_id,
                        ],
                    )
                else:
                    product_id = self.conn.execute(
                        """
                        INSERT INTO products (
                            url, name, brand_name, description, image_url,
                            rating_value, rating_count, categories, json_ld,
                            discovered_at, scraped_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        RETURNING id
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
                            timestamp,
                        ],
                    ).fetchone()[0]
                self.conn.execute(
                    "DELETE FROM product_ingredients WHERE product_id = ?", [product_id]
                )
                if ingredients:
                    rows = [
                        (product_id, url, name, extra) for url, name, extra in ingredients
                    ]
                    self.conn.executemany(
                        """
                        INSERT INTO product_ingredients (
                            product_id, ingredient_url, ingredient_name, extra
                        ) VALUES (?, ?, ?, ?)
                        """,
                        rows,
                    )
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise
        else:
            cur = self.conn.cursor()
            try:
                cur.execute(
                    "SELECT id FROM products WHERE url = ? LIMIT 1", [product.url]
                )
                row = cur.fetchone()
                if row:
                    product_id = row[0]
                    cur.execute(
                        """
                        UPDATE products
                        SET name = COALESCE(?, name),
                            brand_name = COALESCE(?, brand_name),
                            description = ?,
                            image_url = ?,
                            rating_value = ?,
                            rating_count = ?,
                            categories = ?,
                            json_ld = ?,
                            scraped_at = ?
                        WHERE id = ?
                        """,
                        [
                            product.name,
                            product.brand,
                            product.description,
                            product.image_url,
                            product.rating_value,
                            product.rating_count,
                            categories,
                            json_ld,
                            timestamp.isoformat(),
                            product_id,
                        ],
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO products (
                            url, name, brand_name, description, image_url,
                            rating_value, rating_count, categories, json_ld,
                            discovered_at, scraped_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                            timestamp.isoformat(),
                        ],
                    )
                    product_id = cur.lastrowid
                cur.execute(
                    "DELETE FROM product_ingredients WHERE product_id = ?",
                    [product_id],
                )
                if ingredients:
                    rows = [
                        (product_id, url, name, extra) for url, name, extra in ingredients
                    ]
                    cur.executemany(
                        """
                        INSERT INTO product_ingredients (
                            product_id, ingredient_url, ingredient_name, extra
                        ) VALUES (?, ?, ?, ?)
                        """,
                        rows,
                    )
                self.conn.commit()
            finally:
                cur.close()

    def iter_products(self) -> Iterable[Product]:
        cur = self.conn.execute(
            """
            SELECT id, url, name, brand_name, description, image_url,
                   rating_value, rating_count, categories, json_ld
            FROM products
            WHERE scraped_at IS NOT NULL
            """
        )
        rows = cur.fetchall()
        cur.close()
        for row in rows:
            (
                product_id,
                url,
                name,
                brand,
                description,
                image_url,
                rating_value,
                rating_count,
                categories_json,
                json_ld_json,
            ) = row
            categories = json.loads(categories_json) if categories_json else []
            json_ld = json.loads(json_ld_json) if json_ld_json else None
            ingredients = list(self._load_ingredients(product_id))
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

    def _load_ingredients(self, product_id: int):
        cur = self.conn.execute(
            "SELECT ingredient_url, ingredient_name, extra FROM product_ingredients WHERE product_id = ?",
            [product_id],
        )
        rows = cur.fetchall()
        cur.close()
        for url, name, extra in rows:
            extra_data = json.loads(extra) if extra else {}
            yield Ingredient(name=name, url=url, extra=extra_data)

    # ------------------------------------------------------------------

    def _get_brand_name(self, brand_id: int) -> Optional[str]:
        cur = self.conn.execute("SELECT name FROM brands WHERE id = ?", [brand_id])
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None


__all__ = ["DataStore"]
