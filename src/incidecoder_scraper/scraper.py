"""High level INCIDecoder scraping utilities."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Dict, Generator, Iterable, List, Optional, Sequence, Set, Tuple

LOGGER = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "close",
}


class HttpError(RuntimeError):
    """Raised when an unrecoverable HTTP error occurs."""

    def __init__(self, url: str, status: int, message: str | None = None) -> None:
        super().__init__(f"HTTP {status} while fetching {url}: {message or ''}")
        self.url = url
        self.status = status
        self.message = message or ""


class HttpClient:
    """Lightweight HTTP client with retry and throttling support."""

    def __init__(
        self,
        base_url: str = "https://incidecoder.com",
        *,
        timeout: float = 20.0,
        max_retries: int = 3,
        throttle_seconds: float = 1.0,
        headers: Optional[Dict[str, str]] = None,
        jitter: float = 0.25,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.throttle_seconds = throttle_seconds
        self.headers = dict(DEFAULT_HEADERS)
        if headers:
            self.headers.update(headers)
        self._opener = urllib.request.build_opener()
        self._last_request_time: float = 0.0
        self._jitter = jitter

    def _throttle(self) -> None:
        if self.throttle_seconds <= 0:
            return
        now = time.monotonic()
        delta = now - self._last_request_time
        target = self.throttle_seconds + random.uniform(0, self._jitter)
        if delta < target:
            sleep_for = target - delta
            LOGGER.debug("Throttling for %.2f seconds", sleep_for)
            time.sleep(sleep_for)
        self._last_request_time = time.monotonic()

    def build_url(self, path_or_url: str) -> str:
        if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
            return path_or_url
        return urllib.parse.urljoin(self.base_url + "/", path_or_url.lstrip("/"))

    def fetch(self, path_or_url: str) -> str:
        body, encoding = self._fetch_bytes_and_encoding(path_or_url)
        return body.decode(encoding, errors="replace")

    def fetch_bytes(self, path_or_url: str) -> bytes:
        body, _ = self._fetch_bytes_and_encoding(path_or_url)
        return body

    def _fetch_bytes_and_encoding(self, path_or_url: str) -> Tuple[bytes, str]:
        url = self.build_url(path_or_url)
        attempt = 0
        while True:
            attempt += 1
            self._throttle()
            request = urllib.request.Request(url, headers=self.headers)
            try:
                with self._opener.open(request, timeout=self.timeout) as response:
                    status = getattr(response, "status", response.getcode())
                    if status != 200:
                        raise HttpError(url, status, response.reason if hasattr(response, "reason") else None)
                    body = response.read()
                    encoding = response.headers.get_content_charset() or "utf-8"
                    return body, encoding
            except urllib.error.HTTPError as exc:
                status = exc.code
                if status in {403, 429, 500, 502, 503, 504} and attempt <= self.max_retries:
                    backoff = min(60.0, (self.throttle_seconds or 1.0) * (2 ** (attempt - 1)))
                    LOGGER.warning(
                        "Transient HTTP %s for %s (attempt %s/%s), sleeping %.1fs",
                        status,
                        url,
                        attempt,
                        self.max_retries,
                        backoff,
                    )
                    time.sleep(backoff)
                    continue
                raise HttpError(url, status, str(exc))
            except urllib.error.URLError as exc:
                if attempt <= self.max_retries:
                    backoff = min(60.0, (self.throttle_seconds or 1.0) * (2 ** (attempt - 1)))
                    LOGGER.warning(
                        "Network error for %s (attempt %s/%s): %s; sleeping %.1fs",
                        url,
                        attempt,
                        self.max_retries,
                        exc,
                        backoff,
                    )
                    time.sleep(backoff)
                    continue
                raise RuntimeError(f"Failed to fetch {url}: {exc}")


class LinkCollector(HTMLParser):
    """Collect anchor hrefs that start with the provided prefixes."""

    def __init__(self, prefixes: Sequence[str]) -> None:
        super().__init__()
        self.prefixes = tuple(prefixes)
        self.links: List[str] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag != "a":
            return
        attr_map = dict(attrs)
        href = attr_map.get("href")
        if not href:
            return
        for prefix in self.prefixes:
            if href.startswith(prefix):
                sanitized = href.split("#", 1)[0]
                if sanitized not in self.links:
                    self.links.append(sanitized)
                break


@dataclass
class Ingredient:
    """Structured ingredient information."""

    name: str
    url: str
    extra: Dict[str, str] = field(default_factory=dict)


@dataclass
class Product:
    """Structured product information."""

    url: str
    name: Optional[str] = None
    brand: Optional[str] = None
    description: Optional[str] = None
    image_url: Optional[str] = None
    image_path: Optional[str] = None
    rating_value: Optional[float] = None
    rating_count: Optional[int] = None
    categories: List[str] = field(default_factory=list)
    ingredients: List[Ingredient] = field(default_factory=list)
    json_ld: Optional[Dict] = None
    raw_brand_links: List[str] = field(default_factory=list)


class ProductHTMLParser(HTMLParser):
    """Extract product level information from HTML."""

    def __init__(self, base_url: str) -> None:
        super().__init__()
        self.base_url = base_url.rstrip("/")
        self._reset_state()

    def _reset_state(self) -> None:
        self._in_h1 = False
        self._current_text: List[str] = []
        self._json_ld_blocks: List[str] = []
        self._in_json_ld = False
        self._json_buffer: List[str] = []
        self._meta: Dict[str, str] = {}
        self._brand_links: Set[str] = set()
        self._current_link_type: Optional[str] = None
        self._current_link_href: Optional[str] = None
        self._link_text_buffer: List[str] = []
        self.ingredients: Dict[str, Ingredient] = {}
        self.title: Optional[str] = None

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        attr_map = dict(attrs)
        if tag == "script" and attr_map.get("type", "").lower() == "application/ld+json":
            self._in_json_ld = True
            self._json_buffer = []
        elif tag == "meta":
            key = (attr_map.get("property") or attr_map.get("name") or "").strip().lower()
            content = attr_map.get("content")
            if key and content:
                self._meta[key] = content.strip()
        elif tag == "h1":
            self._in_h1 = True
            self._current_text = []
        elif tag == "a":
            href = attr_map.get("href") or ""
            if href.startswith("/ingredients/"):
                self._current_link_type = "ingredient"
                self._current_link_href = href
                self._link_text_buffer = []
                data_name = attr_map.get("data-name") or attr_map.get("title")
                if data_name:
                    full_url = urllib.parse.urljoin(self.base_url + "/", href.lstrip("/"))
                    key = self._ingredient_key(full_url, data_name)
                    self.ingredients[key] = Ingredient(name=data_name.strip(), url=full_url)
            elif href.startswith("/brand") or href.startswith("/brands/"):
                full_url = urllib.parse.urljoin(self.base_url + "/", href.lstrip("/"))
                self._brand_links.add(full_url)
                self._current_link_type = "brand"
                self._current_link_href = href
                self._link_text_buffer = []
            else:
                self._current_link_type = None
                self._current_link_href = None
                self._link_text_buffer = []
        else:
            self._current_link_type = self._current_link_type

    def handle_endtag(self, tag: str) -> None:
        if tag == "script" and self._in_json_ld:
            self._in_json_ld = False
            block = "".join(self._json_buffer).strip()
            if block:
                self._json_ld_blocks.append(block)
        elif tag == "h1" and self._in_h1:
            self._in_h1 = False
            text = " ".join(part.strip() for part in self._current_text if part.strip())
            if text:
                self.title = text
        elif tag == "a" and self._current_link_type and self._current_link_href:
            text = " ".join(part.strip() for part in self._link_text_buffer if part.strip())
            if self._current_link_type == "ingredient":
                full_url = urllib.parse.urljoin(self.base_url + "/", self._current_link_href.lstrip("/"))
                key = self._ingredient_key(full_url, text)
                if key not in self.ingredients:
                    self.ingredients[key] = Ingredient(
                        name=text or self._current_link_href.split("/")[-1].replace("-", " ").title(),
                        url=full_url,
                    )
                elif text:
                    self.ingredients[key].name = text
            elif self._current_link_type == "brand" and text:
                full_url = urllib.parse.urljoin(self.base_url + "/", self._current_link_href.lstrip("/"))
                self._brand_links.add(full_url)
                # store human readable brand name for later heuristics
                self._meta.setdefault("brand:text", text)
            self._current_link_type = None
            self._current_link_href = None
            self._link_text_buffer = []

    def handle_data(self, data: str) -> None:
        if self._in_json_ld:
            self._json_buffer.append(data)
        if self._in_h1:
            self._current_text.append(data)
        if self._current_link_type:
            self._link_text_buffer.append(data)

    def parse(self, html: str, url: str) -> Product:
        self._reset_state()
        self.feed(html)
        self.close()
        product = Product(url=url)
        product.raw_brand_links = sorted(self._brand_links)
        json_ld_data: Optional[Dict] = None
        for block in self._json_ld_blocks:
            try:
                parsed = json.loads(block)
            except json.JSONDecodeError:
                continue
            for node in self._iter_json_nodes(parsed):
                if isinstance(node, dict) and node.get("@type") in {"Product", "ProductGroup"}:
                    json_ld_data = node
                    break
            if json_ld_data:
                break
        product.json_ld = json_ld_data
        if json_ld_data:
            product.name = json_ld_data.get("name") or product.name
            brand_info = json_ld_data.get("brand")
            if isinstance(brand_info, dict):
                product.brand = brand_info.get("name") or brand_info.get("brand") or product.brand
            elif isinstance(brand_info, str):
                product.brand = brand_info
            product.description = json_ld_data.get("description") or product.description
            product.image_url = self._first_of(json_ld_data.get("image")) or product.image_url
            agg = json_ld_data.get("aggregateRating")
            if isinstance(agg, dict):
                try:
                    product.rating_value = float(agg.get("ratingValue")) if agg.get("ratingValue") is not None else None
                except (TypeError, ValueError):
                    product.rating_value = None
                try:
                    product.rating_count = int(agg.get("ratingCount")) if agg.get("ratingCount") is not None else None
                except (TypeError, ValueError):
                    product.rating_count = None
            category = json_ld_data.get("category")
            if isinstance(category, list):
                product.categories = [str(item) for item in category]
            elif isinstance(category, str):
                product.categories = [category]
            ingredient_data = json_ld_data.get("hasIngredient") or json_ld_data.get("ingredient") or json_ld_data.get("ingredients")
            if isinstance(ingredient_data, list):
                for item in ingredient_data:
                    if isinstance(item, dict):
                        name = item.get("name") or item.get("@id") or item.get("identifier")
                        if not name:
                            continue
                        url_value = item.get("@id") or item.get("url")
                        if isinstance(url_value, str):
                            url_value = urllib.parse.urljoin(self.base_url + "/", url_value.lstrip("/"))
                        else:
                            url_value = ""
                        key = self._ingredient_key(url_value, name)
                        if key not in self.ingredients:
                            self.ingredients[key] = Ingredient(name=name, url=url_value)
                        else:
                            if not self.ingredients[key].url and url_value:
                                self.ingredients[key].url = url_value
                    elif isinstance(item, str):
                        key = self._ingredient_key("", item)
                        self.ingredients.setdefault(key, Ingredient(name=item, url=""))
            elif isinstance(ingredient_data, str):
                for token in ingredient_data.split(","):
                    token = token.strip()
                    if token:
                        key = self._ingredient_key("", token)
                        self.ingredients.setdefault(key, Ingredient(name=token, url=""))
        if not product.name:
            product.name = self.title or self._meta.get("og:title") or self._meta.get("twitter:title")
        if not product.brand:
            if "brand:text" in self._meta:
                product.brand = self._meta["brand:text"]
            else:
                for link in product.raw_brand_links:
                    slug = link.rstrip("/").split("/")[-1]
                    product.brand = slug.replace("-", " ").title()
                    break
        if not product.description:
            product.description = self._meta.get("og:description") or self._meta.get("description")
        if not product.image_url:
            product.image_url = self._meta.get("og:image") or self._meta.get("twitter:image")
        deduped_list: List[Ingredient] = []
        index_by_url: Dict[str, int] = {}
        index_by_name: Dict[str, int] = {}
        for ingredient in self.ingredients.values():
            url_key = ingredient.url.lower() if ingredient.url else None
            name_key = ingredient.name.lower() if ingredient.name else None
            target_index: Optional[int] = None
            if url_key and url_key in index_by_url:
                target_index = index_by_url[url_key]
            elif name_key and name_key in index_by_name:
                target_index = index_by_name[name_key]
            if target_index is None:
                copy = Ingredient(
                    name=ingredient.name,
                    url=ingredient.url,
                    extra=dict(ingredient.extra),
                )
                target_index = len(deduped_list)
                deduped_list.append(copy)
            else:
                copy = deduped_list[target_index]
                if ingredient.name and not copy.name:
                    copy.name = ingredient.name
                if ingredient.url and not copy.url:
                    copy.url = ingredient.url
                if ingredient.extra:
                    copy.extra.update({k: v for k, v in ingredient.extra.items() if v is not None})
            if name_key is not None and name_key not in index_by_name:
                index_by_name[name_key] = target_index
            if url_key is not None and url_key not in index_by_url:
                index_by_url[url_key] = target_index
        product.ingredients = deduped_list
        return product

    def _iter_json_nodes(self, node):  # type: ignore[override]
        if isinstance(node, dict):
            yield node
            for value in node.values():
                yield from self._iter_json_nodes(value)
        elif isinstance(node, list):
            for item in node:
                yield from self._iter_json_nodes(item)

    @staticmethod
    def _first_of(value):
        if isinstance(value, list):
            return value[0] if value else None
        return value

    @staticmethod
    def _ingredient_key(url: str, name: Optional[str]) -> str:
        if url:
            return url.lower()
        if name:
            return name.strip().lower()
        return ""


class IncidecoderScraper:
    """Coordinate product discovery, scraping and persistence."""

    def __init__(
        self,
        http_client: Optional[HttpClient] = None,
        *,
        base_url: str = "https://incidecoder.com",
        image_dir: Optional[str] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = http_client or HttpClient(base_url=self.base_url)
        self.image_dir = image_dir
        if self.image_dir:
            os.makedirs(self.image_dir, exist_ok=True)

    def discover_product_urls(self, strategy: str = "auto") -> Generator[str, None, None]:
        strategies: List[Tuple[str, Generator[str, None, None]]] = []
        if strategy in {"auto", "sitemap"}:
            strategies.append(("sitemap", self._discover_from_sitemaps()))
        if strategy in {"auto", "brands"}:
            strategies.append(("brands", self._discover_from_brands()))
        emitted: Set[str] = set()
        for name, iterator in strategies:
            LOGGER.info("Discovering product URLs via %s strategy", name)
            for url in iterator:
                if url not in emitted:
                    emitted.add(url)
                    yield url
            if emitted:
                LOGGER.info("%s strategy discovered %d unique products", name, len(emitted))
            else:
                LOGGER.warning("%s strategy returned no products", name)

    def _discover_from_sitemaps(self) -> Generator[str, None, None]:
        try:
            index_xml = self.http.fetch("/sitemap.xml")
        except Exception as exc:  # pragma: no cover - network behaviour
            LOGGER.warning("Unable to fetch sitemap index: %s", exc)
            return
        try:
            index_tree = ET.fromstring(index_xml)
        except ET.ParseError as exc:
            LOGGER.warning("Failed to parse sitemap index: %s", exc)
            return
        namespace = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
        loc_tag = f"{namespace}loc"
        sitemaps = [elem.text for elem in index_tree.iter(loc_tag) if elem.text]
        for sitemap_url in sitemaps:
            if not sitemap_url:
                continue
            if "product" not in sitemap_url.lower():
                continue
            try:
                xml = self.http.fetch(sitemap_url)
            except Exception as exc:  # pragma: no cover - network behaviour
                LOGGER.warning("Failed to fetch sitemap %s: %s", sitemap_url, exc)
                continue
            try:
                tree = ET.fromstring(xml)
            except ET.ParseError:
                continue
            for loc in tree.iter(loc_tag):
                if not loc.text:
                    continue
                if "/products/" in loc.text:
                    yield loc.text

    def _discover_from_brands(self) -> Generator[str, None, None]:
        visited_brands: Set[str] = set()
        offset = 1
        consecutive_empty = 0
        while True:
            path = f"/brands?offset={offset}"
            try:
                html = self.http.fetch(path)
            except Exception as exc:  # pragma: no cover - network behaviour
                LOGGER.warning("Failed to fetch brand index %s: %s", path, exc)
                break
            collector = LinkCollector(("/brands/",))
            collector.feed(html)
            discovered_links = []
            for link in collector.links:
                absolute = self.http.build_url(link)
                if absolute in visited_brands:
                    continue
                visited_brands.add(absolute)
                discovered_links.append(absolute)
            if not discovered_links:
                consecutive_empty += 1
                if consecutive_empty >= 2:
                    break
            else:
                consecutive_empty = 0
                for brand_link in discovered_links:
                    yield from self._discover_products_for_brand(brand_link)
            offset += 1

    def _discover_products_for_brand(self, brand_url: str) -> Generator[str, None, None]:
        seen_pages: Set[str] = set()
        queue = [brand_url]
        brand_path = urllib.parse.urlparse(brand_url).path
        while queue:
            current_url = queue.pop(0)
            if current_url in seen_pages:
                continue
            seen_pages.add(current_url)
            try:
                html = self.http.fetch(current_url)
            except Exception as exc:  # pragma: no cover - network behaviour
                LOGGER.warning("Failed to fetch brand page %s: %s", current_url, exc)
                continue
            product_collector = LinkCollector(("/products/",))
            product_collector.feed(html)
            for link in product_collector.links:
                yield self.http.build_url(link)
            pagination_collector = LinkCollector(("?offset=", "/brands/"))
            pagination_collector.feed(html)
            for link in pagination_collector.links:
                if "?offset=" not in link:
                    continue
                absolute = urllib.parse.urljoin(current_url + "", link)
                parsed = urllib.parse.urlparse(absolute)
                if parsed.path != brand_path:
                    continue
                if absolute not in seen_pages and absolute not in queue:
                    queue.append(absolute)

    def _slugify(self, value: str) -> str:
        cleaned = []
        value = value.lower()
        for char in value:
            if char.isalnum():
                cleaned.append(char)
            elif char in {" ", "-", "_", "/"}:
                cleaned.append("-")
        slug = "".join(cleaned)
        slug = "-".join(part for part in slug.split("-") if part)
        return slug or "product"

    def _image_filename(self, product: Product, image_url: str) -> str:
        parsed_image = urllib.parse.urlparse(image_url)
        extension = os.path.splitext(parsed_image.path)[1].lower()
        if not extension or len(extension) > 5:
            extension = ".jpg"
        slug_source = urllib.parse.urlparse(product.url).path.rstrip("/").split("/")[-1]
        if not slug_source:
            slug_source = product.name or "product"
        slug = self._slugify(slug_source)
        digest = hashlib.sha1(product.url.encode("utf-8")).hexdigest()[:12]
        return f"{slug}-{digest}{extension}"

    def _download_product_image(self, product: Product) -> Optional[str]:
        if not self.image_dir or not product.image_url:
            return None
        os.makedirs(self.image_dir, exist_ok=True)
        filename = self._image_filename(product, product.image_url)
        destination = os.path.join(self.image_dir, filename)
        if os.path.exists(destination):
            return destination
        try:
            content = self.http.fetch_bytes(product.image_url)
        except Exception as exc:  # pragma: no cover - network behaviour
            LOGGER.warning("Failed to download image %s: %s", product.image_url, exc)
            return None
        try:
            with open(destination, "wb") as handle:
                handle.write(content)
        except OSError as exc:
            LOGGER.error("Unable to write image %s: %s", destination, exc)
            return None
        return destination

    def fetch_product(self, url: str) -> Product:
        html = self.http.fetch(url)
        parser = ProductHTMLParser(self.base_url)
        return parser.parse(html, url)

    def scrape(
        self,
        store: "DataStore",
        *,
        limit: Optional[int] = None,
        strategy: str = "auto",
        resume: bool = True,
    ) -> None:
        count = 0
        for product_url in self.discover_product_urls(strategy=strategy):
            if limit is not None and count >= limit:
                LOGGER.info("Reached limit of %d products; stopping", limit)
                break
            if resume and store.has_product(product_url):
                LOGGER.debug("Skipping already stored product %s", product_url)
                continue
            try:
                product = self.fetch_product(product_url)
            except Exception as exc:  # pragma: no cover - network behaviour
                LOGGER.error("Failed to scrape %s: %s", product_url, exc)
                continue
            image_path = self._download_product_image(product)
            if image_path:
                product.image_path = image_path
            store.save_product(product)
            count += 1
            LOGGER.info("Stored product %s (%d ingredients)", product_url, len(product.ingredients))


from .storage import DataStore  # noqa: E402  (circular import guard)

__all__ = [
    "Ingredient",
    "Product",
    "ProductHTMLParser",
    "HttpClient",
    "IncidecoderScraper",
]
