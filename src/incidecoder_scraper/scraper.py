"""High level INCIDecoder scraping utilities."""

from __future__ import annotations

import copy
import json
import logging
import random
import string
import time
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import Any, Dict, Generator, Iterable, List, Optional, Sequence, Set, Tuple

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
                    return body.decode(encoding, errors="replace")
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
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Product:
    """Structured product information."""

    url: str
    name: Optional[str] = None
    brand: Optional[str] = None
    description: Optional[str] = None
    image_url: Optional[str] = None
    rating_value: Optional[float] = None
    rating_count: Optional[int] = None
    categories: List[str] = field(default_factory=list)
    ingredients: List[Ingredient] = field(default_factory=list)
    materials: List[str] = field(default_factory=list)
    functions: List[Dict[str, Any]] = field(default_factory=list)
    highlights: List[Dict[str, Any]] = field(default_factory=list)
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
        self._link_href_stack: List[str] = []
        self._current_ingredient_href: Optional[str] = None
        self._current_ingredient_data_name: Optional[str] = None
        self.ingredients: Dict[str, Ingredient] = {}
        self.title: Optional[str] = None
        self._detailpage_depth = 0
        self._description_depth = 0
        self._description_buffer: List[str] = []
        self._detail_description: Optional[str] = None
        self._materials_depth = 0
        self._current_material_item_depth = 0
        self._current_material_text: List[str] = []
        self.material_entries: List[str] = []
        self._function_context_stack: List[Dict[str, Any]] = []
        self.function_entries: List[Dict[str, Any]] = []
        self._highlight_context_stack: List[Dict[str, Any]] = []
        self.highlight_entries: List[Dict[str, Any]] = []
        self._ingredient_context_stack: List[Dict[str, Any]] = []

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        attr_map = dict(attrs)
        class_tokens = attr_map.get("class", "")
        class_list = [token.strip() for token in class_tokens.split() if token and token.strip()]
        class_set = set(class_list)
        is_detailpage_root = self._class_matches(class_set, "detailpage") or any(
            token.endswith("detailpage") for token in class_list if token
        )

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

        if is_detailpage_root:
            if self._detailpage_depth == 0:
                self._detailpage_depth = 1
            else:
                self._detailpage_depth += 1
        elif self._detailpage_depth > 0:
            self._detailpage_depth += 1

        inside_detailpage = self._detailpage_depth > 0

        if inside_detailpage and self._class_matches(class_set, "detailpage__description"):
            if self._description_depth == 0:
                self._description_depth = 1
                self._description_buffer = []
            else:
                self._description_depth += 1
        elif self._description_depth > 0:
            self._description_depth += 1

        if inside_detailpage and self._class_matches(class_set, "detailpage__materials"):
            if self._materials_depth == 0:
                self._materials_depth = 1
            else:
                self._materials_depth += 1
        elif self._materials_depth > 0:
            self._materials_depth += 1

        if self._materials_depth > 0 and tag == "li":
            if self._current_material_item_depth == 0:
                self._current_material_text = []
            self._current_material_item_depth += 1

        if inside_detailpage and (
            self._class_matches(class_set, "detailpage__function") or attr_map.get("data-function")
        ):
            function_context: Dict[str, Any] = {
                "depth": 1,
                "name": (attr_map.get("data-function") or "").strip() or None,
                "title_buffer": [],
                "body_buffer": [],
                "items": [],
                "item_depth": 0,
                "current_item": [],
                "title_stack": [],
                "links": [],
            }
            self._function_context_stack.append(function_context)
        elif self._function_context_stack:
            self._function_context_stack[-1]["depth"] += 1

        if inside_detailpage and (
            self._class_matches(class_set, "detailpage__highlight") or "highlight" in class_set
        ):
            highlight_context: Dict[str, Any] = {
                "depth": 1,
                "title_buffer": [],
                "body_buffer": [],
                "items": [],
                "item_depth": 0,
                "current_item": [],
                "title_stack": [],
                "links": [],
            }
            self._highlight_context_stack.append(highlight_context)
        elif self._highlight_context_stack:
            self._highlight_context_stack[-1]["depth"] += 1

        if inside_detailpage and self._class_matches(class_set, "detailpage__ingredient"):
            url_hint = attr_map.get("data-ingredient-url") or ""
            if url_hint:
                url_hint = urllib.parse.urljoin(self.base_url + "/", url_hint.lstrip("/"))
            name_hint = (attr_map.get("data-name") or attr_map.get("data-ingredient-name") or "").strip() or None
            key = self._ingredient_key(url_hint, name_hint)
            ingredient_context: Dict[str, Any] = {
                "depth": 1,
                "key": key or None,
                "name_hint": name_hint,
                "url_hint": url_hint or None,
                "tooltip_depth": 0,
                "tooltip_body_parts": [],
                "tooltip_title_parts": [],
                "tooltip_links": [],
                "tooltip_title_stack": [],
                "tooltip_anchor_depth": 0,
                "data_tooltip_text": attr_map.get("data-tooltip-text"),
                "data_tooltip_link": attr_map.get("data-tooltip-link"),
            }
            tooltip_title_attr = attr_map.get("data-tooltip-title") or attr_map.get("data-title")
            if tooltip_title_attr:
                ingredient_context["tooltip_title_parts"].append(tooltip_title_attr)
            self._ingredient_context_stack.append(ingredient_context)
        elif self._ingredient_context_stack:
            self._ingredient_context_stack[-1]["depth"] += 1

        current_ingredient = self._ingredient_context_stack[-1] if self._ingredient_context_stack else None
        if current_ingredient:
            tooltip_match = (
                self._class_matches(class_set, "tooltip")
                or self._class_matches(class_set, "ingredient-tooltip")
                or attr_map.get("data-tooltip")
            )
            if tooltip_match:
                if current_ingredient["tooltip_depth"] == 0:
                    current_ingredient["tooltip_depth"] = 1
                    current_ingredient["tooltip_body_parts"] = []
                    current_ingredient["tooltip_links"] = []
                    title_attr = (
                        attr_map.get("data-title")
                        or attr_map.get("title")
                        or attr_map.get("data-tooltip-title")
                    )
                    if title_attr:
                        current_ingredient["tooltip_title_parts"].append(title_attr)
                else:
                    current_ingredient["tooltip_depth"] += 1
            elif current_ingredient["tooltip_depth"] > 0:
                current_ingredient["tooltip_depth"] += 1

            if current_ingredient["tooltip_depth"] > 0 and (
                self._class_matches(class_set, "tooltip__title")
                or attr_map.get("data-role") == "title"
                or tag in {"strong", "b", "h1", "h2", "h3", "h4", "h5", "h6"}
            ):
                current_ingredient["tooltip_title_stack"].append(tag)

            if current_ingredient["tooltip_depth"] > 0 and tag == "a":
                href = attr_map.get("href") or ""
                if href:
                    absolute = urllib.parse.urljoin(self.base_url + "/", href.lstrip("/"))
                    current_ingredient["tooltip_links"].append(absolute)
                current_ingredient["tooltip_anchor_depth"] = current_ingredient.get("tooltip_anchor_depth", 0) + 1

        if self._function_context_stack and tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._function_context_stack[-1]["title_stack"].append(tag)

        if self._highlight_context_stack and tag in {"h1", "h2", "h3", "h4", "h5", "h6", "strong"}:
            self._highlight_context_stack[-1]["title_stack"].append(tag)

        if self._function_context_stack and tag == "li":
            context = self._function_context_stack[-1]
            context["item_depth"] += 1
            if context["item_depth"] == 1:
                context["current_item"] = []

        if self._highlight_context_stack and tag == "li":
            context = self._highlight_context_stack[-1]
            context["item_depth"] += 1
            if context["item_depth"] == 1:
                context["current_item"] = []

        if tag == "a":
            href = attr_map.get("href") or ""
            self._link_href_stack.append(href)
            if href.startswith("/ingredients/"):
                self._current_link_type = "ingredient"
                self._current_link_href = href
                self._link_text_buffer = []
                self._current_ingredient_href = href
                self._current_ingredient_data_name = attr_map.get("data-name") or attr_map.get("title")
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
        elif tag == "a":
            text = " ".join(part.strip() for part in self._link_text_buffer if part.strip())
            href_raw: Optional[str] = None
            if self._link_href_stack:
                href_raw = self._link_href_stack.pop()
            elif self._current_link_href:
                href_raw = self._current_link_href
            absolute_href = (
                urllib.parse.urljoin(self.base_url + "/", href_raw.lstrip("/"))
                if href_raw
                else None
            )
            if self._current_link_type == "ingredient" and self._current_link_href:
                full_url = urllib.parse.urljoin(self.base_url + "/", self._current_link_href.lstrip("/"))
                key = self._ingredient_key(full_url, text)
                if key not in self.ingredients:
                    self.ingredients[key] = Ingredient(
                        name=text or self._current_link_href.split("/")[-1].replace("-", " ").title(),
                        url=full_url,
                    )
                elif text:
                    self.ingredients[key].name = text
                if self._ingredient_context_stack:
                    current = self._ingredient_context_stack[-1]
                    if not current.get("key"):
                        current["key"] = key
                    if text and not current.get("name_hint"):
                        current["name_hint"] = text
                    if full_url and not current.get("url_hint"):
                        current["url_hint"] = full_url
            elif self._current_link_type == "brand" and text and self._current_link_href:
                full_url = urllib.parse.urljoin(self.base_url + "/", self._current_link_href.lstrip("/"))
                self._brand_links.add(full_url)
                # store human readable brand name for later heuristics
                self._meta.setdefault("brand:text", text)
            if absolute_href and self._function_context_stack:
                self._function_context_stack[-1]["links"].append(absolute_href)
            if absolute_href and self._highlight_context_stack:
                self._highlight_context_stack[-1]["links"].append(absolute_href)
            self._current_link_type = None
            self._current_link_href = None
            self._link_text_buffer = []
            self._current_ingredient_href = None
            self._current_ingredient_data_name = None
        if self._materials_depth > 0:
            if tag == "li" and self._current_material_item_depth > 0:
                self._current_material_item_depth -= 1
                if self._current_material_item_depth == 0:
                    text = self._normalize_text(self._current_material_text)
                    if text:
                        self.material_entries.append(text)
                    self._current_material_text = []
            self._materials_depth -= 1
            if self._materials_depth < 0:
                self._materials_depth = 0
        if self._description_depth > 0:
            self._description_depth -= 1
            if self._description_depth == 0:
                text = self._normalize_text(self._description_buffer)
                if text:
                    self._detail_description = text
                self._description_buffer = []
        if self._function_context_stack:
            context = self._function_context_stack[-1]
            if tag == "li" and context["item_depth"] > 0:
                context["item_depth"] -= 1
                if context["item_depth"] == 0:
                    text = self._normalize_text(context.get("current_item", []))
                    if text:
                        context["items"].append(text)
                    context["current_item"] = []
            if context["title_stack"] and context["title_stack"][-1] == tag:
                context["title_stack"].pop()
            context["depth"] -= 1
            if context["depth"] == 0:
                title = self._normalize_text(context["title_buffer"])
                body = self._normalize_text(context["body_buffer"])
                entry: Dict[str, Any] = {}
                name = context.get("name") or title
                if name:
                    entry["name"] = name
                if context["items"]:
                    entry["items"] = context["items"]
                if body:
                    entry["text"] = body
                if context["links"]:
                    entry["links"] = sorted({link for link in context["links"] if link})
                if entry:
                    self.function_entries.append(entry)
                self._function_context_stack.pop()
        if self._highlight_context_stack:
            context = self._highlight_context_stack[-1]
            if tag == "li" and context["item_depth"] > 0:
                context["item_depth"] -= 1
                if context["item_depth"] == 0:
                    text = self._normalize_text(context.get("current_item", []))
                    if text:
                        context["items"].append(text)
                    context["current_item"] = []
            if context["title_stack"] and context["title_stack"][-1] == tag:
                context["title_stack"].pop()
            context["depth"] -= 1
            if context["depth"] == 0:
                title = self._normalize_text(context["title_buffer"])
                body = self._normalize_text(context["body_buffer"])
                entry: Dict[str, Any] = {}
                if title:
                    entry["title"] = title
                if body:
                    entry["text"] = body
                if context["items"]:
                    entry["items"] = context["items"]
                if context["links"]:
                    entry["links"] = sorted({link for link in context["links"] if link})
                if entry:
                    self.highlight_entries.append(entry)
                self._highlight_context_stack.pop()
        if self._ingredient_context_stack:
            context = self._ingredient_context_stack[-1]
            if context["tooltip_depth"] > 0:
                if tag == "a" and context.get("tooltip_anchor_depth", 0) > 0:
                    context["tooltip_anchor_depth"] -= 1
                if context["tooltip_title_stack"] and context["tooltip_title_stack"][-1] == tag:
                    context["tooltip_title_stack"].pop()
                context["tooltip_depth"] -= 1
                if context["tooltip_depth"] == 0:
                    title = self._normalize_text(context.get("tooltip_title_parts", []))
                    body = self._normalize_text(context.get("tooltip_body_parts", []))
                    links = sorted({link for link in context.get("tooltip_links", []) if link})
                    extra: Dict[str, Any] = {}
                    if title:
                        extra["tooltip_title"] = title
                    if body:
                        extra["tooltip_text"] = body
                    if context.get("data_tooltip_text"):
                        extra.setdefault("tooltip_text", context["data_tooltip_text"].strip())
                    if context.get("data_tooltip_link"):
                        absolute = urllib.parse.urljoin(
                            self.base_url + "/", context["data_tooltip_link"].lstrip("/")
                        )
                        links.append(absolute)
                    if links:
                        extra["tooltip_links"] = sorted({link for link in links if link})
                    key = context.get("key")
                    self._register_ingredient_extra(
                        key,
                        extra,
                        name_hint=context.get("name_hint"),
                        url_hint=context.get("url_hint"),
                    )
                    context["tooltip_body_parts"] = []
                    context["tooltip_title_parts"] = []
                    context["tooltip_links"] = []
                    context["tooltip_anchor_depth"] = 0
            context["depth"] -= 1
            if context["depth"] <= 0:
                key = context.get("key")
                extra: Dict[str, Any] = {}
                if context.get("data_tooltip_text"):
                    extra["tooltip_text"] = context["data_tooltip_text"].strip()
                if context.get("data_tooltip_link"):
                    absolute = urllib.parse.urljoin(
                        self.base_url + "/", context["data_tooltip_link"].lstrip("/")
                    )
                    extra.setdefault("tooltip_links", []).append(absolute)
                self._register_ingredient_extra(
                    key,
                    extra,
                    name_hint=context.get("name_hint"),
                    url_hint=context.get("url_hint"),
                )
                self._ingredient_context_stack.pop()
        if self._detailpage_depth > 0:
            self._detailpage_depth -= 1
            if self._detailpage_depth < 0:
                self._detailpage_depth = 0

    def handle_data(self, data: str) -> None:
        if self._in_json_ld:
            self._json_buffer.append(data)
        if self._in_h1:
            self._current_text.append(data)
        if self._current_link_type:
            self._link_text_buffer.append(data)
        if self._description_depth > 0:
            self._description_buffer.append(data)
        if self._current_material_item_depth > 0:
            self._current_material_text.append(data)
        if self._function_context_stack:
            context = self._function_context_stack[-1]
            if context["title_stack"]:
                context["title_buffer"].append(data)
            elif context["item_depth"] > 0:
                context.setdefault("current_item", []).append(data)
            else:
                context["body_buffer"].append(data)
        if self._highlight_context_stack:
            context = self._highlight_context_stack[-1]
            if context["title_stack"]:
                context["title_buffer"].append(data)
            elif context["item_depth"] > 0:
                context.setdefault("current_item", []).append(data)
            else:
                context["body_buffer"].append(data)
        if self._ingredient_context_stack:
            context = self._ingredient_context_stack[-1]
            if context["tooltip_depth"] > 0:
                anchor_depth = context.get("tooltip_anchor_depth", 0)
                if anchor_depth == 0:
                    if context["tooltip_title_stack"]:
                        context.setdefault("tooltip_title_parts", []).append(data)
                    else:
                        context.setdefault("tooltip_body_parts", []).append(data)

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
            product.description = (
                self._detail_description
                or self._meta.get("og:description")
                or self._meta.get("description")
            )
        elif self._detail_description:
            product.description = self._detail_description
        product.materials = list(self.material_entries)
        product.functions = [copy.deepcopy(entry) for entry in self.function_entries]
        product.highlights = [copy.deepcopy(entry) for entry in self.highlight_entries]
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
                merged = Ingredient(
                    name=ingredient.name,
                    url=ingredient.url,
                    extra=dict(ingredient.extra),
                )
                target_index = len(deduped_list)
                deduped_list.append(merged)
            else:
                merged = deduped_list[target_index]
                if ingredient.name and not merged.name:
                    merged.name = ingredient.name
                if ingredient.url and not merged.url:
                    merged.url = ingredient.url
                if ingredient.extra:
                    merged.extra.update({k: v for k, v in ingredient.extra.items() if v is not None})
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

    @staticmethod
    def _class_matches(class_set: Set[str], prefix: str) -> bool:
        if not class_set:
            return False
        for token in class_set:
            if token == prefix or token.startswith(prefix + "--") or token.startswith(prefix + "__"):
                return True
        return False

    @staticmethod
    def _normalize_text(chunks: List[str]) -> str:
        text = " ".join(chunk.strip() for chunk in chunks if chunk and chunk.strip())
        return " ".join(text.split())

    def _register_ingredient_extra(
        self,
        key: Optional[str],
        extra: Dict[str, Any],
        *,
        name_hint: Optional[str] = None,
        url_hint: Optional[str] = None,
    ) -> None:
        if not extra:
            return
        sanitized = {
            field: value
            for field, value in extra.items()
            if value not in (None, "", [])
        }
        if not sanitized:
            return
        target_key = key or self._ingredient_key(url_hint or "", name_hint)
        if not target_key:
            return
        ingredient = self.ingredients.get(target_key)
        if ingredient is None:
            ingredient = Ingredient(
                name=name_hint or "",
                url=url_hint or "",
                extra=dict(sanitized),
            )
            self.ingredients[target_key] = ingredient
        else:
            if name_hint and not ingredient.name:
                ingredient.name = name_hint
            if url_hint and not ingredient.url:
                ingredient.url = url_hint
            ingredient.extra.update(sanitized)


class IncidecoderScraper:
    """Coordinate product discovery, scraping and persistence."""

    def __init__(
        self,
        http_client: Optional[HttpClient] = None,
        *,
        base_url: str = "https://incidecoder.com",
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.http = http_client or HttpClient(base_url=self.base_url)

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
        letter_tokens = list(string.ascii_lowercase) + ["0-9", "other"]
        visited_brands: Set[str] = set()
        for token in letter_tokens:
            page = 1
            while True:
                params = {"letter": token}
                if page > 1:
                    params["page"] = str(page)
                query = urllib.parse.urlencode(params)
                path = f"/brands?{query}" if query else "/brands"
                try:
                    html = self.http.fetch(path)
                except Exception as exc:  # pragma: no cover - network behaviour
                    LOGGER.warning("Failed to fetch brand index %s: %s", path, exc)
                    break
                collector = LinkCollector(("/brands/", "/brand/"))
                collector.feed(html)
                brand_links = [self.http.build_url(link) for link in collector.links]
                brand_links = [link for link in brand_links if link not in visited_brands]
                if not brand_links:
                    break
                for brand_link in brand_links:
                    visited_brands.add(brand_link)
                    yield from self._discover_products_for_brand(brand_link)
                page += 1

    def _discover_products_for_brand(self, brand_url: str) -> Generator[str, None, None]:
        seen_pages: Set[str] = set()
        queue = [brand_url]
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
            pagination_collector = LinkCollector(("?page=",))
            pagination_collector.feed(html)
            for link in pagination_collector.links:
                absolute = urllib.parse.urljoin(current_url + "", link)
                if absolute not in seen_pages:
                    queue.append(absolute)

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
