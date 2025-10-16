# INCIDecoder Scraper

This repository contains a minimal yet extensible scraper that collects product information from [INCIDecoder](https://incidecoder.com). It discovers product URLs automatically, downloads the associated product pages, extracts structured information, and stores the results in a DuckDB database (with an automatic SQLite fallback when DuckDB is unavailable).

## Features

- **Product discovery strategies**
  - Sitemap-based discovery using the public XML sitemap hierarchy.
  - Brand catalogue discovery that paginates through the `/brands?offset=` listings to collect every brand and its products.
- **Structured parsing** via a custom HTML parser capable of extracting JSON-LD data, brand names, and ingredient links without relying on external dependencies.
- **Storage for large datasets** powered by DuckDB, making the collected data ready for analytical workloads. When DuckDB is not present the scraper automatically falls back to SQLite for portability.
- **CLI entry point** with throttling, resume support, and configurable discovery strategies.
- **Local asset capture** that optionally downloads product hero images into a directory and records the file path in the database alongside the remote URL.

## Usage

Create and activate a virtual environment (optional but recommended):

```bash
python -m venv .venv
source .venv/bin/activate
```

Ensure the `src/` directory is on `PYTHONPATH` (or install the package). For ad-hoc usage you can run:

```bash
export PYTHONPATH="$(pwd)/src"
```

Install DuckDB if you would like columnar storage (optional):

```bash
pip install duckdb
```

Run the scraper:

```bash
python -m incidecoder_scraper --database data/incidecoder.duckdb --strategy auto --throttle 1.5
```

Common options:

- `--strategy`: `auto` (default), `sitemap`, or `brands`.
- `--limit`: stop after scraping a specific number of products (useful for smoke tests).
- `--no-resume`: force re-scraping of products even if they already exist in the database.
- `--throttle`: control the delay between requests to avoid overwhelming the site.
- `--image-dir`: directory where downloaded product images should be stored (omit to skip downloads).

## Development

Run the unit test suite:

```bash
PYTHONPATH=src python -m unittest discover -s tests
```

Logging can be increased with `--log-level DEBUG` for troubleshooting.

## Scraped Product Fields

Each product page yields the following structured attributes before persistence:

- **Core metadata** – canonical URL, product name, brand, description, primary image URL, downloaded image path (when available), categories, and any rating statistics exposed via JSON-LD (average value and total review count).
- **Brand discovery hints** – raw brand links discovered on the page to support subsequent crawling heuristics.
- **Ingredient roster** – the ordered list of ingredient entries, with each ingredient capturing its display name, canonical INCIDecoder ingredient URL when available, and a placeholder for extra annotations extracted from the markup.

The scraper prefers structured JSON-LD data when available and falls back to Open Graph and on-page hints to populate missing fields, ensuring resilient coverage across product templates.

## Notes

- Network-facing parts of the scraper include retry and exponential back-off logic to behave politely with INCIDecoder's infrastructure.
- Ingredient information is normalised into a dedicated table, enabling scalable analytical queries over large datasets.
