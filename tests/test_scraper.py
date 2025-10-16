import os
import tempfile
import unittest

from incidecoder_scraper.scraper import IncidecoderScraper, Product


class _StubHttpClient:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.requests = []

    def fetch_bytes(self, url: str) -> bytes:  # pragma: no cover - simple stub
        self.requests.append(url)
        return self.payload


class ScraperImageTests(unittest.TestCase):
    def test_download_product_image_writes_to_directory(self) -> None:
        payload = b"binary-data"
        http = _StubHttpClient(payload)
        with tempfile.TemporaryDirectory() as tmpdir:
            scraper = IncidecoderScraper(http_client=http, image_dir=tmpdir)
            product = Product(
                url="https://incidecoder.com/products/example",
                name="Example",
                image_url="https://cdn.example.com/path/to/image.png",
            )

            path = scraper._download_product_image(product)

            self.assertIsNotNone(path)
            assert path is not None
            self.assertTrue(path.endswith(".png"))
            self.assertTrue(os.path.exists(path))
            with open(path, "rb") as handle:
                self.assertEqual(handle.read(), payload)

            # Should reuse the existing file and avoid new downloads
            second_path = scraper._download_product_image(product)
            self.assertEqual(second_path, path)
            self.assertEqual(len(http.requests), 1)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
