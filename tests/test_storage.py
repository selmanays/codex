from incidecoder_scraper.storage import DataStore


def test_reset_brand_processing(tmp_path):
    db_path = tmp_path / "store.db"
    store = DataStore(str(db_path))
    try:
        store.add_brands([("Example", "https://example.com/brand/example")])
        brands = list(store.iter_pending_brands())
        assert len(brands) == 1
        brand_id, name, url = brands[0]
        assert name == "Example"
        assert url == "https://example.com/brand/example"

        store.mark_brand_processed(brand_id)
        assert list(store.iter_pending_brands()) == []

        store.reset_brand_processing()
        pending_again = list(store.iter_pending_brands())
        assert len(pending_again) == 1
        assert pending_again[0][0] == brand_id
    finally:
        store.close()
