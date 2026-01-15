from src.state_store import StateStore


def test_state_store_roundtrip(tmp_path):
    path = tmp_path / "last_run.json"
    store = StateStore(path)

    assert store.read_last_run() is None
    store.write_last_run("2025-01-01T00:00:00Z")
    assert store.read_last_run() == "2025-01-01T00:00:00Z"
