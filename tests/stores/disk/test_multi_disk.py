import json
import sqlite3
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from dirty_equals import IsDatetime
from inline_snapshot import snapshot
from typing_extensions import override

from key_value.aio.stores.disk.multi_store import MultiDiskStore
from key_value.aio.stores.disk.store import _sqlite_clear
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

TEST_SIZE_LIMIT = 100 * 1024  # 100KB


class TestMultiDiskStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, per_test_temp_dir: Path) -> AsyncGenerator[MultiDiskStore, None]:
        store = MultiDiskStore(base_directory=per_test_temp_dir, max_size=TEST_SIZE_LIMIT)

        yield store

        # Wipe the store after returning it (connections may already be closed by ctx manager)
        for conn in store._conn.values():
            try:
                _sqlite_clear(conn=conn)
            except sqlite3.ProgrammingError:
                pass

    async def test_value_stored(self, store: MultiDiskStore):
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})
        conn: sqlite3.Connection = store._conn["test"]

        row = conn.execute("SELECT value FROM kv WHERE key = ?", ("test_key",)).fetchone()
        assert row is not None
        value_as_dict = json.loads(row[0])
        assert value_as_dict == snapshot(
            {
                "collection": "test",
                "value": {"name": "Alice", "age": 30},
                "key": "test_key",
                "created_at": IsDatetime(iso_string=True),
                "version": 1,
            }
        )

        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30}, ttl=10)

        row = conn.execute("SELECT value FROM kv WHERE key = ?", ("test_key",)).fetchone()
        assert row is not None
        value_as_dict = json.loads(row[0])
        assert value_as_dict == snapshot(
            {
                "collection": "test",
                "created_at": IsDatetime(iso_string=True),
                "value": {"age": 30, "name": "Alice"},
                "key": "test_key",
                "expires_at": IsDatetime(iso_string=True),
                "version": 1,
            }
        )
