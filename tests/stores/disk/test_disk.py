import json
import sqlite3
from pathlib import Path

import pytest
from dirty_equals import IsDatetime
from inline_snapshot import snapshot
from typing_extensions import override

from key_value.aio.stores.disk import DiskStore
from key_value.aio.stores.disk.store import _sqlite_clear
from tests.stores.base import BaseStoreTests, ContextManagerStoreTestMixin

TEST_SIZE_LIMIT = 100 * 1024  # 100KB


class TestDiskStore(ContextManagerStoreTestMixin, BaseStoreTests):
    @override
    @pytest.fixture
    async def store(self, per_test_temp_dir: Path) -> DiskStore:
        disk_store = DiskStore(directory=per_test_temp_dir, max_size=TEST_SIZE_LIMIT)

        _sqlite_clear(conn=disk_store._conn)

        return disk_store

    @pytest.fixture
    async def conn(self, store: DiskStore) -> sqlite3.Connection:
        assert isinstance(store._conn, sqlite3.Connection)
        return store._conn

    async def test_value_stored(self, store: DiskStore, conn: sqlite3.Connection):
        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30})

        row = conn.execute("SELECT value FROM kv WHERE key = ?", ("test::test_key",)).fetchone()
        assert row is not None
        value_as_dict = json.loads(row[0])
        assert value_as_dict == snapshot(
            {
                "collection": "test",
                "created_at": IsDatetime(iso_string=True),
                "key": "test_key",
                "value": {"age": 30, "name": "Alice"},
                "version": 1,
            }
        )

        await store.put(collection="test", key="test_key", value={"name": "Alice", "age": 30}, ttl=10)

        row = conn.execute("SELECT value FROM kv WHERE key = ?", ("test::test_key",)).fetchone()
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
