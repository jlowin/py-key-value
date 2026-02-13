import sqlite3
import time

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, overload

from typing_extensions import override

from key_value.aio.stores.base import BaseContextManagerStore, BaseStore
from key_value.shared.compound import compound_key
from key_value.shared.managed_entry import ManagedEntry

_DB_FILENAME = "store.db"


def _create_connection(db_path: Path) -> sqlite3.Connection:
    """Create a SQLite connection with the kv table initialized.

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        A sqlite3.Connection instance.
    """
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS kv ("
        "key TEXT PRIMARY KEY, "
        "value TEXT NOT NULL, "
        "expire_at REAL"
        ")"
    )
    conn.commit()
    return conn


def _sqlite_get_with_expire(conn: sqlite3.Connection, key: str) -> tuple[Any, float | None]:
    """Get a value from the store with its expiration time.

    Args:
        conn: The sqlite3 connection.
        key: The key to retrieve.

    Returns:
        Tuple of (value, expire_time). Value is None if key doesn't exist.
    """
    row = conn.execute("SELECT value, expire_at FROM kv WHERE key = ?", (key,)).fetchone()
    if row is None:
        return (None, None)
    return (row[0], row[1])


def _sqlite_set(
    conn: sqlite3.Connection,
    key: str,
    value: str,
    *,
    expire: float | None = None,
) -> bool:
    """Set a value in the store.

    Args:
        conn: The sqlite3 connection.
        key: The key to set.
        value: The value to store.
        expire: Optional expiration time in seconds (TTL).

    Returns:
        True if successful.
    """
    expire_at: float | None = None
    if expire is not None:
        expire_at = time.time() + expire
    conn.execute(
        "INSERT OR REPLACE INTO kv (key, value, expire_at) VALUES (?, ?, ?)",
        (key, value, expire_at),
    )
    conn.commit()
    return True


def _sqlite_delete(conn: sqlite3.Connection, key: str) -> bool:
    """Delete a key from the store.

    Args:
        conn: The sqlite3 connection.
        key: The key to delete.

    Returns:
        True if the key was deleted, False if it didn't exist.
    """
    cursor = conn.execute("DELETE FROM kv WHERE key = ?", (key,))
    conn.commit()
    return cursor.rowcount > 0


def _sqlite_clear(conn: sqlite3.Connection) -> int:
    """Clear all items from the store.

    Args:
        conn: The sqlite3 connection.

    Returns:
        Number of items removed.
    """
    cursor = conn.execute("DELETE FROM kv")
    conn.commit()
    return cursor.rowcount


def _sqlite_close(conn: sqlite3.Connection) -> None:
    """Close the connection.

    Args:
        conn: The sqlite3 connection.
    """
    conn.close()


def _sqlite_evict(conn: sqlite3.Connection, max_size: int) -> None:
    """Evict entries if the total data size exceeds max_size.

    Expired entries are purged first. If still over the limit, oldest entries
    (by rowid / insertion order) are removed in batches.

    Args:
        conn: The sqlite3 connection.
        max_size: Maximum allowed total data size in bytes.
    """
    conn.execute("DELETE FROM kv WHERE expire_at IS NOT NULL AND expire_at < ?", (time.time(),))
    conn.commit()

    total_size: int = conn.execute("SELECT COALESCE(SUM(LENGTH(key) + LENGTH(value)), 0) FROM kv").fetchone()[0]

    while total_size > max_size:
        total_rows: int = conn.execute("SELECT COUNT(*) FROM kv").fetchone()[0]
        if total_rows == 0:
            break
        to_delete = max(1, total_rows // 10)
        conn.execute(
            "DELETE FROM kv WHERE rowid IN (SELECT rowid FROM kv ORDER BY rowid ASC LIMIT ?)",
            (to_delete,),
        )
        conn.commit()
        total_size = conn.execute("SELECT COALESCE(SUM(LENGTH(key) + LENGTH(value)), 0) FROM kv").fetchone()[0]


class DiskStore(BaseContextManagerStore, BaseStore):
    """A disk-based store that uses SQLite for persistent key-value storage."""

    _conn: sqlite3.Connection
    _max_size: int | None
    _auto_create: bool

    @overload
    def __init__(self, *, connection: sqlite3.Connection, default_collection: str | None = None, auto_create: bool = True) -> None:
        """Initialize the disk store with an existing SQLite connection.

        Args:
            connection: An existing sqlite3 Connection to use. The caller is responsible
                for managing the connection's lifecycle.
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to automatically create the directory if it doesn't exist. Defaults to True.
        """

    @overload
    def __init__(
        self, *, directory: Path | str, max_size: int | None = None, default_collection: str | None = None, auto_create: bool = True
    ) -> None:
        """Initialize the disk store.

        Args:
            directory: The directory to use for the disk store.
            max_size: The maximum approximate data size in bytes before eviction occurs.
                Defaults to an unlimited size disk store.
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to automatically create the directory if it doesn't exist. Defaults to True.
        """

    def __init__(
        self,
        *,
        connection: sqlite3.Connection | None = None,
        directory: Path | str | None = None,
        max_size: int | None = None,
        default_collection: str | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the disk store.

        Args:
            connection: An existing sqlite3 Connection to use. If provided, the store will
                not manage the connection's lifecycle (will not close it). The caller is responsible
                for managing the connection's lifecycle.
            directory: The directory to use for the disk store.
            max_size: The maximum approximate data size in bytes before eviction occurs.
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to automatically create the directory if it doesn't exist. Defaults to True.
                When False, raises ValueError if the directory doesn't exist.
        """
        if connection is not None and directory is not None:
            msg = "Provide only one of connection or directory"
            raise ValueError(msg)

        if connection is None and directory is None:
            msg = "Either connection or directory must be provided"
            raise ValueError(msg)

        client_provided = connection is not None
        self._client_provided_by_user = client_provided
        self._auto_create = auto_create
        self._max_size = max_size

        if connection:
            self._conn = connection
        elif directory:
            directory = Path(directory)

            if not directory.exists():
                if not self._auto_create:
                    msg = f"Directory '{directory}' does not exist. Either create the directory manually or set auto_create=True."
                    raise ValueError(msg)
                directory.mkdir(parents=True, exist_ok=True)

            self._conn = _create_connection(directory / _DB_FILENAME)

        super().__init__(
            default_collection=default_collection,
            client_provided_by_user=client_provided,
            stable_api=True,
        )

    @override
    async def _setup(self) -> None:
        """Register connection cleanup if we own the connection."""
        if not self._client_provided_by_user:
            self._exit_stack.callback(lambda: _sqlite_close(conn=self._conn))

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        combo_key: str = compound_key(collection=collection, key=key)

        expire_epoch: float | None

        managed_entry_str, expire_epoch = _sqlite_get_with_expire(conn=self._conn, key=combo_key)

        if not isinstance(managed_entry_str, str):
            return None

        managed_entry: ManagedEntry = self._serialization_adapter.load_json(json_str=managed_entry_str)

        if expire_epoch:
            managed_entry.expires_at = datetime.fromtimestamp(expire_epoch, tz=timezone.utc)

        return managed_entry

    @override
    async def _put_managed_entry(
        self,
        *,
        key: str,
        collection: str,
        managed_entry: ManagedEntry,
    ) -> None:
        combo_key: str = compound_key(collection=collection, key=key)

        _ = _sqlite_set(
            conn=self._conn,
            key=combo_key,
            value=self._serialization_adapter.dump_json(entry=managed_entry, key=key, collection=collection),
            expire=managed_entry.ttl,
        )

        if self._max_size is not None and self._max_size > 0:
            _sqlite_evict(conn=self._conn, max_size=self._max_size)

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        combo_key: str = compound_key(collection=collection, key=key)

        return _sqlite_delete(conn=self._conn, key=combo_key)

    def __del__(self) -> None:
        if not getattr(self, "_client_provided_by_user", False) and hasattr(self, "_conn"):
            _sqlite_close(conn=self._conn)
