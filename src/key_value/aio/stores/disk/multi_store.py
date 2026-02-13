import sqlite3

from collections.abc import Callable
from datetime import timezone
from pathlib import Path
from typing import overload

from typing_extensions import override

from key_value.aio.stores.base import BaseContextManagerStore, BaseStore
from key_value.aio.stores.disk.store import (
    _DB_FILENAME,
    _create_connection,
    _sqlite_close,
    _sqlite_delete,
    _sqlite_evict,
    _sqlite_get_with_expire,
    _sqlite_set,
)
from key_value.shared.managed_entry import ManagedEntry, datetime
from key_value.shared.serialization import BasicSerializationAdapter

try:
    from pathvalidate import sanitize_filename
except ImportError as e:
    msg = "MultiDiskStore requires py-key-value-aio[disk]"
    raise ImportError(msg) from e

ConnectionFactory = Callable[[str], sqlite3.Connection]


def _sanitize_collection_for_filesystem(collection: str) -> str:
    """Sanitize the collection name so that it can be used as a directory name on the filesystem."""

    return sanitize_filename(filename=collection)


class MultiDiskStore(BaseContextManagerStore, BaseStore):
    """A disk-based store that uses SQLite for persistent storage. The MultiDiskStore creates
    one SQLite database per collection, each in its own subdirectory. A custom connection factory
    can be provided to control the creation of SQLite connections."""

    _conn: dict[str, sqlite3.Connection]

    _connection_factory: ConnectionFactory

    _base_directory: Path
    _max_size: int | None
    _auto_create: bool

    @overload
    def __init__(self, *, connection_factory: ConnectionFactory, default_collection: str | None = None, auto_create: bool = True) -> None:
        """Initialize a multi-disk store with a custom connection factory. The function will be called for each
        collection created by the caller with the collection name as the argument. Use this to tightly
        control the creation of the SQLite connections.

        Args:
            connection_factory: A factory function that creates a sqlite3 Connection for a given collection.
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to automatically create directories if they don't exist. Defaults to True.
        """

    @overload
    def __init__(
        self, *, base_directory: Path, max_size: int | None = None, default_collection: str | None = None, auto_create: bool = True
    ) -> None:
        """Initialize a multi-disk store that creates one SQLite database per collection.

        Args:
            base_directory: The directory to use for the databases.
            max_size: The maximum approximate data size per collection in bytes before eviction occurs.
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to automatically create directories if they don't exist. Defaults to True.
        """

    def __init__(
        self,
        *,
        connection_factory: ConnectionFactory | None = None,
        base_directory: Path | None = None,
        max_size: int | None = None,
        default_collection: str | None = None,
        auto_create: bool = True,
    ) -> None:
        """Initialize the multi-disk store.

        Args:
            connection_factory: A factory function that creates a sqlite3 Connection for a given collection.
            base_directory: The directory to use for the databases.
            max_size: The maximum approximate data size per collection in bytes before eviction occurs.
            default_collection: The default collection to use if no collection is provided.
            auto_create: Whether to automatically create directories if they don't exist. Defaults to True.
                When False, raises ValueError if a directory doesn't exist.
        """
        if connection_factory is None and base_directory is None:
            msg = "Either connection_factory or base_directory must be provided"
            raise ValueError(msg)

        if base_directory is None:
            base_directory = Path.cwd()

        self._base_directory = base_directory.resolve()
        self._auto_create = auto_create
        self._max_size = max_size

        def default_connection_factory(collection: str) -> sqlite3.Connection:
            """Create a default connection factory that creates a SQLite database for a given collection."""
            sanitized_collection: str = _sanitize_collection_for_filesystem(collection=collection)

            cache_directory: Path = self._base_directory / sanitized_collection

            if not cache_directory.exists():
                if not self._auto_create:
                    msg = f"Directory '{cache_directory}' does not exist. Either create the directory manually or set auto_create=True."
                    raise ValueError(msg)
                cache_directory.mkdir(parents=True, exist_ok=True)

            return _create_connection(cache_directory / _DB_FILENAME)

        self._connection_factory = connection_factory or default_connection_factory

        self._conn = {}

        self._serialization_adapter = BasicSerializationAdapter()

        super().__init__(
            default_collection=default_collection,
            stable_api=True,
        )

    @override
    async def _setup(self) -> None:
        """Register connection cleanup."""
        self._exit_stack.callback(self._sync_close)

    @override
    async def _setup_collection(self, *, collection: str) -> None:
        self._conn[collection] = self._connection_factory(collection)

    @override
    async def _get_managed_entry(self, *, key: str, collection: str) -> ManagedEntry | None:
        expire_epoch: float | None

        managed_entry_str, expire_epoch = _sqlite_get_with_expire(conn=self._conn[collection], key=key)

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
        _ = _sqlite_set(
            conn=self._conn[collection],
            key=key,
            value=self._serialization_adapter.dump_json(entry=managed_entry, key=key, collection=collection),
            expire=managed_entry.ttl,
        )

        if self._max_size is not None and self._max_size > 0:
            _sqlite_evict(conn=self._conn[collection], max_size=self._max_size)

    @override
    async def _delete_managed_entry(self, *, key: str, collection: str) -> bool:
        return _sqlite_delete(conn=self._conn[collection], key=key)

    def _sync_close(self) -> None:
        for conn in self._conn.values():
            _sqlite_close(conn=conn)

    def __del__(self) -> None:
        self._sync_close()
