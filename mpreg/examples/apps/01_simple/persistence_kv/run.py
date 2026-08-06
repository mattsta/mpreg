"""L1 persistence_kv — Memory + SQLite KeyValueStore put/get/list/TTL."""

from __future__ import annotations

import asyncio
import tempfile
import time
from pathlib import Path

from mpreg.core.persistence.config import PersistenceConfig, PersistenceMode
from mpreg.core.persistence.backend import (
    MemoryPersistenceBackend,
    SQLitePersistenceBackend,
)
from mpreg.core.persistence.kv_store import MemoryKeyValueStore
from mpreg.examples.apps._shared.runtime import app_run, ensure, ok, scenario, step

async def main() -> None:
    with app_run(
        "persistence_kv",
        "Persistence KV — memory + SQLite stores",
        level="L1",
    ):
        with scenario("memory put/get/list_prefix", "pers.sqlite_kv", "pers.memory_kv"):
            mem = MemoryKeyValueStore()
            await mem.put("cfg/a", b"one")
            await mem.put("cfg/b", b"two")
            await mem.put("other/x", b"z")
            ensure(await mem.get("cfg/a") == b"one", "get a")
            items = await mem.list_prefix("cfg/")
            keys = {k for k, _ in items}
            ensure(keys == {"cfg/a", "cfg/b"}, f"prefix {keys}")
            ok(f"memory keys={sorted(keys)}")

        with scenario("memory TTL expiry", "pers.memory_kv"):
            mem = MemoryKeyValueStore()
            await mem.put("tmp", b"soon", expires_at=time.time() - 1)
            ensure(await mem.get("tmp") is None, "expired should miss")
            await mem.put("live", b"ok", expires_at=time.time() + 60)
            ensure(await mem.get("live") == b"ok", "live miss")
            ok("TTL expiry ok")

        with scenario("memory delete + close", "pers.memory_kv"):
            mem = MemoryKeyValueStore()
            await mem.put("d", b"1")
            await mem.delete("d")
            ensure(await mem.get("d") is None, "delete failed")
            await mem.close()
            ok("delete + close")

        with scenario(
            "SQLite backend Path and str coerce",
            "pers.sqlite_kv",
            "pers.restart",
        ):
            with tempfile.TemporaryDirectory() as td:
                path = Path(td) / "kv.db"
                backend = SQLitePersistenceBackend(db_path=path)
                await backend.open()
                try:
                    store = backend.key_value_store("demo")
                    await store.put("user/1", b'{"n":1}')
                    got = await store.get("user/1")
                    ensure(got == b'{"n":1}', f"sqlite get {got!r}")
                    listed = await store.list_prefix("user/")
                    ensure(len(listed) >= 1, f"list {listed}")
                    ok(f"sqlite path={path.name} bytes={got!r}")
                finally:
                    await backend.close()

                # Phase G F18 fix: str paths coerce to Path
                path2 = Path(td) / "kv2.db"
                backend2 = SQLitePersistenceBackend(db_path=str(path2))
                ensure(isinstance(backend2.db_path, Path), type(backend2.db_path))
                await backend2.open()
                try:
                    store2 = backend2.key_value_store("demo")
                    await store2.put("k", b"v")
                    ensure(await store2.get("k") == b"v", "str path get")
                    step("F18 fixed: db_path accepts str → Path coerce")
                    ok(f"sqlite str coerce path={path2.name}")
                finally:
                    await backend2.close()

        with scenario("memory persistence backend façade", "pers.memory_kv"):
            be = MemoryPersistenceBackend()
            await be.open()
            try:
                kv = be.key_value_store("ns1")
                await kv.put("k", b"v")
                ensure(await kv.get("k") == b"v", "backend kv")
                names = await be.list_queue_names("ns1")
                ensure(isinstance(names, list), "queue names type")
                ok(f"memory backend queues={names}")
            finally:
                await be.close()

        with scenario(
            "PersistenceConfig modes + honest non-claims",
            "pers.mode",
            "pers.memory_kv",
            "pers.sqlite_kv",
        ):
            mem_cfg = PersistenceConfig(mode=PersistenceMode.MEMORY)
            ensure(mem_cfg.mode == PersistenceMode.MEMORY, "mem mode")
            ensure(mem_cfg.mode.value == "memory", f"mem value {mem_cfg.mode}")
            sql_cfg = PersistenceConfig(
                mode=PersistenceMode.SQLITE,
                data_dir=Path(tempfile.mkdtemp(prefix="mpreg-pers-cfg-")),
            )
            ensure(sql_cfg.mode == PersistenceMode.SQLITE, "sql mode")
            ensure(sql_cfg.sqlite_path().name.endswith(".sqlite"), str(sql_cfg.sqlite_path()))
            # Shipped modes only — remote SQL/other stores are plan-only (non-claim).
            shipped = {m.value for m in PersistenceMode}
            ensure(shipped == {"memory", "sqlite"}, f"unexpected modes {shipped}")
            step(
                "non-claim: remote SQL/other stores backends not in PersistenceMode — "
                "see docs/PERSISTENCE_FRAMEWORK_PLAN.md"
            )
            ok(f"PersistenceConfig modes={sorted(shipped)}")

if __name__ == "__main__":
    asyncio.run(main())
