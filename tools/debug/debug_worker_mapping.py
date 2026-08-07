#!/usr/bin/env python3
"""Debug worker offset calculations."""


def calculate_worker_offset(worker_id: str) -> int:
    """Debug version of the worker offset calculation."""
    if worker_id == "master":
        return 0

    # Extract number from worker ID (e.g., gw0 -> 0, gw1 -> 1)
    try:
        if worker_id.startswith("gw"):
            worker_num = int(worker_id[2:])
            # CRITICAL FIX: gw0 should not conflict with master
            # Start gw workers at offset 200 (master has 0-199)
            worker_num += 1  # gw0 becomes 1, gw1 becomes 2, etc.
        else:
            # Fallback: hash the worker ID
            worker_num = hash(worker_id) % 1000
            worker_num += 1  # Ensure non-zero for non-gw workers
    except ValueError, IndexError:
        # Fallback: hash the worker ID
        worker_num = hash(worker_id) % 1000
        worker_num += 1  # Ensure non-zero

    max_workers = 20  # 20 gw workers + 1 master = 21 total (within our capacity)
    ports_per_worker = 200

    # Map worker numbers to safe slots 1-20 (avoiding 0 which is master)
    # worker_num is 1-based for gw workers (gw0->1, gw1->2, etc.)
    safe_worker_num = ((worker_num - 1) % max_workers) + 1  # Map to 1-20
    return safe_worker_num * ports_per_worker


def main():
    workers = [
        "master",
        "gw0",
        "gw1",
        "gw5",
        "gw10",
        "gw11",
        "gw15",
        "gw20",
        "gw25",
        "gw29",
    ]

    print("Worker ID -> Raw Num -> Safe Num -> Offset")
    print("-" * 50)

    offsets_used = set()
    conflicts = []

    for worker_id in workers:
        if worker_id == "master":
            raw_num = 0
            safe_num = 0
            offset = 0
        else:
            gw_num = int(worker_id[2:])
            raw_num = gw_num + 1  # gw0->1, gw1->2, etc.
            safe_num = ((raw_num - 1) % 20) + 1  # Map to 1-20
            offset = safe_num * 200

        print(f"{worker_id:8} -> {raw_num:7} -> {safe_num:8} -> {offset:6}")

        if offset in offsets_used:
            conflicts.append((worker_id, offset))
        else:
            offsets_used.add(offset)

    print("\nCONFLICTS:")
    for worker_id, offset in conflicts:
        print(f"  {worker_id} conflicts at offset {offset}")

    if not conflicts:
        print("  No conflicts found!")


if __name__ == "__main__":
    main()
