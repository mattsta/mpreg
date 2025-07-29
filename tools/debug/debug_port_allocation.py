#!/usr/bin/env python3
"""
DEBUG: Test port allocation performance to find the hang
"""

import time

from tests.port_allocator import get_port_allocator


def test_port_allocation_performance():
    """Test how long it takes to allocate different numbers of ports."""

    allocator = get_port_allocator()

    # Test different allocation sizes
    for count in [5, 10, 20, 30, 50]:
        print(f"\n🔧 Testing allocation of {count} ports...")
        start_time = time.time()

        try:
            ports = allocator.allocate_port_range(count, "servers")
            end_time = time.time()
            duration = end_time - start_time

            print(f"✅ SUCCESS: Allocated {len(ports)} ports in {duration:.2f}s")
            print(f"   Ports: {ports[:5]}{'...' if len(ports) > 5 else ''}")

            # Release ports
            for port in ports:
                allocator.release_port(port)

        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            print(f"❌ FAILED: {e} (took {duration:.2f}s)")


if __name__ == "__main__":
    test_port_allocation_performance()
