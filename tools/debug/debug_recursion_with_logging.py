#!/usr/bin/env python3
"""
Deep debug test with comprehensive logging to track down recursion.
"""

import asyncio
import logging
import tempfile
from pathlib import Path

# Set up detailed logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S.%f",
)

# Enable debug logging for our module
logger = logging.getLogger("mpreg.datastructures.production_raft_implementation")
logger.setLevel(logging.DEBUG)

from tests.test_production_raft_integration import (
    MockNetwork,
    TestProductionRaftIntegration,
)


async def debug_with_comprehensive_logging():
    """Test with full debug logging to catch recursion."""
    print("🔍 COMPREHENSIVE DEBUG WITH LOGGING")
    print("=" * 50)

    test_instance = TestProductionRaftIntegration()

    with tempfile.TemporaryDirectory() as tmpdir:
        temp_dir = Path(tmpdir)
        network = MockNetwork()

        # Create minimal cluster
        nodes = test_instance.create_raft_cluster(
            3, temp_dir, network, storage_type="memory"
        )

        try:
            print("Starting nodes with full debug logging...")
            for node_id, node in nodes.items():
                print(f"Starting {node_id}")
                await node.start()

            print("Running for 3 seconds with debug logging...")
            await asyncio.sleep(3.0)

            print("Checking final states...")
            for node_id, node in nodes.items():
                print(f"{node_id}: {node.current_state.value}")

        except Exception as e:
            print(f"Exception during test: {e}")
            import traceback

            traceback.print_exc()
        finally:
            print("Stopping nodes...")
            for node_id, node in nodes.items():
                try:
                    print(f"Stopping {node_id}")
                    await asyncio.wait_for(node.stop(), timeout=5.0)
                    print(f"Stopped {node_id}")
                except TimeoutError:
                    print(f"Timeout stopping {node_id}")
                except Exception as e:
                    print(f"Error stopping {node_id}: {e}")


if __name__ == "__main__":
    asyncio.run(debug_with_comprehensive_logging())
