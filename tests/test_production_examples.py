"""Production-ready examples showcasing MPREG's real-world applications.

These examples demonstrate practical usage patterns that highlight MPREG's unique
value proposition in distributed computing environments.
"""

import asyncio
import socket
import time

import pytest

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.config import MPREGSettings
from mpreg.core.model import RPCCommand
from mpreg.server import MPREGServer
from tests.test_helpers import TestPortManager, wait_for_condition


@pytest.fixture
async def production_cluster():
    """Create a production-like cluster with realistic specializations."""
    servers = []
    port_manager = TestPortManager()

    try:
        # Get ports for all services
        frontend_port = port_manager.get_server_port()
        auth_port = port_manager.get_server_port()
        payment_port = port_manager.get_server_port()
        inventory_port = port_manager.get_server_port()
        analytics_port = port_manager.get_server_port()

        # Frontend Load Balancer
        frontend = MPREGServer(
            MPREGSettings(
                port=frontend_port,
                name="Frontend-LB",
                resources={"web", "api"},
            )
        )

        # Microservice Cluster
        auth_service = MPREGServer(
            MPREGSettings(
                port=auth_port,
                name="Auth-Service",
                resources={"auth", "users", "sessions"},
                peers=[f"ws://127.0.0.1:{frontend_port}"],
            )
        )

        payment_service = MPREGServer(
            MPREGSettings(
                port=payment_port,
                name="Payment-Service",
                resources={"payments", "billing", "transactions"},
                peers=[f"ws://127.0.0.1:{frontend_port}"],
            )
        )

        inventory_service = MPREGServer(
            MPREGSettings(
                port=inventory_port,
                name="Inventory-Service",
                resources={"inventory", "products", "stock"},
                peers=[f"ws://127.0.0.1:{frontend_port}"],
            )
        )

        analytics_service = MPREGServer(
            MPREGSettings(
                port=analytics_port,
                name="Analytics-Service",
                resources={"analytics", "metrics", "reporting"},
                peers=[f"ws://127.0.0.1:{frontend_port}"],
            )
        )

        servers = [
            frontend,
            auth_service,
            payment_service,
            inventory_service,
            analytics_service,
        ]

        # Initialize tasks list for cleanup
        tasks = []

        # Register business logic functions
        await _register_business_functions(servers)

        # Start servers
        for server in servers:
            task = asyncio.create_task(server.server())
            tasks.append(task)
            await asyncio.sleep(0.1)

        async def wait_for_port(port: int) -> None:
            def port_open() -> bool:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(0.2)
                    return sock.connect_ex(("127.0.0.1", port)) == 0

            await wait_for_condition(
                port_open,
                timeout=10.0,
                interval=0.1,
                error_message=f"Server port {port} did not open in time",
            )

        await asyncio.gather(
            *(wait_for_port(server.settings.port) for server in servers)
        )
        await asyncio.sleep(1.0)  # Cluster formation time

        yield servers

    finally:
        # Proper cleanup
        for server in servers:
            try:
                await server.shutdown_async()
            except Exception as e:
                print(f"Error shutting down server: {e}")

        # Cancel all tasks
        for task in tasks:
            if not task.done():
                task.cancel()

        # Wait for tasks to complete with timeout
        if tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True), timeout=2.0
                )
            except TimeoutError:
                print("Some server tasks did not complete within timeout")

        await asyncio.sleep(0.5)

        # Cleanup port allocations
        port_manager.cleanup()


async def _register_business_functions(servers):
    """Register realistic business functions."""

    # Frontend functions
    def validate_request(request_data: dict) -> dict:
        return {
            "valid": True,
            "request_id": request_data.get("id", "unknown"),
            "validated_at": time.time(),
            "frontend_node": "Frontend-LB",
        }

    def format_response(data: dict) -> dict:
        return {
            "formatted_data": data,
            "response_format": "JSON",
            "compressed": False,
            "content_type": "application/json",
        }

    servers[0].register_command("validate_request", validate_request, ["web", "api"])
    servers[0].register_command("format_response", format_response, ["web", "api"])

    # Auth Service functions
    def authenticate_user(username: str, password: str) -> dict:
        # Simulate authentication - accept any username with correct password
        valid = password == "secret"
        return {
            "authenticated": valid,
            "user_id": f"user_{hash(username) % 1000}" if valid else None,
            "session_token": f"token_{time.time()}" if valid else None,
            "expires_in": 3600 if valid else 0,
            "auth_service": "Auth-Service",
        }

    def validate_session(session_token: str) -> dict:
        return {
            "valid": session_token.startswith("token_"),
            "user_id": "user_123" if session_token.startswith("token_") else None,
            "remaining_ttl": 3500,
            "auth_service": "Auth-Service",
        }

    servers[1].register_command("authenticate", authenticate_user, ["auth", "users"])
    servers[1].register_command(
        "validate_session", validate_session, ["auth", "sessions"]
    )

    # Payment Service functions
    def process_payment(user_id, amount: float, payment_method: str) -> dict:
        # Handle both direct string user_id and resolved auth result objects
        if isinstance(user_id, dict) and "user_id" in user_id:
            # Resolved auth result object
            actual_user_id = user_id["user_id"]
        elif isinstance(user_id, str):
            # Direct string user_id
            actual_user_id = user_id
        else:
            # Fallback for unexpected types
            actual_user_id = str(user_id)

        return {
            "payment_id": f"pay_{hash(actual_user_id + str(amount)) % 10000}",
            "status": "completed",
            "amount": amount,
            "currency": "USD",
            "payment_method": payment_method,
            "transaction_time": time.time(),
            "payment_service": "Payment-Service",
        }

    def refund_payment(payment_id: str, amount: float) -> dict:
        return {
            "refund_id": f"ref_{hash(payment_id) % 10000}",
            "original_payment": payment_id,
            "refund_amount": amount,
            "status": "processed",
            "refund_time": time.time(),
        }

    servers[2].register_command(
        "process_payment", process_payment, ["payments", "transactions"]
    )
    servers[2].register_command(
        "refund_payment", refund_payment, ["payments", "billing"]
    )

    # Inventory Service functions
    def check_inventory(product_id: str, quantity: int) -> dict:
        available = quantity <= 100  # Simulate inventory check
        return {
            "product_id": product_id,
            "requested_quantity": quantity,
            "available_quantity": 100 if available else 50,
            "in_stock": available,
            "inventory_service": "Inventory-Service",
        }

    def reserve_inventory(product_id: str, quantity: int) -> dict:
        return {
            "reservation_id": f"res_{hash(product_id + str(quantity)) % 10000}",
            "product_id": product_id,
            "reserved_quantity": quantity,
            "expires_at": time.time() + 900,  # 15 minutes
            "status": "reserved",
        }

    def update_inventory(product_id: str, quantity_delta: int) -> dict:
        return {
            "product_id": product_id,
            "quantity_change": quantity_delta,
            "new_quantity": 100 + quantity_delta,
            "updated_at": time.time(),
            "inventory_service": "Inventory-Service",
        }

    servers[3].register_command(
        "check_inventory", check_inventory, ["inventory", "stock"]
    )
    servers[3].register_command(
        "reserve_inventory", reserve_inventory, ["inventory", "products"]
    )
    servers[3].register_command(
        "update_inventory", update_inventory, ["inventory", "stock"]
    )

    # Analytics Service functions
    def track_event(event_type, user_id, metadata: dict) -> dict:
        # Handle both direct string event_type and resolved dependency objects
        if isinstance(event_type, dict):
            # Resolved dependency object - use a generic event type
            actual_event_type = "dependency_resolved_event"
        elif isinstance(event_type, str):
            # Direct string event_type
            actual_event_type = event_type
        else:
            # Fallback for unexpected types
            actual_event_type = str(event_type)

        # Handle both direct string user_id and resolved auth result objects
        if isinstance(user_id, dict) and "user_id" in user_id:
            # Resolved auth result object
            actual_user_id = user_id["user_id"]
        elif isinstance(user_id, str):
            # Direct string user_id
            actual_user_id = user_id
        else:
            # Fallback for unexpected types
            actual_user_id = str(user_id)

        return {
            "event_id": f"evt_{hash(actual_event_type + actual_user_id) % 100000}",
            "event_type": actual_event_type,
            "user_id": actual_user_id,
            "metadata": metadata,
            "timestamp": time.time(),
            "analytics_service": "Analytics-Service",
        }

    def generate_report(report_type: str, date_range: str) -> dict:
        return {
            "report_id": f"rpt_{hash(report_type + date_range) % 10000}",
            "report_type": report_type,
            "date_range": date_range,
            "total_events": 12543,
            "unique_users": 1890,
            "generated_at": time.time(),
            "analytics_service": "Analytics-Service",
        }

    servers[4].register_command("track_event", track_event, ["analytics", "metrics"])
    servers[4].register_command(
        "generate_report", generate_report, ["analytics", "reporting"]
    )


class TestECommerceWorkflows:
    """Test complete e-commerce workflows across microservices."""

    async def test_complete_purchase_flow(self, production_cluster):
        """Test a complete purchase workflow spanning multiple services."""
        servers = production_cluster

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{production_cluster[0].settings.port}"
        ) as client:
            # Complete purchase workflow
            purchase_workflow = await client._client.request(
                [
                    # Step 1: Validate incoming request
                    RPCCommand(
                        name="request_validation",
                        fun="validate_request",
                        args=(
                            {
                                "id": "req_001",
                                "user": "admin",
                                "product": "laptop",
                                "qty": 2,
                            },
                        ),
                        locs=frozenset(["web", "api"]),
                    ),
                    # Step 2: Authenticate user
                    RPCCommand(
                        name="auth_result",
                        fun="authenticate",
                        args=("admin", "secret"),
                        locs=frozenset(["auth", "users"]),
                    ),
                    # Step 3: Check inventory availability
                    RPCCommand(
                        name="inventory_check",
                        fun="check_inventory",
                        args=("laptop", 2),
                        locs=frozenset(["inventory", "stock"]),
                    ),
                    # Step 4: Reserve inventory
                    RPCCommand(
                        name="inventory_reserved",
                        fun="reserve_inventory",
                        args=("laptop", 2),
                        locs=frozenset(["inventory", "products"]),
                    ),
                    # Step 5: Process payment
                    RPCCommand(
                        name="payment_result",
                        fun="process_payment",
                        args=("auth_result", 2999.99, "credit_card"),
                        locs=frozenset(["payments", "transactions"]),
                    ),
                    # Step 6: Update inventory
                    RPCCommand(
                        name="inventory_updated",
                        fun="update_inventory",
                        args=("laptop", -2),
                        locs=frozenset(["inventory", "stock"]),
                    ),
                    # Step 7: Track purchase event
                    RPCCommand(
                        name="event_tracked",
                        fun="track_event",
                        args=(
                            "purchase",
                            "auth_result",
                            {"product": "laptop", "amount": 2999.99},
                        ),
                        locs=frozenset(["analytics", "metrics"]),
                    ),
                    # Step 8: Format final response
                    RPCCommand(
                        name="final_response",
                        fun="format_response",
                        args=(
                            {
                                "purchase_complete": True,
                                "payment": "payment_result",
                                "tracking": "event_tracked",
                            },
                        ),
                        locs=frozenset(["web", "api"]),
                    ),
                ]
            )

            # Verify the complete workflow
            assert "final_response" in purchase_workflow
            response = purchase_workflow["final_response"]
            assert response["response_format"] == "JSON"
            assert response["formatted_data"]["purchase_complete"] is True

    async def test_concurrent_user_sessions(self, production_cluster):
        """Test handling multiple concurrent user sessions."""
        servers = production_cluster

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{production_cluster[0].settings.port}"
        ) as client:
            # Simulate multiple users authenticating concurrently
            auth_tasks = []
            for i in range(10):
                username = f"user_{i}"
                password = "secret" if i % 2 == 0 else "wrong"  # Mix of valid/invalid
                task = client.call(
                    "authenticate",
                    username,
                    password,
                    locs=frozenset(["auth", "users"]),
                )
                auth_tasks.append(task)

            results = await asyncio.gather(*auth_tasks)

            # Verify results
            valid_auths = [r for r in results if r["authenticated"]]
            invalid_auths = [r for r in results if not r["authenticated"]]

            assert len(valid_auths) == 5  # Half should be valid
            assert len(invalid_auths) == 5  # Half should be invalid

            # All valid auths should have session tokens
            for auth in valid_auths:
                assert auth["session_token"] is not None
                assert auth["session_token"].startswith("token_")

    async def test_inventory_stress_testing(self, production_cluster):
        """Test inventory system under concurrent load."""
        servers = production_cluster

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{production_cluster[0].settings.port}"
        ) as client:
            # Multiple concurrent inventory operations
            inventory_tasks = []

            # Check inventory for different products
            for i in range(5):
                task = client.call(
                    "check_inventory",
                    f"product_{i}",
                    10,
                    locs=frozenset(["inventory", "stock"]),
                )
                inventory_tasks.append(task)

            # Reserve inventory for some products
            for i in range(3):
                task = client.call(
                    "reserve_inventory",
                    f"product_{i}",
                    5,
                    locs=frozenset(["inventory", "products"]),
                )
                inventory_tasks.append(task)

            results = await asyncio.gather(*inventory_tasks)

            # Verify all operations completed
            assert len(results) == 8

            # Check inventory results
            inventory_checks = results[:5]
            for check in inventory_checks:
                assert "in_stock" in check
                assert check["available_quantity"] > 0

            # Reservation results
            reservations = results[5:]
            for reservation in reservations:
                assert "reservation_id" in reservation
                assert reservation["status"] == "reserved"


class TestMicroserviceOrchestration:
    """Test sophisticated microservice orchestration patterns."""

    async def test_saga_pattern_implementation(self, production_cluster):
        """Test a saga pattern for distributed transactions."""
        servers = production_cluster

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{production_cluster[0].settings.port}"
        ) as client:
            # Saga pattern: Each step can be compensated if needed
            saga_workflow = await client._client.request(
                [
                    # Saga Step 1: Reserve inventory
                    RPCCommand(
                        name="saga_inventory",
                        fun="reserve_inventory",
                        args=("premium_item", 1),
                        locs=frozenset(["inventory", "products"]),
                    ),
                    # Saga Step 2: Authorize payment
                    RPCCommand(
                        name="saga_auth",
                        fun="authenticate",
                        args=("premium_user", "secret"),
                        locs=frozenset(["auth", "users"]),
                    ),
                    # Saga Step 3: Process payment (conditional on auth)
                    RPCCommand(
                        name="saga_payment",
                        fun="process_payment",
                        args=("saga_auth", 9999.99, "premium_card"),
                        locs=frozenset(["payments", "transactions"]),
                    ),
                    # Saga Step 4: Final inventory update
                    RPCCommand(
                        name="saga_complete",
                        fun="update_inventory",
                        args=("premium_item", -1),
                        locs=frozenset(["inventory", "stock"]),
                    ),
                    # Saga Step 5: Track completion
                    RPCCommand(
                        name="saga_tracking",
                        fun="track_event",
                        args=(
                            "saga_complete",
                            "saga_auth",
                            {"type": "premium_purchase"},
                        ),
                        locs=frozenset(["analytics", "metrics"]),
                    ),
                ]
            )

            # Verify saga completed successfully
            assert len(saga_workflow) > 0

            assert "saga_tracking" in saga_workflow
            assert (
                saga_workflow["saga_tracking"]["event_type"]
                == "dependency_resolved_event"
            )

            # Payment should have been processed
            assert "saga_payment" in saga_workflow
            assert saga_workflow["saga_payment"]["status"] == "completed"

    async def test_circuit_breaker_pattern(self, production_cluster):
        """Test circuit breaker-like behavior with timeouts."""
        servers = production_cluster

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{production_cluster[0].settings.port}"
        ) as client:
            # Test rapid successive calls with timeouts
            start_time = time.time()

            try:
                # This should succeed quickly
                result = await client.call(
                    "validate_request",
                    {"id": "fast_req"},
                    locs=frozenset(["web", "api"]),
                    timeout=2.0,
                )
                assert result["valid"] is True

                # Test with very short timeout (simulating circuit breaker)
                with pytest.raises(Exception):
                    await client.call(
                        "generate_report",
                        "slow_report",
                        "2024-01-01",
                        locs=frozenset(["analytics", "reporting"]),
                        timeout=0.001,
                    )  # Very short timeout

            finally:
                elapsed = time.time() - start_time
                # Should fail fast, not hang
                assert elapsed < 5.0

    async def test_event_driven_architecture(self, production_cluster):
        """Test event-driven patterns with analytics tracking."""
        servers = production_cluster

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{production_cluster[0].settings.port}"
        ) as client:
            # Simulate event-driven workflow
            events_workflow = await client._client.request(
                [
                    # Event 1: User login
                    RPCCommand(
                        name="login_event",
                        fun="track_event",
                        args=(
                            "user_login",
                            "user_123",
                            {"source": "web", "device": "desktop"},
                        ),
                        locs=frozenset(["analytics", "metrics"]),
                    ),
                    # Event 2: Page view
                    RPCCommand(
                        name="pageview_event",
                        fun="track_event",
                        args=(
                            "page_view",
                            "user_123",
                            {"page": "/products", "referrer": "/home"},
                        ),
                        locs=frozenset(["analytics", "metrics"]),
                    ),
                    # Event 3: Add to cart
                    RPCCommand(
                        name="cart_event",
                        fun="track_event",
                        args=(
                            "add_to_cart",
                            "user_123",
                            {"product": "widget", "price": 29.99},
                        ),
                        locs=frozenset(["analytics", "metrics"]),
                    ),
                    # Generate real-time analytics
                    RPCCommand(
                        name="analytics_report",
                        fun="generate_report",
                        args=("user_journey", "today"),
                        locs=frozenset(["analytics", "reporting"]),
                    ),
                ]
            )

            # Verify event tracking
            assert "analytics_report" in events_workflow
            report = events_workflow["analytics_report"]
            assert report["report_type"] == "user_journey"
            assert report["unique_users"] > 0


class TestHighThroughputScenarios:
    """Test high-throughput scenarios demonstrating MPREG's scalability."""

    async def test_bulk_operations_processing(self, production_cluster):
        """Test processing of bulk operations efficiently."""
        servers = production_cluster

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{production_cluster[0].settings.port}"
        ) as client:
            # Create a large batch of operations
            batch_size = 20

            # Process payment batch
            payment_tasks = []
            for i in range(batch_size):
                task = client.call(
                    "process_payment",
                    f"user_{i}",
                    99.99,
                    "credit_card",
                    locs=frozenset(["payments", "transactions"]),
                )
                payment_tasks.append(task)

            start_time = time.time()
            payment_results = await asyncio.gather(*payment_tasks)
            processing_time = time.time() - start_time

            # Verify all payments processed
            assert len(payment_results) == batch_size
            for result in payment_results:
                assert result["status"] == "completed"
                assert result["amount"] == 99.99

            # Should process efficiently (under 10 seconds for 20 operations)
            assert processing_time < 10.0

            print(f"Processed {batch_size} payments in {processing_time:.2f} seconds")
            print(f"Throughput: {batch_size / processing_time:.2f} payments/second")

    async def test_mixed_workload_performance(self, production_cluster):
        """Test performance with mixed workload types."""
        servers = production_cluster

        async with MPREGClientAPI(
            f"ws://127.0.0.1:{production_cluster[0].settings.port}"
        ) as client:
            # Mix of different operation types
            mixed_tasks = []

            # Authentication requests
            for i in range(5):
                task = client.call(
                    "authenticate",
                    f"user_{i}",
                    "secret",
                    locs=frozenset(["auth", "users"]),
                )
                mixed_tasks.append(task)

            # Payment processing
            for i in range(5):
                task = client.call(
                    "process_payment",
                    f"user_{i}",
                    149.99,
                    "debit_card",
                    locs=frozenset(["payments", "transactions"]),
                )
                mixed_tasks.append(task)

            # Inventory checks
            for i in range(5):
                task = client.call(
                    "check_inventory",
                    f"product_{i}",
                    1,
                    locs=frozenset(["inventory", "stock"]),
                )
                mixed_tasks.append(task)

            # Analytics tracking
            for i in range(5):
                task = client.call(
                    "track_event",
                    "mixed_test",
                    f"user_{i}",
                    {"test": True},
                    locs=frozenset(["analytics", "metrics"]),
                )
                mixed_tasks.append(task)

            start_time = time.time()
            results = await asyncio.gather(*mixed_tasks)
            total_time = time.time() - start_time

            # Verify all operations completed
            assert len(results) == 20

            # Should handle mixed workload efficiently
            assert total_time < 15.0

            print(
                f"Mixed workload of 20 operations completed in {total_time:.2f} seconds"
            )
            print(f"Average operation time: {total_time / 20:.3f} seconds")
