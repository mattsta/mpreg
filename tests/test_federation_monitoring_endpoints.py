"""
Tests for Federation Monitoring Endpoints.

This test suite provides comprehensive coverage of the federation monitoring
HTTP endpoints, testing both functionality and integration with the unified
monitoring system.
"""

import asyncio

import aiohttp

from mpreg.core.config import MPREGSettings
from mpreg.core.monitoring.unified_monitoring import (
    MonitoringConfig,
    UnifiedSystemMonitor,
)
from mpreg.federation.federation_config import FederationConfig, FederationMode
from mpreg.federation.federation_connection_manager import FederationConnectionManager
from mpreg.federation.federation_monitoring_endpoints import (
    create_federation_monitoring_system,
)
from tests.conftest import AsyncTestContext


class TestFederationMonitoringEndpoints:
    """Test federation monitoring HTTP endpoints."""

    async def test_monitoring_system_creation_and_lifecycle(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test creating and managing the federation monitoring system."""
        port = server_cluster_ports[0]
        monitoring_port = server_cluster_ports[1]

        # Create test configuration
        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="MonitoringTest-Server",
            cluster_id="monitoring-test-cluster",
            resources={"monitor"},
            gossip_interval=1.0,
        )

        federation_config = FederationConfig(
            federation_mode=FederationMode.STRICT_ISOLATION,
            local_cluster_id=settings.cluster_id,
        )

        # Create federation manager (mock for testing)
        federation_manager = FederationConnectionManager(
            federation_config=federation_config
        )

        # Create unified monitor
        monitoring_config = MonitoringConfig()
        unified_monitor = UnifiedSystemMonitor(config=monitoring_config)
        test_context.tasks.append(asyncio.create_task(unified_monitor.start()))

        # Create monitoring system
        monitoring_system = create_federation_monitoring_system(
            settings=settings,
            federation_config=federation_config,
            federation_manager=federation_manager,
            unified_monitor=unified_monitor,
            monitoring_port=monitoring_port,
            enable_cors=True,
        )

        # Test lifecycle
        await monitoring_system.start()

        # Verify server is running
        base_url = f"http://{settings.host}:{monitoring_port}"

        async with aiohttp.ClientSession() as session:
            # Test basic connectivity
            async with session.get(f"{base_url}/") as response:
                assert response.status == 200
                data = await response.json()
                assert data["service"] == "MPREG Federation Monitoring"
                assert data["cluster_id"] == settings.cluster_id
                assert (
                    data["federation_mode"] == federation_config.federation_mode.value
                )

        await monitoring_system.stop()
        print("✅ Federation monitoring system lifecycle test passed")

    async def test_health_endpoints(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test federation health monitoring endpoints."""
        port = server_cluster_ports[0]
        monitoring_port = server_cluster_ports[1]

        # Setup monitoring system
        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="HealthTest-Server",
            cluster_id="health-test-cluster",
            resources={"health"},
            gossip_interval=1.0,
        )

        federation_config = FederationConfig(
            federation_mode=FederationMode.EXPLICIT_BRIDGING,
            local_cluster_id=settings.cluster_id,
        )

        federation_manager = FederationConnectionManager(
            federation_config=federation_config
        )

        monitoring_config = MonitoringConfig()
        unified_monitor = UnifiedSystemMonitor(config=monitoring_config)
        test_context.tasks.append(asyncio.create_task(unified_monitor.start()))

        monitoring_system = create_federation_monitoring_system(
            settings=settings,
            federation_config=federation_config,
            federation_manager=federation_manager,
            unified_monitor=unified_monitor,
            monitoring_port=monitoring_port,
        )

        await monitoring_system.start()
        base_url = f"http://{settings.host}:{monitoring_port}"

        try:
            async with aiohttp.ClientSession() as session:
                # Test /health endpoint
                async with session.get(f"{base_url}/health") as response:
                    assert response.status == 200
                    data = await response.json()

                    assert data["status"] == "ok"
                    assert "federation_health" in data

                    health = data["federation_health"]
                    assert "overall_status" in health
                    assert "health_score" in health
                    assert "total_clusters" in health
                    assert "connection_health" in health
                    assert "performance" in health

                    print(f"Health status: {health['overall_status']}")
                    print(f"Health score: {health['health_score']}")

                # Test /health/summary endpoint
                async with session.get(f"{base_url}/health/summary") as response:
                    assert response.status == 200
                    data = await response.json()

                    assert data["status"] == "ok"
                    assert "health_summary" in data

                    summary = data["health_summary"]
                    assert "overall" in summary
                    assert "cluster_counts" in summary
                    assert "connections" in summary
                    assert "cluster_details" in summary

                    print(f"Cluster counts: {summary['cluster_counts']}")

                # Test /health/clusters endpoint
                async with session.get(f"{base_url}/health/clusters") as response:
                    assert response.status == 200
                    data = await response.json()

                    assert data["status"] == "ok"
                    assert "clusters" in data
                    assert "total_clusters" in data

                    print(f"Total clusters: {data['total_clusters']}")

                # Test /health/clusters/{cluster_id} endpoint
                cluster_id = settings.cluster_id
                async with session.get(
                    f"{base_url}/health/clusters/{cluster_id}"
                ) as response:
                    assert response.status == 200
                    data = await response.json()

                    assert data["status"] == "ok"
                    assert "cluster" in data

                    cluster = data["cluster"]
                    assert cluster["cluster_id"] == cluster_id
                    assert "status" in cluster
                    assert "health_score" in cluster

                    print(f"Cluster {cluster_id} status: {cluster['status']}")

                # Test non-existent cluster
                async with session.get(
                    f"{base_url}/health/clusters/non-existent"
                ) as response:
                    assert response.status == 404
                    data = await response.json()
                    assert data["status"] == "error"

        finally:
            await monitoring_system.stop()

        print("✅ Federation health endpoints test passed")

    async def test_metrics_endpoints(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test federation metrics monitoring endpoints."""
        port = server_cluster_ports[0]
        monitoring_port = server_cluster_ports[1]

        # Setup monitoring system
        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="MetricsTest-Server",
            cluster_id="metrics-test-cluster",
            resources={"metrics"},
            gossip_interval=1.0,
        )

        federation_config = FederationConfig(
            federation_mode=FederationMode.PERMISSIVE_BRIDGING,
            local_cluster_id=settings.cluster_id,
        )

        federation_manager = FederationConnectionManager(
            federation_config=federation_config
        )

        monitoring_config = MonitoringConfig()
        unified_monitor = UnifiedSystemMonitor(config=monitoring_config)
        test_context.tasks.append(asyncio.create_task(unified_monitor.start()))

        monitoring_system = create_federation_monitoring_system(
            settings=settings,
            federation_config=federation_config,
            federation_manager=federation_manager,
            unified_monitor=unified_monitor,
            monitoring_port=monitoring_port,
        )

        await monitoring_system.start()
        base_url = f"http://{settings.host}:{monitoring_port}"

        try:
            async with aiohttp.ClientSession() as session:
                # Test /metrics endpoint
                async with session.get(f"{base_url}/metrics") as response:
                    assert response.status == 200
                    data = await response.json()

                    assert data["status"] == "ok"
                    assert "federation_metrics" in data
                    assert "collection_info" in data

                    metrics = data["federation_metrics"]
                    assert "federation_system" in metrics
                    assert "correlation_metrics" in metrics
                    assert "overall_health" in metrics

                    federation_system = metrics["federation_system"]
                    assert "requests_per_second" in federation_system
                    assert "average_latency_ms" in federation_system
                    assert "error_rate_percent" in federation_system

                    print(f"Federation RPS: {federation_system['requests_per_second']}")
                    print(
                        f"Average latency: {federation_system['average_latency_ms']}ms"
                    )

                # Test /metrics/performance endpoint
                async with session.get(f"{base_url}/metrics/performance") as response:
                    assert response.status == 200
                    data = await response.json()

                    assert data["status"] == "ok"
                    assert "performance" in data

                    performance = data["performance"]
                    assert "throughput" in performance
                    assert "latency" in performance
                    assert "reliability" in performance
                    assert "topology" in performance
                    assert "trend" in performance

                    print(f"Performance trend: {performance['trend']}")

                # Test /metrics/connections endpoint
                async with session.get(f"{base_url}/metrics/connections") as response:
                    assert response.status == 200
                    data = await response.json()

                    assert data["status"] == "ok"
                    assert "connections" in data

                    connections = data["connections"]
                    assert "total_connections" in connections
                    assert "active_connections" in connections
                    assert "connection_success_rate" in connections

                    print(f"Active connections: {connections['active_connections']}")

                # Test /metrics/timeseries endpoint
                params = {"metric": "health_score", "duration": "1", "resolution": "1"}
                async with session.get(
                    f"{base_url}/metrics/timeseries", params=params
                ) as response:
                    assert response.status == 200
                    data = await response.json()

                    assert data["status"] == "ok"
                    assert data["metric"] == "health_score"
                    assert data["duration_hours"] == 1
                    assert data["resolution_minutes"] == 1
                    assert "data" in data
                    assert "data_points" in data

                    print(f"Time series data points: {data['data_points']}")

                # Test invalid parameters
                invalid_params = {"metric": "health_score", "duration": "invalid"}
                async with session.get(
                    f"{base_url}/metrics/timeseries", params=invalid_params
                ) as response:
                    assert response.status == 400
                    data = await response.json()
                    assert data["status"] == "error"

        finally:
            await monitoring_system.stop()

        print("✅ Federation metrics endpoints test passed")

    async def test_topology_endpoints(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test federation topology monitoring endpoints."""
        port = server_cluster_ports[0]
        monitoring_port = server_cluster_ports[1]

        # Setup monitoring system
        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="TopologyTest-Server",
            cluster_id="topology-test-cluster",
            resources={"topology"},
            gossip_interval=1.0,
        )

        federation_config = FederationConfig(
            federation_mode=FederationMode.EXPLICIT_BRIDGING,
            local_cluster_id=settings.cluster_id,
        )

        federation_manager = FederationConnectionManager(
            federation_config=federation_config
        )

        monitoring_config = MonitoringConfig()
        unified_monitor = UnifiedSystemMonitor(config=monitoring_config)
        test_context.tasks.append(asyncio.create_task(unified_monitor.start()))

        monitoring_system = create_federation_monitoring_system(
            settings=settings,
            federation_config=federation_config,
            federation_manager=federation_manager,
            unified_monitor=unified_monitor,
            monitoring_port=monitoring_port,
        )

        await monitoring_system.start()
        base_url = f"http://{settings.host}:{monitoring_port}"

        try:
            async with aiohttp.ClientSession() as session:
                # Test /topology endpoint
                async with session.get(f"{base_url}/topology") as response:
                    assert response.status == 200
                    data = await response.json()

                    assert data["status"] == "ok"
                    assert "topology" in data

                    topology = data["topology"]
                    assert "summary" in topology
                    assert "nodes" in topology
                    assert "edges" in topology
                    assert "clusters" in topology
                    assert "snapshot_timestamp" in topology

                    summary = topology["summary"]
                    assert "total_nodes" in summary
                    assert "total_edges" in summary
                    assert "graph_diameter" in summary
                    assert "clustering_coefficient" in summary

                    print(
                        f"Topology: {summary['total_nodes']} nodes, {summary['total_edges']} edges"
                    )
                    print(f"Graph diameter: {summary['graph_diameter']}")
                    print(
                        f"Clustering coefficient: {summary['clustering_coefficient']}"
                    )

                    # Verify nodes structure
                    nodes = topology["nodes"]
                    assert isinstance(nodes, list)
                    if nodes:
                        node = nodes[0]
                        assert "node_id" in node
                        assert "cluster_id" in node
                        assert "health_score" in node
                        assert "status" in node

                    # Verify clusters structure
                    clusters = topology["clusters"]
                    assert isinstance(clusters, dict)
                    assert settings.cluster_id in clusters

                    cluster_info = clusters[settings.cluster_id]
                    assert "cluster_id" in cluster_info
                    assert "node_count" in cluster_info
                    assert "health_score" in cluster_info
                    assert "status" in cluster_info

        finally:
            await monitoring_system.stop()

        print("✅ Federation topology endpoints test passed")

    async def test_configuration_endpoints(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test federation configuration monitoring endpoints."""
        port = server_cluster_ports[0]
        monitoring_port = server_cluster_ports[1]

        # Setup monitoring system
        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="ConfigTest-Server",
            cluster_id="config-test-cluster",
            resources={"config"},
            gossip_interval=1.0,
        )

        federation_config = FederationConfig(
            federation_mode=FederationMode.STRICT_ISOLATION,
            local_cluster_id=settings.cluster_id,
        )

        federation_manager = FederationConnectionManager(
            federation_config=federation_config
        )

        monitoring_config = MonitoringConfig()
        unified_monitor = UnifiedSystemMonitor(config=monitoring_config)
        test_context.tasks.append(asyncio.create_task(unified_monitor.start()))

        monitoring_system = create_federation_monitoring_system(
            settings=settings,
            federation_config=federation_config,
            federation_manager=federation_manager,
            unified_monitor=unified_monitor,
            monitoring_port=monitoring_port,
        )

        await monitoring_system.start()
        base_url = f"http://{settings.host}:{monitoring_port}"

        try:
            async with aiohttp.ClientSession() as session:
                # Test /config endpoint
                async with session.get(f"{base_url}/config") as response:
                    assert response.status == 200
                    data = await response.json()

                    assert data["status"] == "ok"
                    assert "configuration" in data

                    config = data["configuration"]
                    assert "federation_mode" in config
                    assert "policies" in config
                    assert "version_info" in config
                    assert "validation" in config

                    assert (
                        config["federation_mode"]
                        == federation_config.federation_mode.value
                    )

                    policies = config["policies"]
                    assert "total" in policies
                    assert "active" in policies
                    assert "compliance_percent" in policies

                    version_info = config["version_info"]
                    assert "configuration_version" in version_info
                    assert "last_update" in version_info
                    assert "pending_changes" in version_info

                    validation = config["validation"]
                    assert "errors" in validation
                    assert isinstance(validation["errors"], list)

                    print(f"Federation mode: {config['federation_mode']}")
                    print(f"Policy compliance: {policies['compliance_percent']}%")
                    print(
                        f"Configuration version: {version_info['configuration_version']}"
                    )

        finally:
            await monitoring_system.stop()

        print("✅ Federation configuration endpoints test passed")

    async def test_utility_endpoints(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test utility monitoring endpoints."""
        port = server_cluster_ports[0]
        monitoring_port = server_cluster_ports[1]

        # Setup monitoring system
        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="UtilityTest-Server",
            cluster_id="utility-test-cluster",
            resources={"utility"},
            gossip_interval=1.0,
        )

        federation_config = FederationConfig(
            federation_mode=FederationMode.EXPLICIT_BRIDGING,
            local_cluster_id=settings.cluster_id,
        )

        federation_manager = FederationConnectionManager(
            federation_config=federation_config
        )

        monitoring_config = MonitoringConfig()
        unified_monitor = UnifiedSystemMonitor(config=monitoring_config)
        test_context.tasks.append(asyncio.create_task(unified_monitor.start()))

        monitoring_system = create_federation_monitoring_system(
            settings=settings,
            federation_config=federation_config,
            federation_manager=federation_manager,
            unified_monitor=unified_monitor,
            monitoring_port=monitoring_port,
        )

        await monitoring_system.start()
        base_url = f"http://{settings.host}:{monitoring_port}"

        try:
            async with aiohttp.ClientSession() as session:
                # Test root endpoint
                async with session.get(f"{base_url}/") as response:
                    assert response.status == 200
                    data = await response.json()

                    assert data["service"] == "MPREG Federation Monitoring"
                    assert "version" in data
                    assert (
                        data["federation_mode"]
                        == federation_config.federation_mode.value
                    )
                    assert data["cluster_id"] == settings.cluster_id
                    assert data["monitoring_port"] == monitoring_port
                    assert "endpoints" in data
                    assert "timestamp" in data

                    print(f"Service: {data['service']}")
                    print(f"Version: {data['version']}")
                    print(f"Endpoints available: {len(data['endpoints'])}")

                # Test /endpoints endpoint
                async with session.get(f"{base_url}/endpoints") as response:
                    assert response.status == 200
                    data = await response.json()

                    assert data["status"] == "ok"
                    assert "endpoints" in data
                    assert "total_endpoints" in data
                    assert "timestamp" in data

                    endpoints = data["endpoints"]
                    assert isinstance(endpoints, list)
                    assert len(endpoints) > 0

                    # Verify endpoint structure
                    endpoint = endpoints[0]
                    assert "path" in endpoint
                    assert "method" in endpoint
                    assert "description" in endpoint

                    print(f"Total endpoints: {data['total_endpoints']}")

                    # Verify expected endpoints are present
                    endpoint_paths = {ep["path"] for ep in endpoints}
                    expected_paths = {"/health", "/metrics", "/topology", "/config"}
                    assert expected_paths.issubset(endpoint_paths)

        finally:
            await monitoring_system.stop()

        print("✅ Federation utility endpoints test passed")

    async def test_middleware_and_cors(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test middleware functionality including CORS and metrics tracking."""
        port = server_cluster_ports[0]
        monitoring_port = server_cluster_ports[1]

        # Setup monitoring system with CORS enabled
        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="MiddlewareTest-Server",
            cluster_id="middleware-test-cluster",
            resources={"middleware"},
            gossip_interval=1.0,
        )

        federation_config = FederationConfig(
            federation_mode=FederationMode.STRICT_ISOLATION,
            local_cluster_id=settings.cluster_id,
        )

        federation_manager = FederationConnectionManager(
            federation_config=federation_config
        )

        monitoring_config = MonitoringConfig()
        unified_monitor = UnifiedSystemMonitor(config=monitoring_config)
        test_context.tasks.append(asyncio.create_task(unified_monitor.start()))

        monitoring_system = create_federation_monitoring_system(
            settings=settings,
            federation_config=federation_config,
            federation_manager=federation_manager,
            unified_monitor=unified_monitor,
            monitoring_port=monitoring_port,
            enable_cors=True,  # Enable CORS for this test
        )

        await monitoring_system.start()
        base_url = f"http://{settings.host}:{monitoring_port}"

        try:
            async with aiohttp.ClientSession() as session:
                # Test CORS headers
                async with session.get(f"{base_url}/health") as response:
                    assert response.status == 200

                    # Verify CORS headers are present
                    assert "Access-Control-Allow-Origin" in response.headers
                    assert response.headers["Access-Control-Allow-Origin"] == "*"
                    assert "Access-Control-Allow-Methods" in response.headers
                    assert "Access-Control-Allow-Headers" in response.headers

                    # Verify performance headers are present
                    assert "X-Response-Time-Ms" in response.headers
                    assert "X-Monitoring-System" in response.headers
                    assert (
                        response.headers["X-Monitoring-System"]
                        == "MPREG-Federation-Monitor"
                    )

                    response_time = float(response.headers["X-Response-Time-Ms"])
                    assert response_time > 0.0
                    print(f"Response time: {response_time:.2f}ms")

                # Test that metrics are being tracked
                initial_metrics_count = len(
                    monitoring_system.endpoint_metrics.get("/health", [])
                )

                # Make several requests to accumulate metrics
                for _ in range(3):
                    async with session.get(f"{base_url}/health") as response:
                        assert response.status == 200

                # Verify metrics were recorded
                final_metrics_count = len(
                    monitoring_system.endpoint_metrics.get("/health", [])
                )
                assert final_metrics_count > initial_metrics_count
                print(f"Metrics recorded: {final_metrics_count} measurements")

        finally:
            await monitoring_system.stop()

        print("✅ Federation middleware and CORS test passed")


class TestFederationMonitoringIntegration:
    """Test integration between federation monitoring and unified monitoring system."""

    async def test_unified_monitoring_integration(
        self,
        test_context: AsyncTestContext,
        server_cluster_ports: list[int],
    ):
        """Test integration with unified monitoring system."""
        port = server_cluster_ports[0]
        monitoring_port = server_cluster_ports[1]

        # Setup monitoring system
        settings = MPREGSettings(
            host="127.0.0.1",
            port=port,
            name="IntegrationTest-Server",
            cluster_id="integration-test-cluster",
            resources={"integration"},
            gossip_interval=1.0,
        )

        federation_config = FederationConfig(
            federation_mode=FederationMode.EXPLICIT_BRIDGING,
            local_cluster_id=settings.cluster_id,
        )

        federation_manager = FederationConnectionManager(
            federation_config=federation_config
        )

        # Create unified monitor with shorter intervals for testing
        monitoring_config = MonitoringConfig(
            metrics_collection_interval_ms=100.0,  # Very fast for testing
            health_check_interval_ms=200.0,
        )
        unified_monitor = UnifiedSystemMonitor(config=monitoring_config)

        # Start unified monitor
        unified_task = asyncio.create_task(unified_monitor.start())
        test_context.tasks.append(unified_task)

        # Create federation monitoring system
        monitoring_system = create_federation_monitoring_system(
            settings=settings,
            federation_config=federation_config,
            federation_manager=federation_manager,
            unified_monitor=unified_monitor,
            monitoring_port=monitoring_port,
        )

        await monitoring_system.start()
        base_url = f"http://{settings.host}:{monitoring_port}"

        try:
            # Allow some time for metrics collection
            await asyncio.sleep(0.5)

            async with aiohttp.ClientSession() as session:
                # Test that federation metrics integrate with unified metrics
                async with session.get(f"{base_url}/metrics") as response:
                    assert response.status == 200
                    data = await response.json()

                    assert data["status"] == "ok"
                    federation_metrics = data["federation_metrics"]

                    # Verify unified monitor data is being used
                    assert "federation_system" in federation_metrics
                    assert "correlation_metrics" in federation_metrics
                    assert "overall_health" in federation_metrics

                    # Verify collection info from unified system
                    collection_info = data["collection_info"]
                    assert "collection_timestamp" in collection_info
                    assert "collection_duration_ms" in collection_info

                    print("✅ Unified monitoring integration verified")

                # Test health data integration
                async with session.get(f"{base_url}/health") as response:
                    assert response.status == 200
                    data = await response.json()

                    federation_health = data["federation_health"]

                    # Verify health data is consistent with unified monitor
                    assert "overall_status" in federation_health
                    assert "health_score" in federation_health

                    # Health score should be a valid float between 0 and 1
                    health_score = federation_health["health_score"]
                    assert isinstance(health_score, int | float)
                    assert 0.0 <= health_score <= 1.0

                    print(f"✅ Health integration verified (score: {health_score})")

        finally:
            await monitoring_system.stop()

        print("✅ Federation monitoring integration test passed")
