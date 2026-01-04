"""Real-world workflow examples demonstrating MPREG's distributed capabilities.

These tests showcase practical use cases that demonstrate the power of MPREG's
hierarchical dependency resolution and distributed execution capabilities.
Each test represents a complete, realistic workflow that users might implement
in production environments.
"""

import asyncio
import json
from collections.abc import AsyncGenerator
from typing import Any

import pytest

from mpreg.client.client_api import MPREGClientAPI
from mpreg.core.model import RPCCommand
from mpreg.server import MPREGServer
from tests.conftest import AsyncTestContext


class TestDataPipelineWorkflows:
    """Real-world data processing pipeline examples."""

    @pytest.fixture
    async def data_pipeline_cluster(
        self, test_context: AsyncTestContext, server_cluster_ports: list[int]
    ) -> AsyncGenerator[list[MPREGServer]]:
        """Set up a 3-server cluster optimized for data pipeline processing."""
        from mpreg.core.config import MPREGSettings

        port1, port2, port3 = server_cluster_ports[:3]  # Use first 3 ports

        # Data ingestion server
        ingestion_settings = MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="Data Ingestion Server",
            cluster_id="pipeline-cluster",
            resources={"raw-data", "csv-parser", "json-parser"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=5.0,
        )

        # Processing server
        processing_settings = MPREGSettings(
            host="127.0.0.1",
            port=port2,
            name="Data Processing Server",
            cluster_id="pipeline-cluster",
            resources={"data-cleaner", "transformer", "aggregator"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",
            advertised_urls=None,
            gossip_interval=5.0,
        )

        # Output server
        output_settings = MPREGSettings(
            host="127.0.0.1",
            port=port3,
            name="Data Output Server",
            cluster_id="pipeline-cluster",
            resources={"formatter", "validator", "exporter"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",
            advertised_urls=None,
            gossip_interval=5.0,
        )

        servers: list[MPREGServer] = [
            MPREGServer(settings=s)
            for s in [ingestion_settings, processing_settings, output_settings]
        ]

        # Register pipeline functions on appropriate servers
        def parse_csv_data(raw_data: str) -> list[dict[str, Any]]:
            """Parse CSV data into structured format."""
            lines = raw_data.strip().split("\n")
            headers = lines[0].split(",")
            return [
                {headers[i]: row.split(",")[i] for i in range(len(headers))}
                for row in lines[1:]
            ]

        def clean_data(parsed_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
            """Clean and validate parsed data."""
            cleaned = []
            for row in parsed_data:
                # Remove rows with missing critical fields
                if all(row.get(field) for field in ["id", "value"]):
                    # Clean numeric fields
                    if "value" in row:
                        try:
                            row["value"] = float(row["value"])
                        except ValueError:
                            continue
                    cleaned.append(row)
            return cleaned

        def aggregate_data(cleaned_data: list[dict[str, Any]]) -> dict[str, Any]:
            """Aggregate cleaned data into summary statistics."""
            if not cleaned_data:
                return {"count": 0, "sum": 0, "avg": 0}

            values = [row["value"] for row in cleaned_data if "value" in row]
            return {
                "count": len(cleaned_data),
                "sum": sum(values),
                "avg": sum(values) / len(values) if values else 0,
                "records": len(cleaned_data),
            }

        def format_report(aggregated_data: dict[str, Any]) -> str:
            """Format aggregated data into a final report."""
            return json.dumps(
                {
                    "report_type": "data_pipeline_summary",
                    "processed_records": aggregated_data["count"],
                    "total_value": aggregated_data["sum"],
                    "average_value": aggregated_data["avg"],
                    "status": "completed",
                },
                indent=2,
            )

        # Register functions on appropriate servers
        servers[0].register_command("parse_csv", parse_csv_data, ["csv-parser"])
        servers[1].register_command("clean_data", clean_data, ["data-cleaner"])
        servers[1].register_command("aggregate_data", aggregate_data, ["aggregator"])
        servers[2].register_command("format_report", format_report, ["formatter"])

        test_context.servers.extend(servers)

        # Start all servers
        tasks: list[asyncio.Task[Any]] = [
            asyncio.create_task(server.server()) for server in servers
        ]
        test_context.tasks.extend(tasks)

        # Wait for cluster formation
        await asyncio.sleep(1.0)

        yield servers

    async def test_complete_data_pipeline(
        self, data_pipeline_cluster: list[MPREGServer], client_factory: Any
    ) -> None:
        """Example: Complete data processing pipeline from raw CSV to formatted report.

        This demonstrates a realistic data pipeline where:
        1. Raw CSV data is parsed into structured format
        2. Data is cleaned and validated
        3. Clean data is aggregated into statistics
        4. Final report is formatted for output

        Each step runs on a different server with appropriate resources.
        """
        servers: list[MPREGServer] = data_pipeline_cluster
        client: MPREGClientAPI = await client_factory(servers[0].settings.port)

        # Sample CSV data to process
        raw_csv_data: str = """id,name,value,category
1,Item A,100.5,electronics
2,Item B,200.0,books
3,Item C,invalid,electronics
4,Item D,150.75,books
5,Item E,75.25,electronics"""

        # Execute complete pipeline
        result: dict[str, Any] = await client._client.request(
            [
                RPCCommand(
                    name="parsed",
                    fun="parse_csv",
                    args=(raw_csv_data,),
                    locs=frozenset(["csv-parser"]),
                ),
                RPCCommand(
                    name="cleaned",
                    fun="clean_data",
                    args=("parsed",),
                    locs=frozenset(["data-cleaner"]),
                ),
                RPCCommand(
                    name="aggregated",
                    fun="aggregate_data",
                    args=("cleaned",),
                    locs=frozenset(["aggregator"]),
                ),
                RPCCommand(
                    name="report",
                    fun="format_report",
                    args=("aggregated",),
                    locs=frozenset(["formatter"]),
                ),
            ]
        )

        # Verify pipeline completed successfully
        assert "report" in result
        report_data: dict[str, Any] = json.loads(result["report"])
        assert report_data["report_type"] == "data_pipeline_summary"
        assert report_data["processed_records"] == 4  # Excluding invalid row
        assert report_data["status"] == "completed"
        assert report_data["total_value"] == 526.5  # Sum of valid values


class TestMLWorkflows:
    """Machine learning workflow examples."""

    @pytest.fixture
    async def ml_cluster(
        self, test_context: AsyncTestContext, server_cluster_ports: list[int]
    ) -> AsyncGenerator[list[MPREGServer]]:
        """Set up a cluster optimized for ML workflows."""
        from mpreg.core.config import MPREGSettings

        port1, port2, port3 = server_cluster_ports[:3]  # Use first 3 ports

        # Data preprocessing server
        prep_settings = MPREGSettings(
            host="127.0.0.1",
            port=port1,
            name="ML Preprocessing Server",
            cluster_id="ml-cluster",
            resources={"data-loader", "feature-extractor", "normalizer"},
            peers=None,
            connect=None,
            advertised_urls=None,
            gossip_interval=5.0,
        )

        # Model serving server
        model_settings = MPREGSettings(
            host="127.0.0.1",
            port=port2,
            name="ML Model Server",
            cluster_id="ml-cluster",
            resources={"model-a", "model-b", "ensemble"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",
            advertised_urls=None,
            gossip_interval=5.0,
        )

        # Post-processing server
        post_settings = MPREGSettings(
            host="127.0.0.1",
            port=port3,
            name="ML Postprocessing Server",
            cluster_id="ml-cluster",
            resources={"result-processor", "confidence-calculator", "explainer"},
            peers=None,
            connect=f"ws://127.0.0.1:{port1}",
            advertised_urls=None,
            gossip_interval=5.0,
        )

        servers: list[MPREGServer] = [
            MPREGServer(settings=s)
            for s in [prep_settings, model_settings, post_settings]
        ]

        # Register ML pipeline functions
        def extract_features(raw_data: dict[str, Any]) -> list[float]:
            """Extract numerical features from raw data."""
            features = []
            for key, value in raw_data.items():
                if isinstance(value, int | float):
                    features.append(float(value))
                elif isinstance(value, str):
                    features.append(float(len(value)))  # String length as feature
            return features

        def normalize_features(features: list[float]) -> list[float]:
            """Normalize features to 0-1 range."""
            if not features:
                return features
            min_val, max_val = min(features), max(features)
            if max_val == min_val:
                return [0.5] * len(features)
            return [(f - min_val) / (max_val - min_val) for f in features]

        def model_predict(features: list[float], model_name: str) -> dict[str, Any]:
            """Run model prediction on normalized features."""
            # Simulate different model behaviors
            if model_name == "model-a":
                prediction = sum(features) / len(features) if features else 0
                confidence = min(0.95, prediction + 0.1)
            else:  # model-b
                prediction = max(features) if features else 0
                confidence = min(0.9, prediction + 0.05)

            return {
                "model": model_name,
                "prediction": prediction,
                "confidence": confidence,
                "features_used": len(features),
            }

        def ensemble_predict(
            pred_a: dict[str, Any], pred_b: dict[str, Any]
        ) -> dict[str, Any]:
            """Combine predictions from multiple models."""
            ensemble_pred = (pred_a["prediction"] + pred_b["prediction"]) / 2
            ensemble_conf = min(pred_a["confidence"], pred_b["confidence"])

            return {
                "ensemble_prediction": ensemble_pred,
                "ensemble_confidence": ensemble_conf,
                "individual_predictions": [pred_a, pred_b],
                "model_agreement": abs(pred_a["prediction"] - pred_b["prediction"])
                < 0.1,
            }

        def explain_prediction(ensemble_result: dict[str, Any]) -> dict[str, Any]:
            """Generate explanation for the ensemble prediction."""
            confidence = ensemble_result["ensemble_confidence"]
            agreement = ensemble_result["model_agreement"]

            if confidence > 0.8 and agreement:
                explanation = "High confidence prediction with model agreement"
                reliability = "high"
            elif confidence > 0.6:
                explanation = "Moderate confidence prediction"
                reliability = "medium"
            else:
                explanation = "Low confidence prediction, review recommended"
                reliability = "low"

            return {
                "explanation": explanation,
                "reliability": reliability,
                "confidence_score": confidence,
                "final_prediction": ensemble_result["ensemble_prediction"],
            }

        # Register functions on appropriate servers
        servers[0].register_command(
            "extract_features", extract_features, ["feature-extractor"]
        )
        servers[0].register_command(
            "normalize_features", normalize_features, ["normalizer"]
        )
        servers[1].register_command(
            "model_predict", model_predict, ["model-a", "model-b"]
        )
        servers[1].register_command("ensemble_predict", ensemble_predict, ["ensemble"])
        servers[2].register_command(
            "explain_prediction", explain_prediction, ["explainer"]
        )

        test_context.servers.extend(servers)

        # Start all servers
        tasks: list[asyncio.Task[Any]] = [
            asyncio.create_task(server.server()) for server in servers
        ]
        test_context.tasks.extend(tasks)

        await asyncio.sleep(1.0)
        yield servers

    async def test_ml_inference_pipeline(
        self, ml_cluster: list[MPREGServer], client_factory: Any
    ) -> None:
        """Example: Complete ML inference pipeline with ensemble modeling.

        This demonstrates a realistic ML workflow:
        1. Extract features from raw input data
        2. Normalize features for model consumption
        3. Run predictions with multiple models
        4. Combine predictions using ensemble method
        5. Generate explanation for the final prediction
        """
        servers: list[MPREGServer] = ml_cluster
        client: MPREGClientAPI = await client_factory(servers[0].settings.port)

        # Sample input data for ML inference
        input_data: dict[str, Any] = {
            "user_age": 35,
            "account_balance": 1250.75,
            "transaction_history": "frequent_user",
            "location": "urban",
            "device_type": "mobile",
        }

        # Execute complete ML pipeline
        result: dict[str, Any] = await client._client.request(
            [
                # Feature extraction and preprocessing
                RPCCommand(
                    name="features",
                    fun="extract_features",
                    args=(input_data,),
                    locs=frozenset(["feature-extractor"]),
                ),
                RPCCommand(
                    name="normalized",
                    fun="normalize_features",
                    args=("features",),
                    locs=frozenset(["normalizer"]),
                ),
                # Model predictions
                RPCCommand(
                    name="pred_a",
                    fun="model_predict",
                    args=("normalized", "model-a"),
                    locs=frozenset(["model-a"]),
                ),
                RPCCommand(
                    name="pred_b",
                    fun="model_predict",
                    args=("normalized", "model-b"),
                    locs=frozenset(["model-b"]),
                ),
                # Ensemble and explanation
                RPCCommand(
                    name="ensemble",
                    fun="ensemble_predict",
                    args=("pred_a", "pred_b"),
                    locs=frozenset(["ensemble"]),
                ),
                RPCCommand(
                    name="explanation",
                    fun="explain_prediction",
                    args=("ensemble",),
                    locs=frozenset(["explainer"]),
                ),
            ]
        )

        # Verify ML pipeline results
        assert "explanation" in result
        explanation: dict[str, Any] = result["explanation"]
        assert "explanation" in explanation
        assert "reliability" in explanation
        assert "confidence_score" in explanation
        assert "final_prediction" in explanation
        assert explanation["reliability"] in ["high", "medium", "low"]


class TestBusinessWorkflows:
    """Business process automation examples."""

    async def test_order_processing_workflow(
        self,
        cluster_3_servers: tuple[MPREGServer, MPREGServer, MPREGServer],
        client_factory: Any,
    ) -> None:
        """Example: Complete e-commerce order processing workflow.

        Demonstrates a business process spanning multiple systems:
        1. Validate order details and customer information
        2. Check inventory availability
        3. Process payment
        4. Update inventory and create shipping record
        5. Send confirmation notifications
        """
        primary: MPREGServer
        secondary: MPREGServer
        tertiary: MPREGServer
        primary, secondary, tertiary = cluster_3_servers

        # Register business process functions
        def validate_order(order_data: dict[str, Any]) -> dict[str, Any]:
            """Validate order data and customer information."""
            required_fields = ["customer_id", "items", "total_amount"]

            validation_result = {
                "valid": all(field in order_data for field in required_fields),
                "order_id": f"ORD-{hash(str(order_data)) % 10000:04d}",
                "validated_data": order_data,
            }

            if validation_result["valid"]:
                validation_result["status"] = "validated"
            else:
                validation_result["status"] = "validation_failed"
                validation_result["missing_fields"] = [
                    field for field in required_fields if field not in order_data
                ]

            return validation_result

        def check_inventory(validated_order: dict[str, Any]) -> dict[str, Any]:
            """Check inventory availability for order items."""
            if not validated_order["valid"]:
                return {"status": "skipped", "reason": "invalid_order"}

            # Simulate inventory check
            items = validated_order["validated_data"]["items"]
            inventory_result = {
                "order_id": validated_order["order_id"],
                "items_available": True,  # Simulate availability
                "reserved_items": items,
                "status": "inventory_confirmed",
            }

            return inventory_result

        def process_payment(inventory_result: dict[str, Any]) -> dict[str, Any]:
            """Process payment for the order."""
            if inventory_result["status"] != "inventory_confirmed":
                return {"status": "skipped", "reason": "inventory_not_available"}

            # Simulate payment processing
            payment_result = {
                "order_id": inventory_result["order_id"],
                "payment_status": "completed",
                "transaction_id": f"TXN-{hash(str(inventory_result)) % 10000:04d}",
                "status": "payment_processed",
            }

            return payment_result

        def fulfill_order(payment_result: dict[str, Any]) -> dict[str, Any]:
            """Create shipping record and update inventory."""
            if payment_result["status"] != "payment_processed":
                return {"status": "skipped", "reason": "payment_not_completed"}

            fulfillment_result = {
                "order_id": payment_result["order_id"],
                "shipping_id": f"SHIP-{hash(str(payment_result)) % 10000:04d}",
                "tracking_number": f"TRK{hash(str(payment_result)) % 1000000:06d}",
                "estimated_delivery": "3-5 business days",
                "status": "order_fulfilled",
            }

            return fulfillment_result

        def send_confirmation(fulfillment_result: dict[str, Any]) -> str:
            """Send order confirmation to customer."""
            if fulfillment_result["status"] != "order_fulfilled":
                return "Confirmation not sent - order not fulfilled"

            confirmation = f"""
Order Confirmation: {fulfillment_result["order_id"]}
Shipping ID: {fulfillment_result["shipping_id"]}
Tracking Number: {fulfillment_result["tracking_number"]}
Estimated Delivery: {fulfillment_result["estimated_delivery"]}
Status: Order confirmed and being prepared for shipment
"""
            return confirmation.strip()

        # Register functions across servers
        primary.register_command("validate_order", validate_order, ["validator"])
        primary.register_command("check_inventory", check_inventory, ["inventory"])
        secondary.register_command(
            "process_payment", process_payment, ["payment-processor"]
        )
        secondary.register_command("fulfill_order", fulfill_order, ["fulfillment"])
        tertiary.register_command(
            "send_confirmation", send_confirmation, ["notification"]
        )

        await asyncio.sleep(1.0)

        client: MPREGClientAPI = await client_factory(primary.settings.port)

        # Sample order data
        order_data: dict[str, Any] = {
            "customer_id": "CUST-12345",
            "items": [
                {"sku": "ITEM-001", "quantity": 2, "price": 29.99},
                {"sku": "ITEM-002", "quantity": 1, "price": 15.50},
            ],
            "total_amount": 75.48,
            "shipping_address": "123 Main St, City, State 12345",
        }

        # Execute complete order processing workflow
        result: dict[str, Any] = await client._client.request(
            [
                RPCCommand(
                    name="validation",
                    fun="validate_order",
                    args=(order_data,),
                    locs=frozenset(["validator"]),
                ),
                RPCCommand(
                    name="inventory",
                    fun="check_inventory",
                    args=("validation",),
                    locs=frozenset(["inventory"]),
                ),
                RPCCommand(
                    name="payment",
                    fun="process_payment",
                    args=("inventory",),
                    locs=frozenset(["payment-processor"]),
                ),
                RPCCommand(
                    name="fulfillment",
                    fun="fulfill_order",
                    args=("payment",),
                    locs=frozenset(["fulfillment"]),
                ),
                RPCCommand(
                    name="confirmation",
                    fun="send_confirmation",
                    args=("fulfillment",),
                    locs=frozenset(["notification"]),
                ),
            ]
        )

        # Verify order processing completed successfully
        assert "confirmation" in result
        confirmation_msg: str = result["confirmation"]
        assert "Order Confirmation:" in confirmation_msg
        assert "Tracking Number:" in confirmation_msg
        assert "Order confirmed and being prepared for shipment" in confirmation_msg


class TestMonitoringAndObservability:
    """Examples of monitoring and observability workflows."""

    async def test_distributed_health_check(
        self,
        cluster_3_servers: tuple[MPREGServer, MPREGServer, MPREGServer],
        client_factory: Any,
    ) -> None:
        """Example: Distributed system health monitoring workflow.

        Demonstrates how to implement health checks across a distributed
        MPREG cluster and aggregate results for monitoring.
        """
        primary: MPREGServer
        secondary: MPREGServer
        tertiary: MPREGServer
        primary, secondary, tertiary = cluster_3_servers

        # Register health check functions
        def check_system_health() -> dict[str, Any]:
            """Check local system health metrics."""
            import time

            import psutil

            return {
                "timestamp": time.time(),
                "cpu_usage": psutil.cpu_percent(),
                "memory_usage": psutil.virtual_memory().percent,
                "disk_usage": psutil.disk_usage("/").percent,
                "status": "healthy",
            }

        def check_database_health() -> dict[str, Any]:
            """Simulate database health check."""
            import time

            # Simulate DB connection check
            return {
                "timestamp": time.time(),
                "connection_status": "connected",
                "query_latency_ms": 12.5,
                "active_connections": 8,
                "status": "healthy",
            }

        def check_external_api_health() -> dict[str, Any]:
            """Simulate external API health check."""
            import time

            # Simulate API health check
            return {
                "timestamp": time.time(),
                "api_status": "available",
                "response_time_ms": 45.2,
                "rate_limit_remaining": 950,
                "status": "healthy",
            }

        def aggregate_health_status(
            system_health: dict[str, Any],
            db_health: dict[str, Any],
            api_health: dict[str, Any],
        ) -> dict[str, Any]:
            """Aggregate all health check results."""
            import time

            overall_status = "healthy"
            issues = []

            # Check for any unhealthy components
            for component, health in [
                ("system", system_health),
                ("database", db_health),
                ("external_api", api_health),
            ]:
                if health["status"] != "healthy":
                    overall_status = "degraded"
                    issues.append(f"{component}: {health['status']}")

            return {
                "timestamp": time.time(),
                "overall_status": overall_status,
                "components": {
                    "system": system_health,
                    "database": db_health,
                    "external_api": api_health,
                },
                "issues": issues,
                "healthy_components": sum(
                    1
                    for h in [system_health, db_health, api_health]
                    if h["status"] == "healthy"
                ),
                "total_components": 3,
            }

        # Register health check functions across cluster
        primary.register_command(
            "check_system_health", check_system_health, ["monitor"]
        )
        secondary.register_command(
            "check_database_health", check_database_health, ["db-monitor"]
        )
        tertiary.register_command(
            "check_external_api_health", check_external_api_health, ["api-monitor"]
        )
        primary.register_command(
            "aggregate_health_status", aggregate_health_status, ["aggregator"]
        )

        await asyncio.sleep(1.0)

        client: MPREGClientAPI = await client_factory(primary.settings.port)

        # Execute distributed health check workflow
        result: dict[str, Any] = await client._client.request(
            [
                RPCCommand(
                    name="system_check",
                    fun="check_system_health",
                    locs=frozenset(["monitor"]),
                ),
                RPCCommand(
                    name="db_check",
                    fun="check_database_health",
                    locs=frozenset(["db-monitor"]),
                ),
                RPCCommand(
                    name="api_check",
                    fun="check_external_api_health",
                    locs=frozenset(["api-monitor"]),
                ),
                RPCCommand(
                    name="health_summary",
                    fun="aggregate_health_status",
                    args=("system_check", "db_check", "api_check"),
                    locs=frozenset(["aggregator"]),
                ),
            ]
        )

        # Verify health check results
        assert "health_summary" in result
        summary: dict[str, Any] = result["health_summary"]
        assert "overall_status" in summary
        assert "components" in summary
        assert "healthy_components" in summary
        assert summary["total_components"] == 3
        assert summary["overall_status"] in ["healthy", "degraded", "unhealthy"]
