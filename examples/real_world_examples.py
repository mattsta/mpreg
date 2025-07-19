#!/usr/bin/env python3
"""Real-world examples demonstrating MPREG's practical applications.

Run with: poetry run python examples/real_world_examples.py
"""

import asyncio
import time

from mpreg.client_api import MPREGClientAPI
from mpreg.config import MPREGSettings
from mpreg.model import RPCCommand
from mpreg.server import MPREGServer


class DataPipelineExample:
    """Real-time data processing pipeline across specialized nodes."""

    async def setup_pipeline_cluster(self):
        """Setup a multi-node data processing cluster."""
        servers = []

        # Data Ingestion Node
        ingestion_server = MPREGServer(
            MPREGSettings(
                port=9001,
                name="Data-Ingestion",
                resources={"ingestion", "raw-data", "sensors"},
                log_level="INFO",
            )
        )

        # Data Processing Node
        processing_server = MPREGServer(
            MPREGSettings(
                port=9002,
                name="Data-Processing",
                resources={"processing", "etl", "validation"},
                peers=["ws://127.0.0.1:9001"],
                log_level="INFO",
            )
        )

        # Analytics Node
        analytics_server = MPREGServer(
            MPREGSettings(
                port=9003,
                name="Analytics-Engine",
                resources={"analytics", "ml", "insights"},
                peers=["ws://127.0.0.1:9001"],
                log_level="INFO",
            )
        )

        # Storage Node
        storage_server = MPREGServer(
            MPREGSettings(
                port=9004,
                name="Data-Storage",
                resources={"storage", "database", "persistence"},
                peers=["ws://127.0.0.1:9001"],
                log_level="INFO",
            )
        )

        servers = [
            ingestion_server,
            processing_server,
            analytics_server,
            storage_server,
        ]

        await self._register_pipeline_functions(servers)

        # Start all servers
        tasks = []
        for server in servers:
            task = asyncio.create_task(server.server())
            tasks.append(task)
            await asyncio.sleep(0.1)  # Stagger startup

        await asyncio.sleep(2.0)  # Allow cluster formation
        return servers

    async def _register_pipeline_functions(self, servers):
        """Register functions for each pipeline stage."""

        # Ingestion functions
        def ingest_sensor_data(sensor_id: str, raw_readings: list[float]) -> dict:
            """Ingest raw sensor data."""
            return {
                "sensor_id": sensor_id,
                "raw_readings": raw_readings,
                "ingested_at": time.time(),
                "data_source": "sensor_network",
                "ingestion_node": "Data-Ingestion",
            }

        def validate_data_format(data: dict) -> dict:
            """Validate incoming data format."""
            required_fields = ["sensor_id", "raw_readings"]
            valid = all(field in data for field in required_fields)

            return {
                **data,
                "format_valid": valid,
                "validation_errors": [] if valid else ["Missing raw_readings field"],
                "validated_at": time.time(),
            }

        servers[0].register_command(
            "ingest_sensor_data", ingest_sensor_data, ["ingestion", "raw-data"]
        )
        servers[0].register_command(
            "validate_format", validate_data_format, ["ingestion", "sensors"]
        )

        # Processing functions
        def clean_data(data: dict) -> dict:
            """Clean and normalize sensor data."""
            readings = data.get("raw_readings", [])
            # Remove outliers and normalize
            cleaned = [r for r in readings if 0 <= r <= 100]  # Filter valid range
            normalized = [(r - 50) / 50 for r in cleaned]  # Normalize to -1 to 1

            return {
                **data,
                "cleaned_readings": cleaned,
                "normalized_readings": normalized,
                "outliers_removed": len(readings) - len(cleaned),
                "processing_node": "Data-Processing",
                "processed_at": time.time(),
            }

        def aggregate_metrics(data: dict) -> dict:
            """Calculate aggregate metrics."""
            readings = data.get("normalized_readings", [])
            if not readings:
                return {**data, "metrics": {}}

            metrics = {
                "mean": sum(readings) / len(readings),
                "min": min(readings),
                "max": max(readings),
                "count": len(readings),
                "variance": sum(
                    (x - sum(readings) / len(readings)) ** 2 for x in readings
                )
                / len(readings),
            }

            return {
                **data,
                "metrics": metrics,
                "aggregated_at": time.time(),
                "processing_complete": True,
            }

        servers[1].register_command("clean_data", clean_data, ["processing", "etl"])
        servers[1].register_command(
            "aggregate_metrics", aggregate_metrics, ["processing", "validation"]
        )

        # Analytics functions
        def detect_anomalies(data: dict) -> dict:
            """Detect anomalies in processed data."""
            metrics = data.get("metrics", {})
            variance = metrics.get("variance", 0)
            mean = metrics.get("mean", 0)

            # Simple anomaly detection
            anomaly_score = abs(mean) + variance
            is_anomaly = anomaly_score > 0.8

            return {
                **data,
                "anomaly_detection": {
                    "is_anomaly": is_anomaly,
                    "anomaly_score": anomaly_score,
                    "threshold": 0.8,
                    "detected_at": time.time(),
                },
                "analytics_node": "Analytics-Engine",
            }

        def generate_insights(data: dict) -> dict:
            """Generate business insights from data."""
            metrics = data.get("metrics", {})
            anomaly = data.get("anomaly_detection", {})

            insights = []
            if anomaly.get("is_anomaly"):
                insights.append("ALERT: Anomalous sensor behavior detected")

            if metrics.get("mean", 0) > 0.5:
                insights.append("INFO: Sensor readings above normal range")
            elif metrics.get("mean", 0) < -0.5:
                insights.append("INFO: Sensor readings below normal range")

            if metrics.get("variance", 0) > 0.3:
                insights.append("WARN: High variance in sensor readings")

            return {
                **data,
                "insights": insights,
                "insight_count": len(insights),
                "insights_generated_at": time.time(),
            }

        servers[2].register_command(
            "detect_anomalies", detect_anomalies, ["analytics", "ml"]
        )
        servers[2].register_command(
            "generate_insights", generate_insights, ["analytics", "insights"]
        )

        # Storage functions
        def store_processed_data(data: dict) -> dict:
            """Store processed data with metadata."""
            storage_record = {
                "record_id": f"rec_{hash(str(data)) % 100000}",
                "data": data,
                "stored_at": time.time(),
                "storage_node": "Data-Storage",
                "retention_days": 365,
            }

            return {
                "storage_record": storage_record,
                "stored": True,
                "record_id": storage_record["record_id"],
            }

        def create_data_summary(data: dict) -> dict:
            """Create summary for dashboard/reporting."""
            summary = {
                "sensor_id": data.get("sensor_id"),
                "reading_count": data.get("metrics", {}).get("count", 0),
                "processing_complete": data.get("processing_complete", False),
                "has_anomaly": data.get("anomaly_detection", {}).get(
                    "is_anomaly", False
                ),
                "insight_count": data.get("insight_count", 0),
                "pipeline_duration": time.time() - data.get("ingested_at", time.time()),
                "summary_created_at": time.time(),
            }

            return {**data, "dashboard_summary": summary}

        servers[3].register_command(
            "store_data", store_processed_data, ["storage", "database"]
        )
        servers[3].register_command(
            "create_summary", create_data_summary, ["storage", "persistence"]
        )

    async def run_pipeline_example(self):
        """Demonstrate the complete data pipeline."""
        print("🏭 Setting up real-time data processing pipeline...")
        servers = await self.setup_pipeline_cluster()

        try:
            async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
                print("📊 Processing sensor data through the pipeline...")

                # Simulate processing multiple sensor streams
                sensor_data = [
                    ("temp_sensor_01", [22.5, 23.1, 24.0, 25.2, 23.8]),
                    (
                        "pressure_sensor_02",
                        [101.3, 101.1, 101.4, 102.8, 101.2],
                    ),  # Anomalous reading
                    ("humidity_sensor_03", [45.2, 46.1, 44.8, 45.5, 46.0]),
                ]

                pipeline_results = []

                for sensor_id, readings in sensor_data:
                    print(f"   Processing {sensor_id}...")

                    # Complete pipeline workflow
                    result = await client._client.request(
                        [
                            # Stage 1: Ingest raw data
                            RPCCommand(
                                name="ingested",
                                fun="ingest_sensor_data",
                                args=(sensor_id, readings),
                                locs=frozenset(["ingestion", "raw-data"]),
                            ),
                            # Stage 2: Validate format
                            RPCCommand(
                                name="validated",
                                fun="validate_format",
                                args=("ingested",),
                                locs=frozenset(["ingestion", "sensors"]),
                            ),
                            # Stage 3: Clean and normalize
                            RPCCommand(
                                name="cleaned",
                                fun="clean_data",
                                args=("validated",),
                                locs=frozenset(["processing", "etl"]),
                            ),
                            # Stage 4: Calculate metrics
                            RPCCommand(
                                name="aggregated",
                                fun="aggregate_metrics",
                                args=("cleaned",),
                                locs=frozenset(["processing", "validation"]),
                            ),
                            # Stage 5: Detect anomalies
                            RPCCommand(
                                name="analyzed",
                                fun="detect_anomalies",
                                args=("aggregated",),
                                locs=frozenset(["analytics", "ml"]),
                            ),
                            # Stage 6: Generate insights
                            RPCCommand(
                                name="insights",
                                fun="generate_insights",
                                args=("analyzed",),
                                locs=frozenset(["analytics", "insights"]),
                            ),
                            # Stage 7: Store data
                            RPCCommand(
                                name="stored",
                                fun="store_data",
                                args=("insights",),
                                locs=frozenset(["storage", "database"]),
                            ),
                            # Stage 8: Create dashboard summary
                            RPCCommand(
                                name="summary",
                                fun="create_summary",
                                args=("stored",),
                                locs=frozenset(["storage", "persistence"]),
                            ),
                        ]
                    )

                    pipeline_results.append(result)

                    # Show key results
                    summary = result["summary"]["dashboard_summary"]
                    print(f"      ✅ Processed {summary['reading_count']} readings")
                    print(f"      ⚠️  Anomaly detected: {summary['has_anomaly']}")
                    print(f"      💡 Insights generated: {summary['insight_count']}")
                    print(
                        f"      ⏱️  Pipeline duration: {summary['pipeline_duration']:.3f}s"
                    )

                print("\n🎯 Pipeline Demonstration Complete!")
                print("✨ This showcases MPREG's ability to:")
                print("   • Route data through specialized processing nodes")
                print("   • Handle complex multi-stage dependencies automatically")
                print("   • Provide real-time processing with millisecond latencies")
                print("   • Scale horizontally across different resource types")

                return pipeline_results

        finally:
            print("\n🧹 Shutting down pipeline cluster...")
            for server in servers:
                if hasattr(server, "_shutdown_event"):
                    server._shutdown_event.set()
            await asyncio.sleep(0.5)


class MLInferenceExample:
    """Distributed ML inference pipeline with model routing."""

    async def setup_ml_cluster(self):
        """Setup ML inference cluster with specialized model nodes."""
        servers = []

        # Model Router / Load Balancer
        router = MPREGServer(
            MPREGSettings(
                port=9001,
                name="ML-Router",
                resources={"routing", "orchestration"},
                log_level="INFO",
            )
        )

        # Vision Models Node
        vision_node = MPREGServer(
            MPREGSettings(
                port=9002,
                name="Vision-Models",
                resources={"vision", "image-classification", "object-detection"},
                peers=["ws://127.0.0.1:9001"],
                log_level="INFO",
            )
        )

        # NLP Models Node
        nlp_node = MPREGServer(
            MPREGSettings(
                port=9003,
                name="NLP-Models",
                resources={"nlp", "text-analysis", "sentiment"},
                peers=["ws://127.0.0.1:9001"],
                log_level="INFO",
            )
        )

        # Feature Processing Node
        feature_node = MPREGServer(
            MPREGSettings(
                port=9004,
                name="Feature-Processing",
                resources={"preprocessing", "feature-extraction", "embeddings"},
                peers=["ws://127.0.0.1:9001"],
                log_level="INFO",
            )
        )

        servers = [router, vision_node, nlp_node, feature_node]

        await self._register_ml_functions(servers)

        # Start servers
        tasks = []
        for server in servers:
            task = asyncio.create_task(server.server())
            tasks.append(task)
            await asyncio.sleep(0.1)

        await asyncio.sleep(2.0)
        return servers

    async def _register_ml_functions(self, servers):
        """Register ML inference functions."""

        # Router functions
        def route_inference_request(
            data_type: str, model_name: str, input_data: str
        ) -> dict:
            """Route inference requests to appropriate nodes."""
            return {
                "request_id": f"req_{hash(input_data) % 10000}",
                "data_type": data_type,
                "model_name": model_name,
                "input_data": input_data,
                "routed_at": time.time(),
                "router_node": "ML-Router",
            }

        servers[0].register_command(
            "route_request", route_inference_request, ["routing"]
        )

        # Vision model functions
        def classify_image(request: dict) -> dict:
            """Image classification inference."""
            model = request.get("model_name", "resnet50")
            confidence_scores = {"cat": 0.89, "dog": 0.03, "bird": 0.08}

            return {
                **request,
                "model_type": "vision",
                "model_used": model,
                "predictions": confidence_scores,
                "top_prediction": max(confidence_scores.items(), key=lambda x: x[1]),
                "inference_time_ms": 45,
                "vision_node": "Vision-Models",
            }

        def detect_objects(request: dict) -> dict:
            """Object detection inference."""
            detected_objects = [
                {"class": "person", "confidence": 0.95, "bbox": [100, 100, 200, 300]},
                {"class": "car", "confidence": 0.87, "bbox": [300, 150, 500, 250]},
            ]

            return {
                **request,
                "model_type": "object_detection",
                "detected_objects": detected_objects,
                "object_count": len(detected_objects),
                "inference_time_ms": 78,
            }

        servers[1].register_command(
            "classify_image", classify_image, ["vision", "image-classification"]
        )
        servers[1].register_command(
            "detect_objects", detect_objects, ["vision", "object-detection"]
        )

        # NLP model functions
        def analyze_sentiment(request: dict) -> dict:
            """Sentiment analysis inference."""
            text = request.get("input_data", "")

            # Simple sentiment scoring
            positive_words = ["great", "excellent", "amazing", "wonderful", "fantastic"]
            negative_words = ["terrible", "awful", "horrible", "bad", "disappointing"]

            pos_count = sum(word in text.lower() for word in positive_words)
            neg_count = sum(word in text.lower() for word in negative_words)

            if pos_count > neg_count:
                sentiment = "positive"
                score = 0.7 + (pos_count * 0.1)
            elif neg_count > pos_count:
                sentiment = "negative"
                score = 0.3 - (neg_count * 0.1)
            else:
                sentiment = "neutral"
                score = 0.5

            return {
                **request,
                "model_type": "sentiment",
                "sentiment": sentiment,
                "confidence_score": min(max(score, 0.0), 1.0),
                "text_length": len(text),
                "inference_time_ms": 23,
            }

        def extract_entities(request: dict) -> dict:
            """Named entity recognition."""
            text = request.get("input_data", "")

            # Mock entity extraction
            entities = [
                {"text": "OpenAI", "label": "ORG", "start": 0, "end": 6},
                {"text": "San Francisco", "label": "LOC", "start": 20, "end": 33},
            ]

            return {
                **request,
                "model_type": "ner",
                "entities": entities,
                "entity_count": len(entities),
                "inference_time_ms": 31,
            }

        servers[2].register_command(
            "analyze_sentiment", analyze_sentiment, ["nlp", "sentiment"]
        )
        servers[2].register_command(
            "extract_entities", extract_entities, ["nlp", "text-analysis"]
        )

        # Feature processing functions
        def preprocess_image(request: dict) -> dict:
            """Image preprocessing for vision models."""
            return {
                **request,
                "preprocessed": True,
                "image_size": [224, 224, 3],
                "normalization": "imagenet",
                "preprocessing_time_ms": 12,
            }

        def extract_text_features(request: dict) -> dict:
            """Text feature extraction."""
            text = request.get("input_data", "")

            # Mock feature extraction
            features = {
                "word_count": len(text.split()),
                "char_count": len(text),
                "sentence_count": text.count(".") + text.count("!") + text.count("?"),
                "avg_word_length": sum(len(word) for word in text.split())
                / len(text.split())
                if text.split()
                else 0,
            }

            return {
                **request,
                "text_features": features,
                "feature_extraction_time_ms": 8,
            }

        servers[3].register_command(
            "preprocess_image",
            preprocess_image,
            ["preprocessing", "feature-extraction"],
        )
        servers[3].register_command(
            "extract_text_features",
            extract_text_features,
            ["preprocessing", "embeddings"],
        )

    async def run_ml_inference_example(self):
        """Demonstrate distributed ML inference."""
        print("🤖 Setting up distributed ML inference cluster...")
        servers = await self.setup_ml_cluster()

        try:
            async with MPREGClientAPI("ws://127.0.0.1:9001") as client:
                print("🔍 Running ML inference examples...")

                # Vision inference pipeline
                print("   📸 Image Classification Pipeline:")
                vision_result = await client._client.request(
                    [
                        RPCCommand(
                            name="routed_vision",
                            fun="route_request",
                            args=("image", "resnet50", "cat_photo.jpg"),
                            locs=frozenset(["routing"]),
                        ),
                        RPCCommand(
                            name="preprocessed_image",
                            fun="preprocess_image",
                            args=("routed_vision",),
                            locs=frozenset(["preprocessing", "feature-extraction"]),
                        ),
                        RPCCommand(
                            name="classified",
                            fun="classify_image",
                            args=("preprocessed_image",),
                            locs=frozenset(["vision", "image-classification"]),
                        ),
                    ]
                )

                classification = vision_result["classified"]
                top_pred = classification["top_prediction"]
                print(
                    f"      🎯 Prediction: {top_pred[0]} (confidence: {top_pred[1]:.2f})"
                )
                print(
                    f"      ⏱️  Total inference time: {classification['inference_time_ms']}ms"
                )

                # NLP inference pipeline
                print("\n   📝 Text Analysis Pipeline:")
                text_sample = "This product is absolutely amazing! Great quality and fantastic value."

                nlp_result = await client._client.request(
                    [
                        RPCCommand(
                            name="routed_text",
                            fun="route_request",
                            args=("text", "bert-sentiment", text_sample),
                            locs=frozenset(["routing"]),
                        ),
                        RPCCommand(
                            name="text_features",
                            fun="extract_text_features",
                            args=("routed_text",),
                            locs=frozenset(["preprocessing", "embeddings"]),
                        ),
                        RPCCommand(
                            name="sentiment_analyzed",
                            fun="analyze_sentiment",
                            args=("text_features",),
                            locs=frozenset(["nlp", "sentiment"]),
                        ),
                        RPCCommand(
                            name="entities_extracted",
                            fun="extract_entities",
                            args=("text_features",),
                            locs=frozenset(["nlp", "text-analysis"]),
                        ),
                    ]
                )

                sentiment = nlp_result["sentiment_analyzed"]
                entities = nlp_result["entities_extracted"]
                print(
                    f"      😊 Sentiment: {sentiment['sentiment']} (confidence: {sentiment['confidence_score']:.2f})"
                )
                print(f"      🏷️  Entities found: {entities['entity_count']}")

                # Parallel inference for multiple models
                print("\n   ⚡ Parallel Multi-Model Inference:")
                parallel_tasks = [
                    client.call(
                        "classify_image",
                        {"model_name": "mobilenet", "input_data": "dog_photo.jpg"},
                        locs=frozenset(["vision", "image-classification"]),
                    ),
                    client.call(
                        "detect_objects",
                        {"model_name": "yolo", "input_data": "street_scene.jpg"},
                        locs=frozenset(["vision", "object-detection"]),
                    ),
                    client.call(
                        "analyze_sentiment",
                        {"input_data": "The service was terrible and disappointing."},
                        locs=frozenset(["nlp", "sentiment"]),
                    ),
                ]

                start_time = time.time()
                parallel_results = await asyncio.gather(*parallel_tasks)
                parallel_time = time.time() - start_time

                print(f"      🏃 Processed 3 different models in {parallel_time:.3f}s")
                print(f"      📊 Average per-model time: {parallel_time / 3:.3f}s")

                print("\n🎯 ML Inference Demonstration Complete!")
                print("✨ This showcases MPREG's ability to:")
                print("   • Route inference requests to specialized model nodes")
                print("   • Handle complex ML pipelines with preprocessing steps")
                print("   • Execute multiple models in parallel across the cluster")
                print("   • Provide sub-100ms inference times for most models")

                return {
                    "vision": vision_result,
                    "nlp": nlp_result,
                    "parallel": parallel_results,
                }

        finally:
            print("\n🧹 Shutting down ML cluster...")
            for server in servers:
                if hasattr(server, "_shutdown_event"):
                    server._shutdown_event.set()
            await asyncio.sleep(0.5)


async def main():
    """Run all real-world examples."""
    print("🌟 MPREG Real-World Examples Showcase")
    print("=" * 50)

    # Data Pipeline Example
    print("\n📊 Example 1: Real-Time Data Processing Pipeline")
    print("-" * 50)
    pipeline = DataPipelineExample()
    await pipeline.run_pipeline_example()

    # ML Inference Example
    print("\n🤖 Example 2: Distributed ML Inference")
    print("-" * 50)
    ml_inference = MLInferenceExample()
    await ml_inference.run_ml_inference_example()

    print("\n" + "=" * 50)
    print("🎉 All examples completed successfully!")
    print("\n🚀 What makes MPREG unique:")
    print("   ✅ Automatic function routing based on resources")
    print("   ✅ Late-binding dependency resolution")
    print("   ✅ Zero-configuration cluster formation")
    print("   ✅ Sub-millisecond local function calls")
    print("   ✅ Transparent distributed computing")
    print("   ✅ Self-managing component architecture")
    print("\n💡 Ready to build your own distributed applications with MPREG!")


if __name__ == "__main__":
    asyncio.run(main())
