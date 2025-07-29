# MPREG SQS-like Message Queue System

## Overview

MPREG includes a comprehensive SQS-like message queuing system that provides reliable, scalable message delivery with multiple delivery guarantees. The system is designed for distributed applications requiring robust message processing capabilities.

## Core Features

### 🎯 Delivery Guarantees

The system supports four distinct delivery patterns:

#### 1. Fire and Forget (`FIRE_AND_FORGET`)

- **Use case**: Notifications, logging, non-critical events
- **Behavior**: Send once, no tracking or retries
- **Performance**: Highest throughput, minimal overhead
- **Example**: User activity logging, metrics collection

```python
result = await queue.send_message(
    "notifications.user_login",
    {"user_id": 12345, "timestamp": time.time()},
    DeliveryGuarantee.FIRE_AND_FORGET
)
```

#### 2. At-Least-Once (`AT_LEAST_ONCE`)

- **Use case**: Critical operations requiring guaranteed delivery
- **Behavior**: Retry until acknowledged or max retries exceeded
- **Features**: Configurable timeout, retry limits, dead letter queue
- **Example**: Payment processing, order fulfillment

```python
result = await queue.send_message(
    "payments.process_order",
    {"order_id": "ORD-123", "amount": 99.99},
    DeliveryGuarantee.AT_LEAST_ONCE,
    max_retries=3,
    acknowledgment_timeout_seconds=300
)
```

#### 3. Broadcast (`BROADCAST`)

- **Use case**: System-wide notifications, cache invalidation
- **Behavior**: Deliver to all matching subscribers
- **Features**: Automatic acknowledgment from all subscribers
- **Example**: Configuration updates, system alerts

```python
result = await queue.send_message(
    "system.config_update",
    {"config_key": "feature_flags", "new_value": True},
    DeliveryGuarantee.BROADCAST
)
```

#### 4. Quorum (`QUORUM`)

- **Use case**: Distributed consensus, critical system changes
- **Behavior**: Require N acknowledgments before considering delivered
- **Features**: Configurable quorum size, consensus tracking
- **Example**: Cluster coordination, leadership election

```python
result = await queue.send_message(
    "cluster.leader_election",
    {"candidate_id": "node-5", "term": 42},
    DeliveryGuarantee.QUORUM,
    required_acknowledgments=3  # Need 3 out of 5 nodes
)
```

### 🏗️ Queue Types

#### FIFO Queues (`QueueType.FIFO`)

- **Ordering**: First-In-First-Out processing
- **Use case**: Sequential processing, ordered workflows
- **Example**: Transaction processing, event streaming

#### Priority Queues (`QueueType.PRIORITY`)

- **Ordering**: Higher priority messages processed first
- **Use case**: Task prioritization, SLA-sensitive operations
- **Configuration**: Set priority on message send (higher number = higher priority)

```python
# Critical alert (priority 15)
await queue.send_message("alerts.critical", alert_data, priority=15)

# Routine cleanup (priority 1)
await queue.send_message("maintenance.cleanup", cleanup_data, priority=1)
```

#### Delay Queues (`QueueType.DELAY`)

- **Ordering**: Messages delivered after specified delay
- **Use case**: Scheduled tasks, delayed notifications
- **Configuration**: Set delay_seconds on message send

```python
# Deliver in 5 minutes
await queue.send_message(
    "reminders.meeting",
    {"meeting_id": "MTG-456"},
    delay_seconds=300
)
```

### ⚡ Advanced Features

#### Message Deduplication

- **Purpose**: Prevent duplicate message processing
- **Implementation**: Sliding time window with O(log n) performance using SortedSet
- **Configuration**: Enable per queue with configurable window

```python
config = QueueConfiguration(
    name="orders-queue",
    enable_deduplication=True,
    deduplication_window_seconds=300  # 5 minute window
)
```

#### Dead Letter Queues (DLQ)

- **Purpose**: Handle messages that fail processing after max retries
- **Features**: Automatic DLQ routing, failure reason tracking
- **Configuration**: Enable per queue with retry limits

```python
config = QueueConfiguration(
    name="payments-queue",
    enable_dead_letter_queue=True,
    dead_letter_max_receives=3
)
```

#### Message TTL (Time-To-Live)

- **Purpose**: Automatic expiration of old messages
- **Features**: Configurable per queue, automatic cleanup
- **Use case**: Time-sensitive operations, resource management

```python
config = QueueConfiguration(
    name="realtime-events",
    message_ttl_seconds=60  # Messages expire after 1 minute
)
```

## Quick Start

### Basic Usage

```python
from mpreg.core.message_queue import MessageQueue, QueueConfiguration, DeliveryGuarantee
from mpreg.core.message_queue_manager import MessageQueueManager, QueueManagerConfiguration

# Create queue manager
config = QueueManagerConfiguration()
manager = MessageQueueManager(config)

# Create a queue
await manager.create_queue("my-queue")

# Subscribe to messages
def process_message(message):
    print(f"Processing: {message.payload}")

manager.subscribe_to_queue(
    "my-queue",
    "my-subscriber",
    "events.*",  # Topic pattern
    callback=process_message
)

# Send messages
result = await manager.send_message(
    "my-queue",
    "events.user_signup",
    {"user_id": 12345, "email": "user@example.com"},
    DeliveryGuarantee.AT_LEAST_ONCE
)

print(f"Message sent: {result.success}")
```

### Advanced Configuration

```python
# High-throughput queue configuration
from mpreg.core.message_queue_manager import create_high_throughput_queue_manager

manager = create_high_throughput_queue_manager()

# Custom queue with all features
config = QueueConfiguration(
    name="critical-operations",
    queue_type=QueueType.PRIORITY,
    max_size=50000,
    default_acknowledgment_timeout_seconds=60,
    enable_dead_letter_queue=True,
    enable_deduplication=True,
    deduplication_window_seconds=300,
    message_ttl_seconds=3600,
    max_retries=5
)

await manager.create_queue("critical-operations", config)
```

## Topic Patterns

The system supports flexible topic pattern matching for message routing:

### Exact Match

```python
# Subscribe to exact topic
queue.subscribe("subscriber-1", "user.login", callback)

# Will match: "user.login"
# Will NOT match: "user.logout", "admin.login"
```

### Wildcard Patterns

```python
# Subscribe to all user events
queue.subscribe("subscriber-2", "user.*", callback)

# Will match: "user.login", "user.logout", "user.profile_update"
# Will NOT match: "admin.login", "system.alert"

# Subscribe to all alerts
queue.subscribe("subscriber-3", "*.alert.*", callback)

# Will match: "system.alert.critical", "user.alert.warning"
# Will NOT match: "system.notification", "user.login"
```

## Performance and Scalability

### Optimized Data Structures

- **Deduplication**: SortedSet with O(log n) range operations for millions/billions of entries
- **Priority Queues**: Efficient insertion and retrieval maintaining priority order
- **Topic Matching**: Optimized pattern matching for high-throughput scenarios

### Monitoring and Statistics

```python
# Queue-specific statistics
stats = manager.get_queue_statistics("my-queue")
print(f"Messages processed: {stats.messages_acknowledged}")
print(f"Success rate: {stats.success_rate():.1%}")

# Global statistics across all queues
global_stats = manager.get_global_statistics()
print(f"Total queues: {global_stats.total_queues}")
print(f"Overall success rate: {global_stats.overall_success_rate():.1%}")
```

### Configuration Factories

The system provides pre-configured setups for common use cases:

```python
# Standard balanced configuration
manager = create_standard_queue_manager()

# High-throughput optimized
manager = create_high_throughput_queue_manager()

# Reliability-focused with strict guarantees
manager = create_reliable_queue_manager()
```

## Integration with MPREG

### Topic Exchange Integration

The message queue system integrates seamlessly with MPREG's topic exchange for pub/sub coordination:

```python
from mpreg.core.topic_exchange import TopicExchange

# Create with topic exchange integration
topic_exchange = TopicExchange()
manager = MessageQueueManager(config, topic_exchange)

# Route topic exchange messages to queues
await manager.route_topic_to_queue(
    "system.events.*",
    "system-queue",
    DeliveryGuarantee.BROADCAST
)
```

### Federation Support

Works with MPREG's federation system for distributed message processing across clusters.

## Error Handling

The system provides comprehensive error handling and recovery:

### Automatic Retries

- Configurable retry limits per message type
- Exponential backoff (future enhancement)
- Circuit breaker patterns for failing subscribers

### Dead Letter Queue Processing

```python
# Access dead letter queue for failed messages
dead_letters = queue.dead_letter_queue
for failed_message in dead_letters:
    print(f"Failed message: {failed_message.payload}")
    print(f"Failure reason: {failed_message.failure_reason}")
```

### Graceful Degradation

- Queue capacity limits with overflow handling
- Message TTL for automatic cleanup
- Graceful shutdown with task cleanup

## Best Practices

### Message Design

1. **Use structured payloads**: Prefer JSON-serializable dictionaries
2. **Include correlation IDs**: For distributed tracing
3. **Set appropriate TTLs**: Prevent resource leaks
4. **Choose delivery guarantees wisely**: Balance reliability vs performance

### Queue Configuration

1. **Size queues appropriately**: Consider memory usage vs latency
2. **Enable deduplication for critical paths**: Prevent duplicate processing
3. **Use priority queues sparingly**: Overhead vs benefit analysis
4. **Monitor queue depth**: Prevent unbounded growth

### Subscription Management

1. **Use specific topic patterns**: Avoid overly broad subscriptions
2. **Handle message processing errors**: Prevent subscription failures
3. **Clean up subscriptions**: Prevent resource leaks
4. **Use auto-acknowledgment carefully**: Balance convenience vs control

## Example: Complete E-commerce Order Processing

```python
async def setup_ecommerce_queues():
    """Complete e-commerce order processing setup."""

    manager = create_reliable_queue_manager()

    # High-priority payment processing
    payment_config = QueueConfiguration(
        name="payments",
        queue_type=QueueType.PRIORITY,
        enable_deduplication=True,
        deduplication_window_seconds=300,
        max_retries=5,
        default_acknowledgment_timeout_seconds=30
    )
    await manager.create_queue("payments", payment_config)

    # Reliable order fulfillment
    fulfillment_config = QueueConfiguration(
        name="fulfillment",
        queue_type=QueueType.FIFO,
        enable_dead_letter_queue=True,
        max_retries=3
    )
    await manager.create_queue("fulfillment", fulfillment_config)

    # Real-time notifications
    notification_config = QueueConfiguration(
        name="notifications",
        queue_type=QueueType.FIFO,
        message_ttl_seconds=300  # 5 minute TTL
    )
    await manager.create_queue("notifications", notification_config)

    # Set up subscribers
    manager.subscribe_to_queue("payments", "payment-processor", "payment.*")
    manager.subscribe_to_queue("fulfillment", "warehouse-system", "order.*")
    manager.subscribe_to_queue("notifications", "notification-service", "notify.*")

    return manager

# Usage
manager = await setup_ecommerce_queues()

# Process a new order
await manager.send_message(
    "payments",
    "payment.process",
    {"order_id": "ORD-123", "amount": 99.99, "card_token": "tok_123"},
    DeliveryGuarantee.AT_LEAST_ONCE,
    priority=10
)

# Send order to fulfillment
await manager.send_message(
    "fulfillment",
    "order.fulfill",
    {"order_id": "ORD-123", "items": [{"sku": "WIDGET-1", "qty": 2}]},
    DeliveryGuarantee.AT_LEAST_ONCE
)

# Send customer notification
await manager.send_message(
    "notifications",
    "notify.order_confirmed",
    {"customer_email": "customer@example.com", "order_id": "ORD-123"},
    DeliveryGuarantee.FIRE_AND_FORGET
)
```

## Running the Demo

Experience all features with the comprehensive demo:

```bash
poetry run mpreg-queue-demo
```

The demo showcases:

- All four delivery guarantees in action
- Different queue types (FIFO, Priority, Delay)
- Error handling and retry logic
- Performance monitoring and statistics
- Real-world usage patterns

## API Reference

See the comprehensive test suite in `tests/test_message_queue.py` for detailed API usage examples covering all features and edge cases.
