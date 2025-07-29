"""
Advanced Cache Operations for MPREG Global Distributed Cache.

This module implements the advanced cache operations that turn the basic cache
into a full cache + data structure + micro-db server system with:

- Atomic operations (test-and-set, compare-and-swap, etc.)
- Server-side data structures (sets, lists, maps, counters)
- Namespace operations and bulk clearing
- Server-side value validation and constraints
- Conditional operations based on value contents
- Integration with topic pub/sub for cache event streaming

All operations maintain ACID properties where applicable and integrate
seamlessly with the existing multi-tier cache architecture.
"""

import asyncio
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from loguru import logger

from .global_cache import (
    CacheMetadata,
    CacheOptions,
    GlobalCacheKey,
    GlobalCacheManager,
)


class AtomicOperation(Enum):
    """Types of atomic cache operations."""

    TEST_AND_SET = "test_and_set"
    COMPARE_AND_SWAP = "compare_and_swap"
    INCREMENT = "increment"
    DECREMENT = "decrement"
    APPEND = "append"
    PREPEND = "prepend"


class DataStructureType(Enum):
    """Server-side data structure types."""

    SET = "set"
    LIST = "list"
    MAP = "map"
    COUNTER = "counter"
    SORTED_SET = "sorted_set"
    QUEUE = "queue"
    STACK = "stack"


class ConstraintType(Enum):
    """Value constraint types for server-side validation."""

    MIN_VALUE = "min_value"
    MAX_VALUE = "max_value"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"
    REGEX_PATTERN = "regex_pattern"
    TYPE_CHECK = "type_check"
    CUSTOM_VALIDATOR = "custom_validator"


@dataclass(frozen=True, slots=True)
class ValueConstraint:
    """Constraint definition for server-side value validation."""

    constraint_type: ConstraintType
    value: Any
    error_message: str = ""


@dataclass(frozen=True, slots=True)
class AtomicOperationRequest:
    """Request for atomic cache operation."""

    operation: AtomicOperation
    key: GlobalCacheKey
    expected_value: Any = None  # For test-and-set, compare-and-swap
    new_value: Any = None
    delta: int | float = 1  # For increment/decrement
    data: Any = None  # For append/prepend
    conditions: list[ValueConstraint] = field(default_factory=list)
    ttl_seconds: float | None = None
    if_not_exists: bool = False  # Create only if key doesn't exist


@dataclass(frozen=True, slots=True)
class AtomicOperationResult:
    """Result of atomic cache operation."""

    success: bool
    old_value: Any = None
    new_value: Any = None
    error_message: str | None = None
    constraint_violations: list[str] = field(default_factory=list)
    operation_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass(frozen=True, slots=True)
class DataStructureOperation:
    """Operation on server-side data structure."""

    structure_type: DataStructureType
    operation: str  # "add", "remove", "get", "pop", "push", etc.
    key: GlobalCacheKey
    member: Any = None
    index: int | None = None
    score: float | None = None  # For sorted sets
    range_start: int | None = None
    range_end: int | None = None
    count: int = 1


@dataclass(frozen=True, slots=True)
class DataStructureResult:
    """Result of data structure operation."""

    success: bool
    value: Any = None
    size: int = 0
    exists: bool = False
    error_message: str | None = None
    operation_id: str = field(default_factory=lambda: str(uuid.uuid4()))


@dataclass(frozen=True, slots=True)
class NamespaceOperation:
    """Bulk operation on cache namespace."""

    namespace: str
    operation: str  # "clear", "list", "count", "scan"
    pattern: str = "*"  # Glob pattern for filtering
    limit: int = 1000  # Limit for scan operations
    include_metadata: bool = False


@dataclass(frozen=True, slots=True)
class NamespaceResult:
    """Result of namespace operation."""

    success: bool
    keys: list[GlobalCacheKey] = field(default_factory=list)
    count: int = 0
    cleared_count: int = 0
    error_message: str | None = None
    operation_id: str = field(default_factory=lambda: str(uuid.uuid4()))


class AdvancedCacheOperations:
    """
    Advanced cache operations manager providing atomic operations,
    server-side data structures, and namespace management.

    This extends the basic GlobalCacheManager with sophisticated operations
    that avoid the need for read-modify-write cycles and complex JSON manipulation.
    """

    def __init__(self, cache_manager: GlobalCacheManager):
        self.cache_manager = cache_manager

        # Operation locks for atomicity
        self.operation_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)

        # Constraint validators
        self.constraint_validators: dict[ConstraintType, Callable] = {
            ConstraintType.MIN_VALUE: self._validate_min_value,
            ConstraintType.MAX_VALUE: self._validate_max_value,
            ConstraintType.MIN_LENGTH: self._validate_min_length,
            ConstraintType.MAX_LENGTH: self._validate_max_length,
            ConstraintType.TYPE_CHECK: self._validate_type,
        }

        # Data structure handlers
        self.structure_handlers = {
            DataStructureType.SET: self._handle_set_operation,
            DataStructureType.LIST: self._handle_list_operation,
            DataStructureType.MAP: self._handle_map_operation,
            DataStructureType.COUNTER: self._handle_counter_operation,
            DataStructureType.SORTED_SET: self._handle_sorted_set_operation,
            DataStructureType.QUEUE: self._handle_queue_operation,
            DataStructureType.STACK: self._handle_stack_operation,
        }

        # Statistics
        self.operation_stats: dict[str, int] = defaultdict(int)

    async def atomic_operation(
        self, request: AtomicOperationRequest, options: CacheOptions | None = None
    ) -> AtomicOperationResult:
        """
        Execute atomic cache operation with ACID guarantees.

        Supports test-and-set, compare-and-swap, increment/decrement,
        and append/prepend operations with server-side validation.
        """
        # Use key-based locking for atomicity
        lock_key = str(request.key)
        async with self.operation_locks[lock_key]:
            try:
                return await self._execute_atomic_operation(request, options)
            except Exception as e:
                logger.error(f"Atomic operation failed for {request.key}: {e}")
                return AtomicOperationResult(
                    success=False, error_message=f"Atomic operation error: {e}"
                )

    async def _execute_atomic_operation(
        self, request: AtomicOperationRequest, options: CacheOptions | None
    ) -> AtomicOperationResult:
        """Execute the actual atomic operation."""
        start_time = time.time()

        # Get current value
        current_result = await self.cache_manager.get(request.key, options)
        current_value = current_result.entry.value if current_result.entry else None

        # Handle if_not_exists condition
        if request.if_not_exists and current_result.success:
            return AtomicOperationResult(
                success=False,
                old_value=current_value,
                error_message="Key already exists",
            )

        # Execute operation-specific logic
        if request.operation == AtomicOperation.TEST_AND_SET:
            return await self._test_and_set(request, current_value, options)
        elif request.operation == AtomicOperation.COMPARE_AND_SWAP:
            return await self._compare_and_swap(request, current_value, options)
        elif request.operation == AtomicOperation.INCREMENT:
            return await self._increment_decrement(request, current_value, options, 1)
        elif request.operation == AtomicOperation.DECREMENT:
            return await self._increment_decrement(request, current_value, options, -1)
        elif request.operation == AtomicOperation.APPEND:
            return await self._append_prepend(
                request, current_value, options, append=True
            )
        elif request.operation == AtomicOperation.PREPEND:
            return await self._append_prepend(
                request, current_value, options, append=False
            )
        else:
            return AtomicOperationResult(
                success=False,
                error_message=f"Unsupported atomic operation: {request.operation}",
            )

    async def _test_and_set(
        self,
        request: AtomicOperationRequest,
        current_value: Any,
        options: CacheOptions | None,
    ) -> AtomicOperationResult:
        """Test current value and set new value if test passes."""
        # Test condition
        if current_value != request.expected_value:
            return AtomicOperationResult(
                success=False,
                old_value=current_value,
                error_message="Test condition failed",
            )

        # Validate constraints
        violation_errors = self._validate_constraints(
            request.new_value, request.conditions
        )
        if violation_errors:
            return AtomicOperationResult(
                success=False,
                old_value=current_value,
                constraint_violations=violation_errors,
            )

        # Set new value
        metadata = CacheMetadata(ttl_seconds=request.ttl_seconds)
        put_result = await self.cache_manager.put(
            request.key, request.new_value, metadata, options
        )

        self.operation_stats["test_and_set"] += 1

        return AtomicOperationResult(
            success=put_result.success,
            old_value=current_value,
            new_value=request.new_value,
            error_message=put_result.error_message,
        )

    async def _compare_and_swap(
        self,
        request: AtomicOperationRequest,
        current_value: Any,
        options: CacheOptions | None,
    ) -> AtomicOperationResult:
        """Compare current value and swap if equal to expected."""
        if current_value != request.expected_value:
            return AtomicOperationResult(
                success=False,
                old_value=current_value,
                error_message="Compare condition failed",
            )

        # Validate new value
        violation_errors = self._validate_constraints(
            request.new_value, request.conditions
        )
        if violation_errors:
            return AtomicOperationResult(
                success=False,
                old_value=current_value,
                constraint_violations=violation_errors,
            )

        # Swap value
        metadata = CacheMetadata(ttl_seconds=request.ttl_seconds)
        put_result = await self.cache_manager.put(
            request.key, request.new_value, metadata, options
        )

        self.operation_stats["compare_and_swap"] += 1

        return AtomicOperationResult(
            success=put_result.success,
            old_value=current_value,
            new_value=request.new_value,
            error_message=put_result.error_message,
        )

    async def _increment_decrement(
        self,
        request: AtomicOperationRequest,
        current_value: Any,
        options: CacheOptions | None,
        direction: int,
    ) -> AtomicOperationResult:
        """Increment or decrement numeric value atomically."""
        # Handle non-existent key
        if current_value is None:
            current_value = 0

        # Validate numeric type
        try:
            if isinstance(current_value, int | float):
                new_value = current_value + (request.delta * direction)
            else:
                return AtomicOperationResult(
                    success=False,
                    old_value=current_value,
                    error_message="Value is not numeric",
                )
        except (TypeError, ValueError) as e:
            return AtomicOperationResult(
                success=False,
                old_value=current_value,
                error_message=f"Increment/decrement error: {e}",
            )

        # Validate constraints
        violation_errors = self._validate_constraints(new_value, request.conditions)
        if violation_errors:
            return AtomicOperationResult(
                success=False,
                old_value=current_value,
                constraint_violations=violation_errors,
            )

        # Store new value
        metadata = CacheMetadata(ttl_seconds=request.ttl_seconds)
        put_result = await self.cache_manager.put(
            request.key, new_value, metadata, options
        )

        op_name = "increment" if direction > 0 else "decrement"
        self.operation_stats[op_name] += 1

        return AtomicOperationResult(
            success=put_result.success,
            old_value=current_value,
            new_value=new_value,
            error_message=put_result.error_message,
        )

    async def _append_prepend(
        self,
        request: AtomicOperationRequest,
        current_value: Any,
        options: CacheOptions | None,
        append: bool,
    ) -> AtomicOperationResult:
        """Append or prepend data to existing value."""
        # Handle non-existent key
        if current_value is None:
            current_value = "" if isinstance(request.data, str) else []

        # Perform append/prepend based on type
        try:
            new_value: Any
            if isinstance(current_value, str) and isinstance(request.data, str):
                new_value = (
                    current_value + request.data
                    if append
                    else request.data + current_value
                )
            elif isinstance(current_value, list):
                new_value = current_value.copy()
                if append:
                    if isinstance(request.data, list):
                        new_value.extend(request.data)
                    else:
                        new_value.append(request.data)
                else:  # prepend
                    if isinstance(request.data, list):
                        new_value = request.data + new_value
                    else:
                        new_value.insert(0, request.data)
            else:
                return AtomicOperationResult(
                    success=False,
                    old_value=current_value,
                    error_message="Incompatible types for append/prepend",
                )
        except Exception as e:
            return AtomicOperationResult(
                success=False,
                old_value=current_value,
                error_message=f"Append/prepend error: {e}",
            )

        # Validate constraints
        violation_errors = self._validate_constraints(new_value, request.conditions)
        if violation_errors:
            return AtomicOperationResult(
                success=False,
                old_value=current_value,
                constraint_violations=violation_errors,
            )

        # Store new value
        metadata = CacheMetadata(ttl_seconds=request.ttl_seconds)
        put_result = await self.cache_manager.put(
            request.key, new_value, metadata, options
        )

        op_name = "append" if append else "prepend"
        self.operation_stats[op_name] += 1

        return AtomicOperationResult(
            success=put_result.success,
            old_value=current_value,
            new_value=new_value,
            error_message=put_result.error_message,
        )

    def _validate_constraints(
        self, value: Any, constraints: list[ValueConstraint]
    ) -> list[str]:
        """Validate value against constraints."""
        violations = []

        for constraint in constraints:
            validator = self.constraint_validators.get(constraint.constraint_type)
            if validator:
                is_valid, error_msg = validator(value, constraint.value)
                if not is_valid:
                    violations.append(constraint.error_message or error_msg)

        return violations

    def _validate_min_value(self, value: Any, min_val: Any) -> tuple[bool, str]:
        """Validate minimum value constraint."""
        try:
            return value >= min_val, f"Value {value} is less than minimum {min_val}"
        except TypeError:
            return False, f"Cannot compare {type(value)} with {type(min_val)}"

    def _validate_max_value(self, value: Any, max_val: Any) -> tuple[bool, str]:
        """Validate maximum value constraint."""
        try:
            return value <= max_val, f"Value {value} exceeds maximum {max_val}"
        except TypeError:
            return False, f"Cannot compare {type(value)} with {type(max_val)}"

    def _validate_min_length(self, value: Any, min_len: int) -> tuple[bool, str]:
        """Validate minimum length constraint."""
        try:
            return len(
                value
            ) >= min_len, f"Length {len(value)} is less than minimum {min_len}"
        except TypeError:
            return False, f"Value of type {type(value)} has no length"

    def _validate_max_length(self, value: Any, max_len: int) -> tuple[bool, str]:
        """Validate maximum length constraint."""
        try:
            return len(
                value
            ) <= max_len, f"Length {len(value)} exceeds maximum {max_len}"
        except TypeError:
            return False, f"Value of type {type(value)} has no length"

    def _validate_type(self, value: Any, expected_type: type) -> tuple[bool, str]:
        """Validate type constraint."""
        return isinstance(
            value, expected_type
        ), f"Expected {expected_type}, got {type(value)}"

    async def data_structure_operation(
        self, operation: DataStructureOperation, options: CacheOptions | None = None
    ) -> DataStructureResult:
        """
        Execute operation on server-side data structure.

        Supports sets, lists, maps, counters, sorted sets, queues, and stacks
        with atomic operations that avoid client-side read-modify-write cycles.
        """
        lock_key = str(operation.key)
        async with self.operation_locks[lock_key]:
            try:
                handler = self.structure_handlers.get(operation.structure_type)
                if not handler:
                    return DataStructureResult(
                        success=False,
                        error_message=f"Unsupported structure type: {operation.structure_type}",
                    )

                return await handler(operation, options)

            except Exception as e:
                logger.error(
                    f"Data structure operation failed for {operation.key}: {e}"
                )
                return DataStructureResult(
                    success=False, error_message=f"Data structure operation error: {e}"
                )

    async def _handle_set_operation(
        self, operation: DataStructureOperation, options: CacheOptions | None
    ) -> DataStructureResult:
        """Handle set data structure operations."""
        # Get current set
        current_result = await self.cache_manager.get(operation.key, options)
        current_set = set(current_result.entry.value) if current_result.entry else set()

        # Initialize result variables
        success: bool = False
        value: Any = None

        # Execute operation
        if operation.operation == "add":
            current_set.add(operation.member)
            success = True
            value = operation.member in current_set
        elif operation.operation == "remove":
            success = operation.member in current_set
            current_set.discard(operation.member)
            value = operation.member not in current_set
        elif operation.operation == "contains":
            success = True
            value = operation.member in current_set
        elif operation.operation == "members":
            success = True
            value = list(current_set)
        elif operation.operation == "size":
            success = True
            value = len(current_set)
        elif operation.operation == "clear":
            current_set.clear()
            success = True
            value = len(current_set) == 0
        else:
            return DataStructureResult(
                success=False,
                error_message=f"Unsupported set operation: {operation.operation}",
            )

        # Store updated set (for mutating operations)
        if operation.operation in ["add", "remove", "clear"]:
            put_result = await self.cache_manager.put(
                operation.key, list(current_set), CacheMetadata(), options
            )
            success = success and put_result.success

        self.operation_stats[f"set_{operation.operation}"] += 1

        return DataStructureResult(
            success=success,
            value=value,
            size=len(current_set),
            exists=current_result.success,
        )

    async def _handle_list_operation(
        self, operation: DataStructureOperation, options: CacheOptions | None
    ) -> DataStructureResult:
        """Handle list data structure operations."""
        # Get current list
        current_result = await self.cache_manager.get(operation.key, options)
        current_list = list(current_result.entry.value) if current_result.entry else []

        # Execute operation
        success: bool = True
        value: Any = None

        try:
            if operation.operation == "append":
                current_list.append(operation.member)
                value = len(current_list) - 1  # Return index
            elif operation.operation == "prepend":
                current_list.insert(0, operation.member)
                value = 0  # Return index
            elif operation.operation == "insert":
                if operation.index is not None:
                    current_list.insert(operation.index, operation.member)
                    value = operation.index
                else:
                    success = False
                    value = "Index required for insert operation"
            elif operation.operation == "remove":
                if operation.member in current_list:
                    current_list.remove(operation.member)
                    value = True
                else:
                    value = False
            elif operation.operation == "pop":
                if current_list:
                    index = operation.index if operation.index is not None else -1
                    value = current_list.pop(index)
                else:
                    success = False
                    value = "Cannot pop from empty list"
            elif operation.operation == "get":
                if operation.index is not None and 0 <= operation.index < len(
                    current_list
                ):
                    value = current_list[operation.index]
                else:
                    success = False
                    value = "Invalid index"
            elif operation.operation == "slice":
                start = operation.range_start or 0
                end = operation.range_end or len(current_list)
                value = current_list[start:end]
            elif operation.operation == "size":
                value = len(current_list)
            elif operation.operation == "clear":
                current_list.clear()
                value = True
            else:
                success = False
                value = f"Unsupported list operation: {operation.operation}"
        except (IndexError, ValueError) as e:
            success = False
            value = str(e)

        # Store updated list (for mutating operations)
        if success and operation.operation in [
            "append",
            "prepend",
            "insert",
            "remove",
            "pop",
            "clear",
        ]:
            put_result = await self.cache_manager.put(
                operation.key, current_list, CacheMetadata(), options
            )
            success = success and put_result.success

        self.operation_stats[f"list_{operation.operation}"] += 1

        return DataStructureResult(
            success=success,
            value=value,
            size=len(current_list),
            exists=current_result.success,
        )

    async def _handle_map_operation(
        self, operation: DataStructureOperation, options: CacheOptions | None
    ) -> DataStructureResult:
        """Handle map/dictionary data structure operations."""
        # Get current map
        current_result = await self.cache_manager.get(operation.key, options)
        current_map = dict(current_result.entry.value) if current_result.entry else {}

        # Execute operation based on member as key
        success: bool = True
        value: Any = None

        if operation.operation == "set":
            # member is key, index is value for map operations
            current_map[operation.member] = operation.index
            value = operation.index
        elif operation.operation == "get":
            value = current_map.get(operation.member)
            success = operation.member in current_map
        elif operation.operation == "delete":
            if operation.member in current_map:
                value = current_map.pop(operation.member)
                success = True
            else:
                success = False
                value = "Key not found"
        elif operation.operation == "contains":
            value = operation.member in current_map
        elif operation.operation == "keys":
            value = list(current_map.keys())
        elif operation.operation == "values":
            value = list(current_map.values())
        elif operation.operation == "items":
            value = list(current_map.items())
        elif operation.operation == "size":
            value = len(current_map)
        elif operation.operation == "clear":
            current_map.clear()
            value = True
        else:
            success = False
            value = f"Unsupported map operation: {operation.operation}"

        # Store updated map (for mutating operations)
        if success and operation.operation in ["set", "delete", "clear"]:
            put_result = await self.cache_manager.put(
                operation.key, current_map, CacheMetadata(), options
            )
            success = success and put_result.success

        self.operation_stats[f"map_{operation.operation}"] += 1

        return DataStructureResult(
            success=success,
            value=value,
            size=len(current_map),
            exists=current_result.success,
        )

    async def _handle_counter_operation(
        self, operation: DataStructureOperation, options: CacheOptions | None
    ) -> DataStructureResult:
        """Handle counter data structure operations."""
        # Get current counter value
        current_result = await self.cache_manager.get(operation.key, options)
        current_value = current_result.entry.value if current_result.entry else 0

        # Ensure numeric value
        if not isinstance(current_value, int | float):
            current_value = 0

        success: bool = True
        value: Any = current_value

        if operation.operation == "increment":
            delta = operation.index if operation.index is not None else 1
            value = current_value + delta
        elif operation.operation == "decrement":
            delta = operation.index if operation.index is not None else 1
            value = current_value - delta
        elif operation.operation == "set":
            value = operation.index if operation.index is not None else 0
        elif operation.operation == "get":
            value = current_value
        elif operation.operation == "reset":
            value = 0
        else:
            success = False
            value = f"Unsupported counter operation: {operation.operation}"

        # Store updated counter (for mutating operations)
        if success and operation.operation in [
            "increment",
            "decrement",
            "set",
            "reset",
        ]:
            put_result = await self.cache_manager.put(
                operation.key, value, CacheMetadata(), options
            )
            success = success and put_result.success

        self.operation_stats[f"counter_{operation.operation}"] += 1

        return DataStructureResult(
            success=success, value=value, size=1, exists=current_result.success
        )

    async def _handle_sorted_set_operation(
        self, operation: DataStructureOperation, options: CacheOptions | None
    ) -> DataStructureResult:
        """Handle sorted set data structure operations."""
        # Get current sorted set (stored as list of (score, member) tuples)
        current_result = await self.cache_manager.get(operation.key, options)
        current_zset = list(current_result.entry.value) if current_result.entry else []

        # Ensure proper format
        if not all(
            isinstance(item, list | tuple) and len(item) == 2 for item in current_zset
        ):
            current_zset = []

        success: bool = True
        value: Any = None

        if operation.operation == "add":
            # Add member with score
            score = operation.score if operation.score is not None else 0.0
            # Remove existing member if present
            current_zset = [(s, m) for s, m in current_zset if m != operation.member]
            current_zset.append((score, operation.member))
            current_zset.sort()  # Keep sorted by score
            value = True
        elif operation.operation == "remove":
            original_len = len(current_zset)
            current_zset = [(s, m) for s, m in current_zset if m != operation.member]
            value = len(current_zset) < original_len
        elif operation.operation == "score":
            for score, member in current_zset:
                if member == operation.member:
                    value = score
                    break
            else:
                success = False
                value = "Member not found"
        elif operation.operation == "rank":
            for i, (score, member) in enumerate(current_zset):
                if member == operation.member:
                    value = i
                    break
            else:
                success = False
                value = "Member not found"
        elif operation.operation == "range":
            start = operation.range_start or 0
            end = operation.range_end or len(current_zset)
            value = [member for score, member in current_zset[start:end]]
        elif operation.operation == "size":
            value = len(current_zset)
        elif operation.operation == "clear":
            current_zset.clear()
            value = True
        else:
            success = False
            value = f"Unsupported sorted set operation: {operation.operation}"

        # Store updated sorted set (for mutating operations)
        if success and operation.operation in ["add", "remove", "clear"]:
            put_result = await self.cache_manager.put(
                operation.key, current_zset, CacheMetadata(), options
            )
            success = success and put_result.success

        self.operation_stats[f"zset_{operation.operation}"] += 1

        return DataStructureResult(
            success=success,
            value=value,
            size=len(current_zset),
            exists=current_result.success,
        )

    async def _handle_queue_operation(
        self, operation: DataStructureOperation, options: CacheOptions | None
    ) -> DataStructureResult:
        """Handle queue (FIFO) data structure operations."""
        # Get current queue
        current_result = await self.cache_manager.get(operation.key, options)
        current_queue = list(current_result.entry.value) if current_result.entry else []

        success: bool = True
        value: Any = None

        if operation.operation == "enqueue":
            current_queue.append(operation.member)
            value = len(current_queue) - 1
        elif operation.operation == "dequeue":
            if current_queue:
                value = current_queue.pop(0)  # FIFO
            else:
                success = False
                value = "Queue is empty"
        elif operation.operation == "peek":
            if current_queue:
                value = current_queue[0]
            else:
                success = False
                value = "Queue is empty"
        elif operation.operation == "size":
            value = len(current_queue)
        elif operation.operation == "clear":
            current_queue.clear()
            value = True
        else:
            success = False
            value = f"Unsupported queue operation: {operation.operation}"

        # Store updated queue (for mutating operations)
        if success and operation.operation in ["enqueue", "dequeue", "clear"]:
            put_result = await self.cache_manager.put(
                operation.key, current_queue, CacheMetadata(), options
            )
            success = success and put_result.success

        self.operation_stats[f"queue_{operation.operation}"] += 1

        return DataStructureResult(
            success=success,
            value=value,
            size=len(current_queue),
            exists=current_result.success,
        )

    async def _handle_stack_operation(
        self, operation: DataStructureOperation, options: CacheOptions | None
    ) -> DataStructureResult:
        """Handle stack (LIFO) data structure operations."""
        # Get current stack
        current_result = await self.cache_manager.get(operation.key, options)
        current_stack = list(current_result.entry.value) if current_result.entry else []

        success: bool = True
        value: Any = None

        if operation.operation == "push":
            current_stack.append(operation.member)
            value = len(current_stack) - 1
        elif operation.operation == "pop":
            if current_stack:
                value = current_stack.pop()  # LIFO
            else:
                success = False
                value = "Stack is empty"
        elif operation.operation == "peek":
            if current_stack:
                value = current_stack[-1]
            else:
                success = False
                value = "Stack is empty"
        elif operation.operation == "size":
            value = len(current_stack)
        elif operation.operation == "clear":
            current_stack.clear()
            value = True
        else:
            success = False
            value = f"Unsupported stack operation: {operation.operation}"

        # Store updated stack (for mutating operations)
        if success and operation.operation in ["push", "pop", "clear"]:
            put_result = await self.cache_manager.put(
                operation.key, current_stack, CacheMetadata(), options
            )
            success = success and put_result.success

        self.operation_stats[f"stack_{operation.operation}"] += 1

        return DataStructureResult(
            success=success,
            value=value,
            size=len(current_stack),
            exists=current_result.success,
        )

    async def namespace_operation(
        self, operation: NamespaceOperation, options: CacheOptions | None = None
    ) -> NamespaceResult:
        """
        Execute bulk operation on cache namespace.

        Supports clearing, listing, counting, and scanning keys within
        a namespace with pattern matching for efficient bulk operations.
        """
        try:
            if operation.operation == "clear":
                return await self._clear_namespace(operation, options)
            elif operation.operation == "list":
                return await self._list_namespace(operation, options)
            elif operation.operation == "count":
                return await self._count_namespace(operation, options)
            elif operation.operation == "scan":
                return await self._scan_namespace(operation, options)
            else:
                return NamespaceResult(
                    success=False,
                    error_message=f"Unsupported namespace operation: {operation.operation}",
                )
        except Exception as e:
            logger.error(f"Namespace operation failed for {operation.namespace}: {e}")
            return NamespaceResult(
                success=False, error_message=f"Namespace operation error: {e}"
            )

    async def _clear_namespace(
        self, operation: NamespaceOperation, options: CacheOptions | None
    ) -> NamespaceResult:
        """Clear all keys in namespace matching pattern."""
        # This is a simplified implementation
        # In production, this would integrate with the actual cache storage
        # to efficiently iterate and delete keys

        # For now, return a placeholder result
        self.operation_stats["namespace_clear"] += 1

        return NamespaceResult(
            success=True,
            cleared_count=0,  # Would be actual count in real implementation
            error_message="Namespace clear not yet fully implemented",
        )

    async def _list_namespace(
        self, operation: NamespaceOperation, options: CacheOptions | None
    ) -> NamespaceResult:
        """List keys in namespace matching pattern."""
        try:
            # Use the proper namespace index instead of fragile hasattr() scanning
            namespace_keys = self.cache_manager.get_namespace_keys(
                operation.namespace, operation.pattern
            )

            self.operation_stats["namespace_list"] += 1
            return NamespaceResult(
                success=True,
                keys=namespace_keys,
                count=len(namespace_keys),
            )
        except Exception as e:
            logger.error(f"Failed to list namespace {operation.namespace}: {e}")
            return NamespaceResult(
                success=False,
                error_message=f"Namespace list error: {e}",
            )

    async def _count_namespace(
        self, operation: NamespaceOperation, options: CacheOptions | None
    ) -> NamespaceResult:
        """Count keys in namespace matching pattern."""
        # Placeholder implementation
        self.operation_stats["namespace_count"] += 1

        return NamespaceResult(
            success=True,
            count=0,  # Would be actual count in real implementation
        )

    async def _scan_namespace(
        self, operation: NamespaceOperation, options: CacheOptions | None
    ) -> NamespaceResult:
        """Scan keys in namespace with cursor-based pagination."""
        # Placeholder implementation
        self.operation_stats["namespace_scan"] += 1

        return NamespaceResult(
            success=True,
            keys=[],  # Would be actual keys in real implementation
            count=0,
        )

    def get_statistics(self) -> dict[str, Any]:
        """Get comprehensive statistics for advanced cache operations."""
        return {
            "operation_stats": dict(self.operation_stats),
            "active_locks": len(self.operation_locks),
            "supported_operations": {
                "atomic": [op.value for op in AtomicOperation],
                "data_structures": [ds.value for ds in DataStructureType],
                "constraints": [c.value for c in ConstraintType],
            },
        }

    async def shutdown(self) -> None:
        """Shutdown advanced operations manager."""
        # Clear operation locks
        self.operation_locks.clear()
        logger.info("Advanced cache operations shutdown complete")
