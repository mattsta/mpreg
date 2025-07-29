"""
Intermediate Results Support for MPREG RPC System

This module provides data structures and utilities for capturing and returning
intermediate results during RPC execution, enabling better debugging and monitoring
of complex distributed multi-hop dependency resolution pipelines.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from mpreg.core.model import RPCCommand, RPCRequest, RPCResponse


@dataclass(frozen=True, slots=True)
class RPCIntermediateResult:
    """Intermediate result from RPC execution level completion."""

    request_id: str
    level_index: int
    level_results: dict[str, Any]  # Results from this specific level
    accumulated_results: dict[str, Any]  # All results accumulated so far
    total_levels: int
    completed_levels: int
    timestamp: float = field(default_factory=time.time)
    execution_time_ms: float = 0.0

    @property
    def is_final_level(self) -> bool:
        """Check if this is the final execution level."""
        return self.completed_levels == self.total_levels

    @property
    def progress_percentage(self) -> float:
        """Calculate execution progress as percentage."""
        if self.total_levels == 0:
            return 100.0
        return (self.completed_levels / self.total_levels) * 100.0


@dataclass(frozen=True, slots=True)
class RPCExecutionSummary:
    """Summary of RPC execution performance and characteristics."""

    request_id: str
    total_commands: int
    total_levels: int
    total_execution_time_ms: float
    commands_per_level: list[int]
    level_execution_times_ms: list[float]
    dependency_graph_depth: int
    parallel_execution_efficiency: float  # Ratio of parallel vs sequential time

    @property
    def average_level_time_ms(self) -> float:
        """Calculate average execution time per level."""
        if not self.level_execution_times_ms:
            return 0.0
        return sum(self.level_execution_times_ms) / len(self.level_execution_times_ms)

    @property
    def slowest_level_index(self) -> int:
        """Get the index of the slowest execution level."""
        if not self.level_execution_times_ms:
            return -1
        return self.level_execution_times_ms.index(max(self.level_execution_times_ms))


@dataclass(slots=True)
class IntermediateResultCollector:
    """Collector for capturing intermediate results during RPC execution."""

    request_id: str
    total_levels: int
    intermediate_results: list[RPCIntermediateResult] = field(default_factory=list)
    level_start_times: dict[int, float] = field(default_factory=dict)

    def start_level(self, level_index: int) -> None:
        """Mark the start of execution level."""
        self.level_start_times[level_index] = time.time()

    def complete_level(
        self,
        level_index: int,
        level_results: dict[str, Any],
        accumulated_results: dict[str, Any],
    ) -> RPCIntermediateResult:
        """Record completion of an execution level."""
        current_time = time.time()

        # Calculate execution time for this level
        start_time = self.level_start_times.get(level_index, current_time)
        execution_time_ms = (current_time - start_time) * 1000.0

        # Create intermediate result
        intermediate_result = RPCIntermediateResult(
            request_id=self.request_id,
            level_index=level_index,
            level_results=dict(level_results),  # Copy to avoid mutations
            accumulated_results=dict(accumulated_results),  # Copy to avoid mutations
            total_levels=self.total_levels,
            completed_levels=level_index + 1,
            timestamp=current_time,
            execution_time_ms=execution_time_ms,
        )

        self.intermediate_results.append(intermediate_result)
        return intermediate_result

    def create_execution_summary(
        self, total_execution_time_ms: float
    ) -> RPCExecutionSummary:
        """Create a summary of the RPC execution."""
        commands_per_level = []
        level_execution_times_ms = []

        for intermediate in self.intermediate_results:
            commands_per_level.append(len(intermediate.level_results))
            level_execution_times_ms.append(intermediate.execution_time_ms)

        # Calculate parallel execution efficiency
        sequential_time = sum(level_execution_times_ms)
        parallel_efficiency = (
            total_execution_time_ms / sequential_time if sequential_time > 0 else 1.0
        )

        return RPCExecutionSummary(
            request_id=self.request_id,
            total_commands=sum(commands_per_level),
            total_levels=len(self.intermediate_results),
            total_execution_time_ms=total_execution_time_ms,
            commands_per_level=commands_per_level,
            level_execution_times_ms=level_execution_times_ms,
            dependency_graph_depth=self.total_levels,
            parallel_execution_efficiency=parallel_efficiency,
        )


@dataclass(slots=True)
class EnhancedRPCRequest:
    """Enhanced RPC request with intermediate result support."""

    # Core RPC fields
    role: str = "rpc"
    cmds: tuple[RPCCommand, ...] = field(default_factory=tuple)
    u: str = ""

    # Intermediate result features
    return_intermediate_results: bool = False
    include_execution_summary: bool = False
    intermediate_result_callback_topic: str | None = None

    @classmethod
    def from_rpc_request(
        cls,
        request: RPCRequest,
        return_intermediate_results: bool = False,
        include_execution_summary: bool = False,
        intermediate_result_callback_topic: str | None = None,
    ) -> EnhancedRPCRequest:
        """Create enhanced request from standard RPC request."""
        return cls(
            role=request.role,
            cmds=request.cmds,
            u=request.u,
            return_intermediate_results=return_intermediate_results,
            include_execution_summary=include_execution_summary,
            intermediate_result_callback_topic=intermediate_result_callback_topic,
        )

    def to_rpc_request(self) -> RPCRequest:
        """Convert back to standard RPC request for compatibility."""
        return RPCRequest(
            role=self.role,  # type: ignore
            cmds=self.cmds,
            u=self.u,
        )


@dataclass(slots=True)
class EnhancedRPCResponse:
    """Enhanced RPC response with intermediate results."""

    # Core response fields
    r: Any = None
    error: Any = None  # Keep as Any for compatibility
    u: str = ""

    # Enhanced debugging fields
    intermediate_results: list[RPCIntermediateResult] = field(default_factory=list)
    execution_summary: RPCExecutionSummary | None = None

    @classmethod
    def from_rpc_response(
        cls,
        response: RPCResponse,
        intermediate_results: list[RPCIntermediateResult] | None = None,
        execution_summary: RPCExecutionSummary | None = None,
    ) -> EnhancedRPCResponse:
        """Create enhanced response from standard RPC response."""
        return cls(
            r=response.r,
            error=response.error,
            u=response.u,
            intermediate_results=intermediate_results or [],
            execution_summary=execution_summary,
        )

    def to_rpc_response(self) -> RPCResponse:
        """Convert back to standard RPC response for compatibility."""
        return RPCResponse(r=self.r, error=self.error, u=self.u)

    @property
    def has_intermediate_results(self) -> bool:
        """Check if response includes intermediate results."""
        return len(self.intermediate_results) > 0

    def get_result_at_level(self, level_index: int) -> dict[str, Any] | None:
        """Get results from a specific execution level."""
        for intermediate in self.intermediate_results:
            if intermediate.level_index == level_index:
                return intermediate.level_results
        return None

    def get_accumulated_results_at_level(
        self, level_index: int
    ) -> dict[str, Any] | None:
        """Get accumulated results up to a specific execution level."""
        for intermediate in self.intermediate_results:
            if intermediate.level_index == level_index:
                return intermediate.accumulated_results
        return None


# Utility functions for working with intermediate results


def analyze_execution_bottlenecks(
    intermediate_results: list[RPCIntermediateResult],
) -> dict[str, Any]:
    """Analyze intermediate results to identify performance bottlenecks."""
    if not intermediate_results:
        return {"error": "No intermediate results to analyze"}

    # Find slowest level
    slowest_level = max(intermediate_results, key=lambda r: r.execution_time_ms)

    # Calculate level timing statistics
    execution_times = [r.execution_time_ms for r in intermediate_results]
    avg_time = sum(execution_times) / len(execution_times)

    # Identify levels that are significantly slower than average
    slow_threshold = avg_time * 1.5  # 50% above average
    slow_levels = [
        r for r in intermediate_results if r.execution_time_ms > slow_threshold
    ]

    return {
        "slowest_level": {
            "level_index": slowest_level.level_index,
            "execution_time_ms": slowest_level.execution_time_ms,
            "results": list(slowest_level.level_results.keys()),
        },
        "average_level_time_ms": avg_time,
        "slow_levels": [
            {
                "level_index": r.level_index,
                "execution_time_ms": r.execution_time_ms,
                "percentage_above_average": ((r.execution_time_ms / avg_time) - 1)
                * 100,
            }
            for r in slow_levels
        ],
        "total_levels": len(intermediate_results),
        "total_execution_time_ms": sum(execution_times),
    }


def trace_dependency_resolution(
    intermediate_results: list[RPCIntermediateResult],
) -> dict[str, Any]:
    """Trace how dependencies were resolved across execution levels."""
    dependency_trace = {}

    for intermediate in intermediate_results:
        level_trace = {
            "level_index": intermediate.level_index,
            "timestamp": intermediate.timestamp,
            "new_results": list(intermediate.level_results.keys()),
            "available_dependencies": list(intermediate.accumulated_results.keys()),
            "dependency_count": len(intermediate.accumulated_results),
        }

        # Identify which dependencies were used in this level
        if intermediate.level_index > 0:
            prev_accumulated: set[str] = set()
            for prev_intermediate in intermediate_results[: intermediate.level_index]:
                prev_accumulated.update(prev_intermediate.accumulated_results.keys())

            # New dependencies are those not seen in previous levels
            new_dependencies = (
                set(intermediate.accumulated_results.keys()) - prev_accumulated
            )
            level_trace["newly_resolved_dependencies"] = list(new_dependencies)

        dependency_trace[f"level_{intermediate.level_index}"] = level_trace

    return {
        "dependency_trace": dependency_trace,
        "total_dependencies_resolved": len(intermediate_results[-1].accumulated_results)
        if intermediate_results
        else 0,
        "dependency_resolution_efficiency": {
            "levels_required": len(intermediate_results),
            "average_dependencies_per_level": (
                len(intermediate_results[-1].accumulated_results)
                / len(intermediate_results)
                if intermediate_results
                else 0
            ),
        },
    }


def format_intermediate_results_for_debugging(
    intermediate_results: list[RPCIntermediateResult],
) -> str:
    """Format intermediate results in a human-readable format for debugging."""
    if not intermediate_results:
        return "No intermediate results available."

    lines = []
    lines.append("=== RPC Execution Trace ===")
    lines.append(f"Request ID: {intermediate_results[0].request_id}")
    lines.append(f"Total Levels: {intermediate_results[0].total_levels}")
    lines.append("")

    for i, intermediate in enumerate(intermediate_results):
        lines.append(
            f"Level {intermediate.level_index} ({intermediate.execution_time_ms:.1f}ms):"
        )
        lines.append(f"  Progress: {intermediate.progress_percentage:.1f}%")
        lines.append(f"  New Results: {list(intermediate.level_results.keys())}")
        lines.append(
            f"  Total Available: {list(intermediate.accumulated_results.keys())}"
        )

        # Show some result values (truncated for readability)
        for name, value in intermediate.level_results.items():
            value_str = str(value)
            if len(value_str) > 50:
                value_str = value_str[:47] + "..."
            lines.append(f"    {name}: {value_str}")
        lines.append("")

    lines.append("=== End Execution Trace ===")
    return "\n".join(lines)
