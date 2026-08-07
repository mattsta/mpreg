#!/usr/bin/env python3

import sys

sys.path.append(".")

from graphlib import TopologicalSorter

from mpreg.core.model import RPCCommand


def debug_dependency_resolution():
    print("=== DEBUG: Dependency Resolution for Parallel Convergence ===")

    # Create the exact commands from the failing test
    commands = [
        # Parallel Branch A: GPU processing
        RPCCommand(
            name="gpu_branch",
            fun="run_inference",
            args=("ModelA", "input_data"),
            locs=frozenset(["gpu", "ml-models"]),
        ),
        # Parallel Branch B: CPU processing
        RPCCommand(
            name="cpu_branch",
            fun="heavy_compute",
            args=("input_data", 20),
            locs=frozenset(["cpu-heavy"]),
        ),
        # Parallel Branch C: Database lookup
        RPCCommand(
            name="db_branch",
            fun="query",
            args=("SELECT * FROM data",),
            locs=frozenset(["database"]),
        ),
        # Convergence: Analytics using all three branches
        RPCCommand(
            name="converged_analysis",
            fun="analytics",
            args=("convergence_test", ["gpu_branch", "cpu_branch", "db_branch"]),
            locs=frozenset(["analytics"]),
        ),
    ]

    # Create the funs dict like the server does
    funs = {}
    for cmd in commands:
        funs[cmd.name] = cmd

    # Replicate the exact dependency detection logic from server.py
    sorter: TopologicalSorter[str] = TopologicalSorter()

    def find_dependencies_recursive(obj, deps_list):
        """Recursively search for dependencies in nested data structures."""
        if isinstance(obj, str):
            # Check for exact match first
            if obj in funs:
                deps_list.append(obj)
                print(f"  Found dependency: '{obj}' (exact match)")
            # Check for field access pattern (e.g., "step2_result.final")
            elif "." in obj:
                base_name = obj.split(".", 1)[0]
                if base_name in funs:
                    deps_list.append(base_name)
                    print(
                        f"  Found dependency: '{base_name}' (field access from '{obj}')"
                    )
        elif isinstance(obj, dict):
            # Search dictionary values
            for value in obj.values():
                find_dependencies_recursive(value, deps_list)
        elif isinstance(obj, list | tuple):
            # Search list/tuple elements
            for item in obj:
                find_dependencies_recursive(item, deps_list)

    print("\nAnalyzing dependencies for each command:")
    for name, rpc_command in funs.items():
        print(f"\nCommand: {name}")
        print(f"  Function: {rpc_command.fun}")
        print(f"  Args: {rpc_command.args}")

        # Check both direct string arguments AND nested structures
        deps: list[str] = []
        for i, arg in enumerate(rpc_command.args):
            print(f"  Checking arg[{i}]: {arg} (type: {type(arg).__name__})")
            find_dependencies_recursive(arg, deps)

        # Remove duplicates while preserving order
        deps = list(dict.fromkeys(deps))
        print(f"  Final dependencies: {deps}")
        sorter.add(name, *deps)

    print("\nTopological sorting:")
    levels = []
    sorter.prepare()
    level_num = 0
    while sorter.is_active():
        level: list[str] = []
        level_num += 1
        ready = sorter.get_ready()
        print(f"Level {level_num}: {ready}")
        level.extend(ready)
        levels.append(level)
        sorter.done(*ready)

    print(f"\nTotal levels: {len(levels)}")
    for i, level in enumerate(levels):
        print(f"Level {i + 1}: {level}")

    # Check if convergence analysis is in the final level
    final_level = levels[-1] if levels else []
    if "converged_analysis" in final_level:
        print("\n✅ converged_analysis is correctly in the final level")
    else:
        print("\n❌ converged_analysis is NOT in the final level - PROBLEM!")
        for i, level in enumerate(levels):
            if "converged_analysis" in level:
                print(f"   converged_analysis found in level {i + 1}")


if __name__ == "__main__":
    debug_dependency_resolution()
