"""CLI discovery summary include_ingress fix."""

from __future__ import annotations

import ast
from pathlib import Path

def test_discovery_summary_does_not_shadow_include_ingress() -> None:
    source = Path("mpreg/cli/main.py").read_text()
    tree = ast.parse(source)
    found = False
    for node in ast.walk(tree):
        if (
            isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and node.name == "discovery_summary"
        ):
            for child in ast.walk(node):
                if (
                    isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                    and child.name == "_summary"
                ):
                    found = True
                    for stmt in child.body:
                        if isinstance(stmt, ast.Assign):
                            for target in stmt.targets:
                                if isinstance(target, ast.Name):
                                    assert target.id != "include_ingress", (
                                        "assigning to include_ingress inside "
                                        "_summary causes UnboundLocalError"
                                    )
    assert found, "discovery_summary._summary not found"
