"""Consistent CLI output formatting (json | table | plain)."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import Any

import click
from rich.console import Console
from rich.table import Table

console = Console()

def add_format_option(fn):  # type: ignore[no-untyped-def]
    return click.option(
        "--format",
        "output_format",
        type=click.Choice(["json", "table", "plain"], case_sensitive=False),
        default="json",
        show_default=True,
        help="Output format",
    )(fn)

def emit(
    data: Any,
    *,
    output_format: str = "json",
    table_title: str | None = None,
) -> None:
    fmt = (output_format or "json").lower()
    if fmt == "json":
        console.print_json(data=_jsonable(data))
        return
    if fmt == "plain":
        if isinstance(data, (dict, list)):
            console.print(json.dumps(_jsonable(data), indent=2, default=str))
        else:
            console.print(str(data))
        return
    # table
    if isinstance(data, Mapping):
        table = Table(title=table_title)
        table.add_column("Key")
        table.add_column("Value")
        for key, value in data.items():
            table.add_row(str(key), _cell(value))
        console.print(table)
        return
    if isinstance(data, Sequence) and not isinstance(data, (str, bytes)):
        rows = list(data)
        if not rows:
            console.print("[dim](empty)[/dim]")
            return
        if all(isinstance(r, Mapping) for r in rows):
            keys: list[str] = []
            for row in rows:
                assert isinstance(row, Mapping)
                for k in row:
                    sk = str(k)
                    if sk not in keys:
                        keys.append(sk)
            table = Table(title=table_title)
            for k in keys:
                table.add_column(k)
            for row in rows:
                assert isinstance(row, Mapping)
                table.add_row(*[_cell(row.get(k, "")) for k in keys])
            console.print(table)
            return
        table = Table(title=table_title)
        table.add_column("Value")
        for row in rows:
            table.add_row(_cell(row))
        console.print(table)
        return
    console.print(str(data))

def _cell(value: Any) -> str:
    if isinstance(value, (dict, list)):
        text = json.dumps(value, default=str)
        return text if len(text) < 80 else text[:77] + "..."
    return str(value)

def _jsonable(data: Any) -> Any:
    if hasattr(data, "to_dict") and callable(data.to_dict):
        return data.to_dict()
    if isinstance(data, Mapping):
        return {str(k): _jsonable(v) for k, v in data.items()}
    if isinstance(data, Sequence) and not isinstance(data, (str, bytes)):
        return [_jsonable(v) for v in data]
    if hasattr(data, "__dict__") and not isinstance(data, type):
        try:
            return _jsonable(vars(data))
        except TypeError:
            return str(data)
    return data
