"""Product-shaped curriculum example apps.

See ``docs/examples-curriculum/`` and run via::

    uv run mpreg-example list
    uv run mpreg-example smoke
    uv run mpreg examples list   # same runner via main CLI
"""

from __future__ import annotations

from mpreg.examples.apps._shared.registry import APPS, ExampleApp, get_app, list_apps

__all__ = ["APPS", "ExampleApp", "get_app", "list_apps"]
