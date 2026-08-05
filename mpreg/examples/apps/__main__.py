"""Thin package hook — prefer the ``mpreg-example`` console script.

Use::

    uv run mpreg-example list|run|smoke|suite
    uv run mpreg examples list|run|smoke|suite
"""

from __future__ import annotations

from mpreg.examples.apps._shared.runner import main

if __name__ == "__main__":
    main()
