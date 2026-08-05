"""Pytest surface for curriculum example apps.

Demos are the product proof: each app's async ``main()`` is exercised live
under real servers/ports. Prefer entrypoints for manual runs::

    uv run mpreg-example smoke
    uv run mpreg-example suite
    uv run pytest tests/examples_apps -m example_smoke
"""
