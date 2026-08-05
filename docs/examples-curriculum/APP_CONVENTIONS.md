# App Conventions

## Package layout

```text
mpreg/examples/apps/<level>/<app_id>/
  README.md       # story, run, architecture, production next steps
  run.py          # async main(); asyncio.run; sys.exit on failure
  # optional:
  app.py          # domain helpers
  client.py       # caller-only scripts
  settings.toml   # long-lived server sample
```

Register every shipped app in `mpreg/examples/apps/_shared/registry.py`.

## Required README sections

1. **Story** — one paragraph product narrative  
2. **Lesson** — single primary takeaway  
3. **Run** — `uv run mpreg-example run <id>` (also `uv run mpreg examples run <id>`)  

4. **What it proves** — asserts / invariants  
5. **Architecture** — components (bullet or mermaid)  
6. **Non-claims** — what it does *not* guarantee  
7. **Production exit ramp** — profiles, HA, monitoring, next apps  

## Code rules

1. **Dynamic ports** via `port_range_context` or server auto-port.  
2. **`ensure()`** (from `_shared.runtime`) for invariants — raise `ExampleFailed`.  
3. **Cleanup** in `try/finally` or `run_with_servers`.  
4. **Logging** default INFO; avoid DEBUG spam in happy path.  
5. **No network beyond localhost** in default runs.  
6. **Public client APIs** preferred (`MPREGClientAPI`, `MPREGClusterClient`).  
7. **Exit codes:** 0 success, 1 assertion/runtime failure, 2 usage error.  

## Shared runtime (`_shared`)

| Module | Role |
|--------|------|
| `runtime.py` | ensure, banners, server lifecycle wrappers |
| `registry.py` | app metadata + import paths |
| `runner.py` | list/run/smoke/suite — console script `mpreg-example` |
| `__main__.py` | thin hook only; prefer `uv run mpreg-example` |

**Entrypoints (pyproject `[project.scripts]`):**

| Script | Target |
|--------|--------|
| `mpreg-example` | `mpreg.examples.apps._shared.runner:main` |
| `mpreg examples …` | Click group → same runner |

**Pytest:** `tests/examples_apps/` runs each app's real `main()` under markers
`example_apps`, `example_smoke`, `example_suite`.

## Naming

- **app_id**: `snake_case`, stable forever (CLI key).  
- **level dir**: zero-padded for sort order.  
- **title**: human string in registry only.  

## Chaos / optional modes

Optional flags (`--chaos`, `--slow`) must:

- default **off**  
- document expected outcome  
- still clean up resources  

## Honesty template (copy into READMEs)

```markdown
## Non-claims
- Not Byzantine fault tolerant.
- Not exactly-once unless the queue/app protocol is specified.
- Multi-cluster demos show routing availability, not global linearizability.
```
