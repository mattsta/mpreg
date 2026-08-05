# rpc_fqn_namespace (L1 · product)

## Story

Every RPC name on the wire is a **dotted FQN**. Bare names auto-qualify under
the active namespace (`default_rpc_namespace`, default `app`). Users **cannot**
register under the reserved `mpreg.*` platform root. Optional
`bound_rpc_namespace` locks register/call to a hierarchical prefix
(operator↔client conformance).

## Lesson

**Explicit > implicit.** `demo.add` ≠ `app.add` ≠ `mpreg.system.echo`. There is
no context-dependent short-name magic — only bare → active-ns qualification.

## Run

```bash
uv run mpreg-example run rpc_fqn_namespace
```

## What it proves

- `qualify_rpc_name("add")` → `app.add` (helpers)
- Live bare register/call under `default_rpc_namespace="demo"` → `demo.*`
- Explicit FQN `orders.create` pass-through
- `mpreg.*` registration denied (namespace deny, not a short-name list)
- Bound client: bare `create` → `orders.create`; `demo.add` rejected
- Wrong default ns does **not** hit another ns's bare leaf

## API drill-down

| API | Role |
|-----|------|
| `qualify_rpc_name` / `assert_registration_allowed` / `assert_call_allowed` | Pure policy helpers (`mpreg.core.rpc_naming`) |
| `MPREGSettings.default_rpc_namespace` | Server bare-register qualifier |
| `MPREGSettings.bound_rpc_namespace` | Hierarchical lock (optional) |
| `MPREGClientAPI(default_rpc_namespace=, bound_rpc_namespace=)` | Client qualify + bound enforce |
| `PlatformRpc.*` | Canonical platform FQNs under `mpreg.*` |

## Non-claims

- Not multi-tenant authZ beyond hierarchical bound prefix.
- Not renaming historical short names in external systems (migrate to FQN).

## Production exit ramp

- Prefer reverse-DNS style roots (`acme.billing.charge`)
- Platform surfaces: always call `PlatformRpc` / `mpreg.*` explicitly
- Next: `rpc_versioned_topic`, `namespace_policy_gate`, `ops_cli_tour`

## Observability

Default-on `ExampleProbe` via `app_run` — look for `◆ obs` on the run summary.
