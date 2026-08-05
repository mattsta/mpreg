# rpc_intermediate_results (L2 · product)

## Story

Multi-level RPC DAGs can expose per-level progress for debugging soft-RT and
streaming (M3) callers.

## Lesson

`IntermediateResultCollector` + live dependency DAG; wire flag
`return_intermediate_results` on server `RPCRequest`.

## Run

```bash
uv run mpreg-example run rpc_intermediate_results
```
