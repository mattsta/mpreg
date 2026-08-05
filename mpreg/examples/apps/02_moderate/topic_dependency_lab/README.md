# topic_dependency_lab (L2 · plane)

## Story

Topic-aware RPC commands declare dependencies on other command results, queue
messages, or cache updates. The resolver builds a graph and exposes the ready set.

## Lesson

`TopicDependencyResolver` / `DependencyGraph` / `create_topic_dependency_resolver`.

## Run

```bash
uv run mpreg-example run topic_dependency_lab
```

## Proves

- Factory + statistics surface
- Graph from explicit `dependency_topic_patterns`
- Manual graph ready-set with required vs optional deps
- Cleanup of request graphs
- Heuristic `.result` / queue message detection
- Empty command list → 100% progress

## Non-claims

- Does not run a live multi-hop RPC wait loop over the wire — graph model only.
