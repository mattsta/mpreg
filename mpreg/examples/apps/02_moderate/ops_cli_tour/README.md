# ops_cli_tour (L2 · legacy/ops)

## Story

Operators drive MPREG through `mpreg` CLI — call, dns, doctor, config-check,
examples — against a live local node.

## Lesson

`click.testing.CliRunner` exercises the same entrypoints as `uv run mpreg …`
(never `python -m`).

## Run

```bash
uv run mpreg-example run ops_cli_tour
```

## What it proves

- `config-check` on dev profile
- `examples list`
- `client call` with `--locs`
- `client cache-put` / `cache-get` plane CLI
- `client cache-strong-retry-abort --help` (ops-driven CFT; not auto-heal)
- `dns register` + `dns list`
- `doctor` / discovery status against live URL
- `monitor strong|audit --format table` capability honesty lines
- `doctor --strong --audit` against monitoring HTTP

## Non-claims

- Not full interactive TTY UX.
- DNS CLI flag names may evolve — app records friction if aliases needed.
- CLI retry-abort help is not a live residual clear (see DistLab / live mesh e2e).

## Production exit ramp

- Script these in runbooks; pair with monitoring auth tokens
