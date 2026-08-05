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
- `dns register` + `dns list`
- `doctor` / discovery status against live URL

## Non-claims

- Not full interactive TTY UX.
- DNS CLI flag names may evolve — app records friction if aliases needed.

## Production exit ramp

- Script these in runbooks; pair with monitoring auth tokens
