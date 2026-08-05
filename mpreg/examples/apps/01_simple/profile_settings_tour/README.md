# profile_settings_tour (L1 · product)

## Story

Operators start from packaged TOML profiles (`dev`, `single-node`, `federated`,
`soft-rt`) rather than hand-building `MPREGSettings`.

## Lesson

`MPREGSettings.from_path` + `mpreg config-check`.

## Run

```bash
uv run mpreg-example run profile_settings_tour
```
