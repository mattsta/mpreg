# media_pipeline (L2)

## Story

Four specialized nodes process a media asset: ingest → transcode → analyze → store.

## Lesson

Multi-stage RPC DAG across resource-tagged workers.

## Run

```bash
uv run mpreg-example run media_pipeline
```

## What it proves

- Terminal stored=True for vid-1
- quality score derived from bytes

## Architecture

```text
Client → Ingest → Process → Analytics → Storage
```

## Non-claims

- Not real codecs; not object storage.

## Production exit ramp

- From real_world DataPipelineExample patterns
- Next: ml_inference_mesh
