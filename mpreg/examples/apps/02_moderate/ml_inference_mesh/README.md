# ml_inference_mesh (L2)

## Story

A router fans inference to vision vs NLP workers based on request kind.

## Lesson

Specialized resource locs for heterogeneous ML serving.

## Run

```bash
uv run mpreg-example run ml_inference_mesh
```

## What it proves

- Image path labels cat
- Text path sentiment pos

## Architecture

```text
Client → Router → Vision|NLP
```

## Non-claims

- Toy models only.
- Not GPU scheduling or batching.

## Production exit ramp

- From real_world MLInferenceExample
- Next: multi_region_shop
