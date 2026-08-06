# queue_ack_receive_lab (L1 · plane)

Poll receive + explicit acknowledge on the queue manager, plus broadcast/FNF.

```bash
uv run mpreg-example run queue_ack_receive_lab
```

## Proves

- `queue.receive` — `MessageQueueManager.receive_message`
- `queue.ack` — `acknowledge_message(queue, id, subscriber)`
- `queue.broadcast` / `queue.fnf` delivery guarantees

## Non-claims

- Multi-node queue federation durability (see `queue_federation_lab`)
