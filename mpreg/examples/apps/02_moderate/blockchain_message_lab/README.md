# blockchain_message_lab (L2 · plane)

## Story

Fabric can carry blockchain-recorded messages with priority, delivery
guarantees, and DAO-governed routes. This lab teaches the **wire types**.

## Lesson

`BlockchainMessage`, `MessageRoute`, `MessagePriority`, `DeliveryGuarantee`.

## Run

```bash
uv run mpreg-example run blockchain_message_lab
```

## What it proves

- Enum surfaces and route construction
- Message validation fail-closed
- Federation manager class importable

## Non-claims

- Does not run a full hub mesh with on-chain settlement.

## Production exit ramp

- `docs/BLOCKCHAIN_*` + fabric hub hierarchy tours
