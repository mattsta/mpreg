# packet_loss_chaos (L3 · plane)

Plane-separated packet loss via `FaultInjector` drop rates, composed with live
`/mgmt` drain admission.

```bash
uv run mpreg-example run packet_loss_chaos
```

Non-claim: does not splice loss into the kernel TCP stack; teaches the platform
delivery model operators actually configure.
