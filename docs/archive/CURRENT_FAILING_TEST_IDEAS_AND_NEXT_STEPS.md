# Deep-Dive Debug Plan: GOODBYE/STATUS/Gossip Failures

## Goal

Make the GOODBYE protocol correct under normal and adverse conditions, and get reliable end-to-end test verification.

## Current failing tests (from latest reports)

- `tests/test_goodbye_immediate_reentry.py::TestGoodbyeImmediateReentry::test_gossip_still_blocked_after_reentry`
  - Fails: `assert server2_url not in server1.cluster.peers_info` after `server2.send_goodbye(...)`
- `tests/test_goodbye_comprehensive_integration.py::TestGoodbyeComprehensiveIntegration::test_gossip_stability_with_departed_peers`
  - Fails: departed peers re-appear in `peers_info` during gossip stability window
- `tests/test_goodbye_comprehensive_integration.py::TestGoodbyeComprehensiveIntegration::test_complete_gossip_goodbye_lifecycle`
  - Fails: re-joined peer not present in `peers_info` after expected re-entry

## Observations from logs (symptoms)

- After GOODBYE, some nodes still show the departed peer in `peers_info`, or `_departed_peers` is unexpectedly empty.
- Some nodes appear to hold peer info under a different URL than the GOODBYE `departing_node_url`.
- Re-entry propagation is inconsistent: direct STATUS may re-add on one node, but gossip does not consistently clear `_departed_peers` on others.

## Top suspected root causes (ranked)

1. **GOODBYE only sent to outbound connections**  
   `send_goodbye` and `_broadcast_peer_reentry` only use `self.peer_connections`. If a peer is only inbound (server connected to us), it never receives GOODBYE or re-entry.
2. **Peer URL mismatch (advertised vs connection URL)**  
   `remove_peer` returns `False` if `departing_node_url` is not an exact key in `peers_info`. That skips `_departed_peers` updates and leaves stale peers in the registry.
3. **Gossip re-entry blocked unconditionally**  
   `process_gossip_message` currently skips any peer that is in `_departed_peers`, even if the gossip comes from a third-party node with fresh STATUS-derived data. This prevents cluster-wide re-entry.
4. **Connection management ignores departed list**  
   `_manage_peer_connections` does not skip `_departed_peers`, so reconnects can happen before the system is ready, creating flapping state and re-addition.

## Instrumentation plan (targeted, not noisy)

Add temporary DEBUG-level logs to expose:

- GOODBYE send targets: outbound peers + inbound peers list (if added).
- `_handle_goodbye_message` outcomes:
  - `departing_node_url`
  - was `peers_info` key found
  - advertised URL matches used for fallback
  - `_departed_peers` updates
- STATUS processing:
  - connection URL vs advertised URL
  - when `_departed_peers` entries are cleared
- `process_gossip_message`:
  - when gossip is skipped for departed peers, include sender URL and peer URL
  - when re-entry via gossip is allowed

## Repro setup (deterministic)

- Run single-worker to avoid port allocator collisions:
  - `PYTEST_XDIST_WORKER=gw0 uv run pytest tests/test_goodbye_immediate_reentry.py::TestGoodbyeImmediateReentry::test_gossip_still_blocked_after_reentry -s`
- If the environment blocks socket binds, run with unsandboxed permissions.

## Concrete fix candidates (ordered)

1. **Track inbound server connections and use them for GOODBYE/re-entry broadcasts**
   - Add a map like `self._inbound_server_connections` keyed by `peer_url`.
   - Update in `opened` when a STATUS is accepted; remove on disconnect.
   - Use both outbound and inbound connections in `send_goodbye` and `_broadcast_peer_reentry`.

2. **Robust GOODBYE removal by advertised URL matching**
   - If `departing_node_url` not found in `peers_info`, search for any peer whose `advertised_urls` contains that URL.
   - Remove and blocklist all matching URLs (primary + advertised).

3. **Allow gossip-driven re-entry**
   - In `process_gossip_message`, if `peer_info.url` is in `_departed_peers` but:
     - gossip comes from a different sender, and
     - `peer_info` has active capabilities or `last_seen > 0`,
       then clear `_departed_peers` for that URL and accept the update.

4. **Block connection retries for departed peers**
   - In `_manage_peer_connections`, skip peers that are in `_departed_peers` unless explicitly cleared by STATUS/gossip.

5. **Expand unit tests for URL mismatch**
   - Add test coverage where `departing_node_url` differs from `peer_info.url` but exists in `advertised_urls`.

## Immediate next steps

1. Add inbound connection tracking and route GOODBYE/re-entry to all peers (inbound + outbound).
2. Implement URL-resolution fallback in GOODBYE removal and update `_departed_peers` consistently.
3. Update `process_gossip_message` to allow re-entry via third-party gossip.
4. Rerun the three failing tests with single-worker config.
5. Remove temporary DEBUG logs once behavior stabilizes.
