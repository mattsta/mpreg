# Unified Correctness — Burndown

| ID   | Track | Item                                 | Status                                                         |
| ---- | ----- | ------------------------------------ | -------------------------------------------------------------- |
| U0.1 | U0    | Architecture docs                    | DONE                                                           |
| U0.2 | U0    | Master plan + burndown + ledger      | DONE                                                           |
| U0.3 | U0    | consensus.md honesty with A1         | DONE                                                           |
| U1.1 | U1    | Implement pre-vote algorithm         | DONE                                                           |
| U1.2 | U1    | RPC pre_vote field + codec           | DONE                                                           |
| U1.3 | U1    | Pre-vote metrics + status            | DONE                                                           |
| U1.4 | U1    | Flag false path = legacy             | DONE                                                           |
| U1.5 | U1    | Rewrite prevote minority test        | DONE                                                           |
| U1.6 | U1    | consensus.md pre-vote                | DONE                                                           |
| U1.7 | U1    | Remove dead config knobs             | DONE                                                           |
| U2.1 | U2    | Election callback via TaskManager    | DONE                                                           |
| U2.2 | U2    | Vote fanout owned (gather)           | DONE                                                           |
| U2.3 | U2    | stop() cancels election work         | DONE                                                           |
| U2.4 | U2    | Status surfaces election task        | DONE                                                           |
| U2.5 | U2    | Task manager tests                   | DONE                                                           |
| U3.1 | U3    | force_follower / step_down helpers   | DONE                                                           |
| U3.2 | U3    | Eliminate test current_state assigns | DONE                                                           |
| U3.3 | U3    | Heal paths use wait_for_leader       | DONE                                                           |
| U3.4 | U3    | Grep/release guard                   | DONE                                                           |
| U4.1 | U4    | command_apply_timeout config         | DONE                                                           |
| U4.2 | U4    | Status effective window + pre_vote   | DONE                                                           |
| U4.3 | U4    | status_dict parity                   | DONE                                                           |
| U4.4 | U4    | Ops field docs                       | DONE (OBSERVABILITY.md)                                        |
| U5.1 | U5    | Raft LCE completeness                | DONE                                                           |
| U5.2 | U5    | task_manager LCE                     | DONE                                                           |
| U5.3 | U5    | Optional boundary helper             | DONE                                                           |
| U5.4 | U5    | fabric raft_transport + supervisors  | DONE                                                           |
| U5.5 | U5    | Logging tests expand                 | DONE                                                           |
| U5.6 | U5    | server supervisor subset             | DONE (snapshot restore, pubsub notify, shared-audit LCE)       |
| U6.1 | U6    | wait_for_leader contract             | DONE                                                           |
| U6.2 | U6    | Marker hygiene                       | DONE (live raft integration+slow; markers registered)          |
| U6.3 | U6    | ci_raft.sh                           | DONE                                                           |
| U6.4 | U6    | release_gate wire                    | DONE                                                           |
| U6.5 | U6    | Hypothesis green                     | DONE                                                           |
| U6.6 | U6    | Multi-run stress                     | DONE (5× ci_raft + 5× raft-core + 3× integ/fabric; see ledger) |
| U7.1 | U7    | fabric raft integration              | DONE                                                           |
| U7.2 | U7    | live raft integration                | DONE                                                           |
| U7.3 | U7    | DistLab first-class Raft scenarios   | DONE (adapter+presets+CI)                                      |
| U7.4 | U7    | full -n auto health                  | DONE (3× 3382p, destroyed=0, 0 warnings; ledger)               |
| U7.5 | U7    | claims.yaml update                   | DONE                                                           |
| U7.6 | U7    | commit (no push)                     | DONE (0.3.2 release commit)                                    |
| U8.1 | U8    | Raft pending-destroy closeout        | DONE (retain bag + aborted-stop tests)                         |
| U8.2 | U8    | Catalog serialize-once               | DONE                                                           |
| U8.3 | U8    | 0.3.2 version + CHANGELOG + claims   | DONE                                                           |
| U8.4 | U8    | Teardown / zero-warning full lock    | DONE (3× 3382p, 0 warnings; filterwarnings + product closes)   |
| U8.5 | U8    | release_gate + milestone commit      | DONE (local; no push)                                          |
