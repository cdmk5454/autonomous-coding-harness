# v1 → v2 migration validation checklist

## Preservation before main replacement

- [x] Confirm existing `main` public v1 baseline: `4e9393fcf56ca23f5da177dcfd63719a135f5523` (0.99.2.1)
- [x] Confirm `archive/harness-1.x-reference` exists: `c7c6bb6d37ce53f9eb145ec58c55d0d50e533430`
- [ ] Confirm any additional legacy tag is needed **and targets the intended commit**; don't misleadingly call 0.99.2.1 "0.99.1" or conflate it with 1.0/1.1 archive

## Source and publication

- [x] Keep real `profiles/`, runtime `config.json`, private keys, logs, control state and nested ZIP artifacts out of v2 candidate
- [x] Keep external third-party Skill code out of the public release pending provenance/license review
- [ ] Inspect PR diff and complete GitHub Secret Scanning results before merge
- [ ] Pin exact candidate commit and inspect all changed and deleted paths
- [ ] Verify no concurrent `main` changes or stale base before merge

## Runtime and authorization (not yet proven)

- [ ] Verify tool denial and file-write prevention separately for each actual provider/mode
- [ ] Verify real safe `read-only` enforcement (including shell/subprocess escape attempts)
- [ ] Verify workspace/allowed path, candidate isolation, diff ownership, recovery and rollback or explicitly de-scope claims
- [ ] Verify failure domains, bounded retry and no-success-without-proof behavior
- [ ] Verify Build/Test/Review/QA evidence is bound to the final commit and exact inputs
- [ ] Verify cross-family Reviewer routing and that review cannot mutate the candidate
- [ ] Verify credential redaction and DB/data access contract for target environment
- [ ] Exercise a low-risk actual job, a failure path, an isolated QA path and integrated release flow

`STATIC PASS` on this public package **does not** close the runtime checklist.
