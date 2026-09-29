# Harness 1.x Architecture Closure

## Decision

Harness 1.x is closed as a historical/reference baseline. This record does not convert 1.0 or 1.1 into a production release and does not rewrite prior verification history.

### Harness 1.0

- `release_status = NOT_RELEASED`
- `implementation_status = REFERENCE_IMPLEMENTATION`
- `architecture_status = ARCHITECTURE_SUPERSEDED`
- source fingerprint: `835acec55a8f940f515e854c80ebf7199c86b8c6979d16a1d4142d11c7e46b65`
- manifest SHA256: `3522ad3cb7dd02b019538d8bb7fbfca00cd7e476b2ea48e50114c50da9cbdc2e`
- exact reference tag: `harness-1.0.0-reference`

### Harness 1.1 candidate

- `release_status = NOT_RELEASED`
- `implementation_status = REFERENCE_IMPLEMENTATION`
- `architecture_status = ARCHITECTURE_SUPERSEDED`
- `purpose = MIGRATION_AND_SALVAGE_SOURCE`
- exact 1.0 baseline: `835acec55a8f940f515e854c80ebf7199c86b8c6979d16a1d4142d11c7e46b65`
- source fingerprint: `73289893f92c8691d4b51c384ecaf0a779bca220636e50c0dd489b02be143b3d`
- manifest SHA256: `faf33cafbefa4863bffc71a5c515ca7a133897889f86f847b607cfce619781af`
- exact reference tag: `harness-1.1.0-candidate-reference`

## Closure rationale

The 1.x correctness mechanisms remain useful reference material. The architecture is superseded because it directly owned too much runtime/orchestration/process/workspace machinery, increasing operational complexity and reducing accepted-job throughput. `ARCHITECTURE_SUPERSEDED` is therefore not equivalent to `FAILED` and is not a release-readiness claim.

## Known incomplete gates

- S1/A/B final acceptance incomplete
- production release incomplete
- 1.1 release incomplete

No historical evidence is reclassified as a completed production release by this closure.

## Known operational issues

- inventory/isolation overhead
- metadata race
- orphan claim / `QUEUE_NOT_QUIESCENT`
- process/session/runtime ownership complexity

## Salvage principles

The following principles remain valid inputs for the successor architecture:

- Worker completion != Product success
- Frozen Job Contract
- hard safety boundaries
- trusted evidence
- evidence binding/freshness
- retry authorization
- canonical ownership/publication guard
- false-success protection

## Superseded responsibilities

These responsibilities should not be carried forward by default as direct Harness ownership:

- runtime adapters
- scheduler
- worker/process custody
- worktree/candidate copier
- full-tree inventory
- N-worker slot machinery
- runtime-specific retry engine
- agent console/control plane

## Provenance

Primary verification bundle: `HARNESS_VERIFICATION_BUNDLE_20260929_093004.zip`

Bundle SHA256: `5ced79c5d8a17297c0333d2e31bd8f2e68601f345e70f49292c1d15e6aa2c95e`

The exact 1.0 and 1.1 source tags were constructed from the verified source/evidence bytes in that bundle. The closure record itself is intentionally committed after the 1.1 reference tag so it cannot change either tagged source identity.
