# Evidence and Claim Discipline

Status: `PROPOSED_GOVERNANCE_STANDARD`

This document is proposed for Proppadia model-development and platform-governance work. It is not canonical merely because it exists.

## Core Rule

No claim becomes an established project fact because it is plausible, repeated, convenient, or necessary to continue. A claim becomes established only when it is precisely defined, its evidence requirement is known, supporting evidence exists, counterevidence is addressed, scope and limitations are recorded, and it has not been superseded or invalidated.

## Claim Statuses

Use the controlled vocabulary defined in:

`artifacts/analysis/model_development/evidence_claim_discipline/2026-07-10/claim_status_vocabulary_2026-07-10.md`

Key statuses include `OBSERVED`, `REPRODUCED`, `VERIFIED`, `GOVERNED_DECISION`, `SUPPORTED_HYPOTHESIS`, `UNTESTED_HYPOTHESIS`, `ASSUMPTION`, `UNRESOLVED`, `INVALIDATED`, `SUPERSEDED`, and `NOT_PROVABLE_FROM_AVAILABLE_EVIDENCE`.

## Evidence Hierarchy

Evidence strength ranges from exact runtime capture and deterministic replay with hashes down to filenames, assistant inference, and unsupported assertion. Filenames such as `latest`, `canonical`, `production`, `final`, and `prepared` are never proof by themselves.

## Advancement Principle

Exploratory analysis may proceed from `SUPPORTED_HYPOTHESIS`. Irreversible implementation requires `VERIFIED` or explicit `GOVERNED_DECISION`. Production and promotion require both verified evidence and governed approval.

## Assistant Work Protocol

Every substantive task should begin by listing verified facts, unverified hypotheses, assumptions, evidence to be produced, stop conditions, and out-of-scope changes.

Every final report should separate findings, interpretations, decisions, unknowns, and invalidated prior claims.

## Stop Conditions

Stop before implementation when a required premise is unresolved, when the source of truth is not bound by lineage or reproducibility, when historical model version is unknown, when row population or feature semantics are not proven, or when the next action would hide prior history or make a governance-significant change.

## Current Scope

This first edition was created in response to MLB-CC-0001 governance and replayability issues. It should be reviewed before being adopted as a durable governance standard for all future Champion-Challenger work.
