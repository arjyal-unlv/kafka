# Report evidence — Aayusha Shrestha

Supporting evidence for the CS 789 Final Report. The report cites concise figures and repository artifacts rather than embedding complete terminal logs. Detailed LLM prompts, raw outputs, generated tests, verification evidence, and model configuration are preserved under `llm-assisted/Aayusha/` on branch `llm-assisted-group01`.

## Model and reproducibility evidence

| File | What it covers |
|---|---|
| `Modelfile` | Local Ollama configuration used for the experiments, including `num_ctx = 16384` and `temperature = 0`. |
| `model-version.txt` | Recorded local model/version information used for reproducibility. |
| `model-details.txt` | Qwen2.5-Coder 7B model details, including architecture, context information, quantization, and model metadata. |
| `model-list.txt` | Local Ollama model listing showing the base model and the configured 16K-context variant. |
| `evidence.md` | Index of the evidence supporting the Final Report. |

The experiments used Qwen2.5-Coder 7B locally through Ollama. The configured model variant was `qwen2.5-coder-7b-16k:latest`, with `num_ctx = 16384` and `temperature = 0`.

---

## Part A — PR #12875

`part-a/` contains the complete record of the LLM-assisted integration experiment for Apache Kafka PR #12875.

| Folder | What it covers |
|---|---|
| `part-a/prompts/` | Prompt exactly as sent to the local model, including the selected source-target context and integration constraints. |
| `part-a/outputs/` | Raw model response preserved without rewriting. |
| `part-a/evidence/` | Verification and assessment of the model result, together with supporting source-target design evidence. |

PR #12875 concerns `LockException` handling during Kafka Streams task initialization. The upstream implementation assumes the newer `StateUpdater` and pending-initialization architecture, while LinkedIn Kafka 3.0-li uses an older synchronous restoration path.

The model recognized that the upstream implementation itself was absent but that the target already preserved the essential retry behavior through a different architecture. It therefore returned a no-change conclusion rather than inventing unavailable `StateUpdater` APIs.

The result was independently verified by inspecting the target `tryToCompleteRestoration()` control flow. The final disposition was:

**NO CHANGE REQUIRED**

No generated production patch was accepted because no source modification was necessary.

Supporting report evidence also includes the #12875 architectural-divergence figure comparing the upstream `StateUpdater` retry path with the older LinkedIn synchronous restoration loop.

---

## Part B — PR #12486

`part-b/` contains the complete record of the LLM-assisted test-generation experiment for Apache Kafka PR #12486.

| Folder | What it covers |
|---|---|
| `part-b/prompts/` | Prompt exactly as sent to the local model, including the batching behavior, existing test structure, and requested boundary cases. |
| `part-b/outputs/` | Raw model-generated test response. |
| `part-b/evidence/` | Generated-test verification, minimal repair, regression experiment, restored-fix validation, and final assessment. |

The model generated three batching-boundary tests:

- exactly **1,024 records**
- **1,025 records**, immediately above the batch limit
- **2,049 records**, requiring multiple batches

None compiled exactly as generated because the response used Scala `.asJava` without the required collection-conversion import. After a minimal compatibility repair, all three tests compiled and passed.

A controlled regression experiment then removed the batching behavior:

- 1,024-record test: **PASS**
- 1,025-record test: **FAIL**
- 2,049-record test: **FAIL**

After restoring the implementation, all three tests passed again.

Final accounting:

- **3 tests generated**
- **0 usable exactly as generated**
- **3 usable after minimal repair**
- **2/3 regression-sensitive**

The 1,024-record test was retained as a boundary characterization test, while the 1,025- and 2,049-record tests demonstrated direct regression sensitivity.

Supporting screenshots include:

- Part B baseline focused test run — build successful
- controlled regression with batching removed — 3 tests executed, 2 failed
- restored batching implementation — all 3 generated tests passed

---

## Conventional PR integration evidence

The individual integration branches preserve the final outcomes for all seven assigned PRs.

| PR | Category | Evidence / outcome |
|---|---:|---|
| `#12753` | 1 | Clean cherry-pick; focused `PlaintextConsumerTest.testSeek()` passed. |
| `#12921` | 2 | Integrated successfully; added focused HTML-format regression test. |
| `#13325` | 2 | Integrated successfully; strengthened `testInvalidGroupMetadata()` to verify concurrent-access diagnostic information. |
| `#12486` | 3 | Cherry-pick conflicted in two Scala files; batching behavior manually adapted to the older LinkedIn writer/test design; focused tests and Scoverage validation completed. |
| `#12875` | 4 | Git and RePatch failed because of architectural divergence; manual design recovery showed equivalent retry intent in the older target architecture. Final result: **NO CHANGE REQUIRED**. |
| `#14313` | 4 | Required later `TopicAdmin` retry/API infrastructure is absent from 3.0-li. Final result: **NOT PORTABLE AS A TARGETED BACKPORT**. |
| `#12478` | 4 | Targeted adaptation of retry behavior completed; six focused retry/backoff tests passed. |

For #12486, Scoverage measured:

- `RecordListConsumer`: **100% statement / 100% branch**
- `BrokerMetadataSnapshotter`: **97.37% statement / 100% branch**
- `CreateSnapshotEvent`: **80% statement / 100% branch**

Coverage was treated as supporting evidence rather than proof of correctness. The LLM Part B revert-and-restore experiment provided stronger evidence that the generated tests could detect removal of the batching behavior.

---

## RePatch and conflict evidence

RePatch was used where direct cherry-pick integration failed or required investigation.

For #12486, the stored RePatch zero-conflict result could not be reproduced against the required baseline. Investigation of the stored experiment showed that the RePatch run used a different target parent from the required 3.0-li baseline. The result was therefore treated as a target-version mismatch rather than evidence that the required integration was conflict-free.

For #12875, RePatch reported two conflicting files, two conflicts, and approximately 1,456 conflicting lines. No relevant scoped refactoring conflict explained the failure. Manual design recovery showed that the barrier was the missing newer `StateUpdater` architecture rather than a rename or move that RePatch could invert.

For #14313, direct source-target investigation showed that the required `TopicAdmin` retry/API evolution was absent from the target, making a small targeted backport inappropriate.

For #12478, the design difference was local enough to resolve manually, and the resulting targeted adaptation was verified with focused tests.

---

## Upstream Apache Kafka PRs

- PR #12753  
  https://github.com/apache/kafka/pull/12753

- PR #12921  
  https://github.com/apache/kafka/pull/12921

- PR #13325  
  https://github.com/apache/kafka/pull/13325

- PR #12486  
  https://github.com/apache/kafka/pull/12486

- PR #12875  
  https://github.com/apache/kafka/pull/12875

- PR #14313  
  https://github.com/apache/kafka/pull/14313

- PR #12478  
  https://github.com/apache/kafka/pull/12478

---

## Repository evidence links

Integration branches:

- `pr-12753`
- `pr-12921`
- `pr-13325`
- `pr-12486`
- `pr-12875`
- `pr-14313`
- `pr-12478`

LLM evidence branch:

`llm-assisted-group01`

Complete LLM evidence:

`llm-assisted/Aayusha/part-a/`

`llm-assisted/Aayusha/part-b/`

The detailed prompts and outputs are preserved in the repository so the Final Report can summarize the experimental results while retaining a reproducible record of exactly what was provided to and returned by the model.