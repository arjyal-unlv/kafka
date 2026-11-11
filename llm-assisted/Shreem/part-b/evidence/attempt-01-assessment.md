# Part B attempt 01 — assessment

**PR:** #12244, integrated by hand as commit `1597d78733` on branch `pr-12244`.
**Model:** qwen2.5-coder:7b (digest `dae161e27b0e`, Q4_K_M, 7.6B), temperature 0.2, top_p 0.9,
seed 42, num_ctx 16384, repeat_penalty 1.15, num_predict 4096. Apple M4, 24 GB, ollama 0.40.3.
**Run:** 585 tokens, 45.9 s, `done_reason: stop`.

## The gap being filled

`commitOffsetsAsync` is referenced 14 times in `ConsumerCoordinatorTest`, but never with an empty
offsets map. The empty-offsets local-completion path added by `1597d78733` was executed by
**0 of the 246 passing tests**, and the method sat at **BRANCH 2/4**.

## Context supplied, and why

The adaptation diff, the method as it now stands, and 94 lines of the real test fixture — field
declarations, `@BeforeEach` setUp, and one existing async-commit test as a style example. The
fixture was included deliberately: without the real field names a model cannot produce code that
compiles, and "it did not compile" would then say nothing about whether it understood the change.

## Generated vs usable

**3 generated. 0 compiled as written. 1 usable after hand repair.**

8 compiler errors, three distinct mistakes each repeated:

| Mistake | Count | What it reveals |
|---|---|---|
| `client.numSendRequests()` | 3 | invented a `MockClient` method that does not exist |
| `assertTrue(int)` | 3 | `MockCommitCallback.invoked` is an int counter, not a boolean |
| `when(boolean)` | 2 | treated the real `coordinator` from `buildCoordinator()` as a Mockito mock |

It used `mockOffsetCommitCallback` correctly — because that field was in the supplied fixture.
**Every failure is in API knowledge it was not given and did not ask for.** That is the useful
distinction: the model was not confused about the change, it was confused about the test harness.

Tests 2 and 3 were discarded rather than repaired. Both depend on stubbing `coordinatorUnknown()`
on a live object, which the fixture cannot do; repairing them would have meant rewriting the test
from a different premise, which is authoring rather than repairing.

Test 1 was repaired because its **idea was correct**: empty offsets, callback completes, no
coordinator needed. The repair was mechanical — drop the invented method, compare the int counter,
invoke the completed callbacks. Saved as `attempt-01-test1-repaired.java`, keeping the model's own
name `testCommitEmptyOffsetsLocally` so its provenance stays visible.

## Can-it-fail validation

The repaired test passes against the integrated code: **248 tests, 0 failures**, under both the
Eager and Cooperative rebalance protocols.

The `offsets.isEmpty()` branch was then reverted in `ConsumerCoordinator.java`, restoring the
pre-patch `if (!coordinatorUnknown())`. Result:

```
248 tests completed, 2 failed
FAILED: EagerConsumerCoordinatorTest.testCommitEmptyOffsetsLocally()
FAILED: CooperativeConsumerCoordinatorTest.testCommitEmptyOffsetsLocally()
        org.opentest4j.AssertionFailedError: expected: <1> but was: <0>
```

Exactly 2 of 248, both the same test; nothing else broke. `expected: <1> but was: <0>` means the
callback was never invoked — without the adaptation an empty commit falls into the
discover-coordinator branch and never completes locally. The test detects the absence of the
change, not merely the presence of working code.

One reproducibility note: reverting leaves `import java.time.Duration;` unused, and
`:clients:checkstyleMain` then fails the build *before the tests run*. That failure is not the
can-it-fail result. The import must be deleted as part of the revert.

## The model's own can-it-fail answer was wrong

Asked which test would catch a revert, it named **`testNonEmptyOffsetsWithCoordinatorUnknown`** —
a test it never wrote; the name it actually used was `testCommitNonEmptyOffsetsWithCoordinatorUnknown`.
Its reasoning was also backwards: it claimed a revert would break coordinator discovery for
*non-empty* commits, when the change affects the *empty* path. The test that genuinely catches a
revert is its own first test, which it did not name.

So the model produced a correct test and an incorrect account of why it mattered.

## Coverage

```
method                        INSTRUCTION       BRANCH        LINE
coordinatorUnknownAndUnready   9/11 ->  9/11    3/4 -> 3/4    1/1 -> 1/1
commitOffsetsAsync            21/39 -> 26/39    2/4 -> 3/4    6/9 -> 7/9
doCommitOffsetsAsync          17/20 -> 17/20    1/2 -> 1/2    4/4 -> 4/4
```

One test, one branch, and nothing else moved — so the gain is attributable to this test rather
than to a general rise in module coverage. The branch gained is precisely the one the adaptation
added. Test count 246 -> 248.

Remaining gap, reported rather than hidden: the 4th branch of `commitOffsetsAsync` is still
uncovered — the false path of `coordinatorUnknownAndUnready`. It was uncovered before this test
and this test does not target it.

## Outcome

The model was useful but not trustworthy. It identified the right behaviour to test and wrote a
test whose logic was sound, then expressed it with an invented API, a type error, and a mock that
is not a mock — and explained its own work incorrectly. A developer who could not read Kafka's
test fixture would have had no way to tell the good test from the two unusable ones, or to notice
that the stated reason was wrong. The judgement that salvaged it came from understanding the
adaptation, not from the model.
