# Part B, attempt 01 — generate tests for the empty-offsets path

**Model:** qwen2.5-coder:7b (digest dae161e27b0e, Q4_K_M, 7.6B)
**Sampling:** temperature 0.2, top_p 0.9, seed 42, num_ctx 16384, repeat_penalty 1.15, num_predict 4096
**Hardware:** Apple M4, 24 GB RAM, macOS 15.7.9, ollama 0.40.3

**Target:** PR #12244, integrated by hand as commit 1597d78733 on branch pr-12244.
**Gap being filled:** `commitOffsetsAsync` sits at BRANCH 2/4. The empty-offsets local-completion
path added by that commit is executed by 0 of the 246 currently passing tests.

**Context supplied:** the adaptation diff, the method as it now stands, and 94 lines of the real
test fixture — field declarations, `@BeforeEach` setUp, and one existing async-commit test as a
style example. The fixture is included because without the real field names and helpers the model
cannot produce code that compiles, and "did not compile" would tell us nothing about its
understanding of the change.

---

## Prompt as sent

You are writing JUnit 5 tests for the Apache Kafka consumer client.

A change was recently made to `ConsumerCoordinator.commitOffsetsAsync`. Before the change, an
async commit always checked whether the group coordinator was known before proceeding. After the
change, a commit of an EMPTY offsets map completes locally and does not require a coordinator
at all.

That new empty-offsets path currently has no test. Write tests for it.

(A) is the diff that introduced the change.
(B) is the method as it now stands.
(C) is the real test fixture from ConsumerCoordinatorTest.java - the fields, the setUp method,
    and one existing async-commit test showing the house style. Use these exact names.

Requirements:
- Write up to 3 JUnit 5 test methods, each annotated @Test.
- They must compile against the fixture in (C). Use only the fields and helpers shown there.
- At least one test must FAIL if the change in (A) were reverted. Say which one, and why.
- Do not rewrite the production code. Tests only.

Answer with exactly these two sections:

1. TESTS - the test methods, as Java.
2. WHICH TEST CATCHES A REVERT - name the test and explain what breaks without the change.

--- BEGIN (A) ADAPTATION DIFF ---
commit 1597d78733fb5bc0f3b341658f12375d2d74b46c
Author: ShreemUNLV <arjyal@unlv.nevada.edu>
Date:   Tue Oct 27 10:19:28 2026 -0700

    Manually adapt PR #12244 commit-path fix to 3.0-li (Category 4, full integration)

diff --git a/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java b/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java
index 87485cc4df..e9c2cb784b 100644
--- a/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java
+++ b/clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java
@@ -68,6 +68,7 @@ import org.apache.kafka.common.utils.Utils;
 import org.slf4j.Logger;
 
 import java.nio.ByteBuffer;
+import java.time.Duration;
 import java.util.ArrayList;
 import java.util.Collections;
 import java.util.HashMap;
@@ -961,10 +962,27 @@ public final class ConsumerCoordinator extends AbstractCoordinator {
         }
     }
 
+    private boolean coordinatorUnknownAndUnready(Timer timer) {
+        return coordinatorUnknown() && !ensureCoordinatorReady(timer);
+    }
+
     public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
         invokeCompletedOffsetCommitCallbacks();
 
-        if (!coordinatorUnknown()) {
+        if (offsets.isEmpty()) {
+            // No need to check coordinator if offsets is empty since commit of empty offsets is completed locally.
+            doCommitOffsetsAsync(offsets, callback);
+        } else if (!coordinatorUnknownAndUnready(time.timer(Duration.ZERO))) {
+            // we need to make sure coordinator is ready before committing, since
+            // this is for async committing we do not try to block, but just try once to
+            // clear the previous discover-coordinator future, resend, or get responses;
+            // if the coordinator is not ready yet then we would just proceed and put that into the
+            // pending requests, and future poll calls would still try to complete them.
+            //
+            // the key here though is that we have to try sending the discover-coordinator if
+            // it's not known or ready, since this is the only place we can send such request
+            // under manual assignment (there we would not have heartbeat thread trying to auto-rediscover
+            // the coordinator).
             doCommitOffsetsAsync(offsets, callback);
         } else {
             // we don't know the current coordinator, so try to find it and then send the commit
--- END (A) ADAPTATION DIFF ---

--- BEGIN (B) commitOffsetsAsync AS IT NOW STANDS ---
        }
        while (true) {
            OffsetCommitCompletion completion = completedOffsetCommits.poll();
            if (completion == null) {
                break;
            }
            completion.invoke();
        }
    }

    private boolean coordinatorUnknownAndUnready(Timer timer) {
        return coordinatorUnknown() && !ensureCoordinatorReady(timer);
    }

    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        invokeCompletedOffsetCommitCallbacks();

        if (offsets.isEmpty()) {
            // No need to check coordinator if offsets is empty since commit of empty offsets is completed locally.
            doCommitOffsetsAsync(offsets, callback);
        } else if (!coordinatorUnknownAndUnready(time.timer(Duration.ZERO))) {
            // we need to make sure coordinator is ready before committing, since
            // this is for async committing we do not try to block, but just try once to
            // clear the previous discover-coordinator future, resend, or get responses;
            // if the coordinator is not ready yet then we would just proceed and put that into the
            // pending requests, and future poll calls would still try to complete them.
            //
            // the key here though is that we have to try sending the discover-coordinator if
            // it's not known or ready, since this is the only place we can send such request
            // under manual assignment (there we would not have heartbeat thread trying to auto-rediscover
            // the coordinator).
            doCommitOffsetsAsync(offsets, callback);
        } else {
            // we don't know the current coordinator, so try to find it and then send the commit
            // or fail (we don't want recursive retries which can cause offset commits to arrive
            // out of order). Note that there may be multiple offset commits chained to the same
            // coordinator lookup request. This is fine because the listeners will be invoked in
            // the same order that they were added. Note also that AbstractCoordinator prevents
            // multiple concurrent coordinator lookup requests.
            pendingAsyncCommits.incrementAndGet();
            lookupCoordinator().addListener(new RequestFutureListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    pendingAsyncCommits.decrementAndGet();
                    doCommitOffsetsAsync(offsets, callback);
                    client.pollNoWakeup();
                }

                @Override
                public void onFailure(RuntimeException e) {
                    pendingAsyncCommits.decrementAndGet();
                    completedOffsetCommits.add(new OffsetCommitCompletion(callback, offsets,
                            new RetriableCommitFailedException(e)));
                }
            });
        }
--- END (B) ---

--- BEGIN (C) TEST FIXTURE FROM ConsumerCoordinatorTest.java ---
// ---- fields (lines 150-172) ----
    private final Map<String, MockPartitionAssignor> assignorMap;
    private final String consumerId = "consumer";
    private final String consumerId2 = "consumer2";

    private MockClient client;
    private MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith(1, new HashMap<String, Integer>() {
        {
            put(topic1, 1);
            put(topic2, 1);
        }
    });
    private Node node = metadataResponse.brokers().iterator().next();
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private Metrics metrics;
    private ConsumerNetworkClient consumerClient;
    private MockRebalanceListener rebalanceListener;
    private MockCommitCallback mockOffsetCommitCallback;
    private ConsumerCoordinator coordinator;

    public ConsumerCoordinatorTest(final ConsumerPartitionAssignor.RebalanceProtocol protocol) {
        this.protocol = protocol;


// ---- @BeforeEach setUp (lines 186-215) ----
    @BeforeEach
    public void setup() {
        LogContext logContext = new LogContext();
        this.subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        this.metrics = new Metrics(time);
        this.metadata = new ConsumerMetadata(0, Long.MAX_VALUE, false,
            false, subscriptions, logContext, new ClusterResourceListeners(), metrics);
        this.client = new MockClient(time, metadata);
        this.client.updateMetadata(metadataResponse);
        this.consumerClient = new ConsumerNetworkClient(logContext, client, metadata, time, 100,
                requestTimeoutMs, Integer.MAX_VALUE);
        this.rebalanceListener = new MockRebalanceListener();
        this.mockOffsetCommitCallback = new MockCommitCallback();
        this.partitionAssignor.clear();
        this.rebalanceConfig = buildRebalanceConfig(Optional.empty());
        this.coordinator = buildCoordinator(rebalanceConfig,
                                            metrics,
                                            assignors,
                                            false,
                                            subscriptions);
    }

    private GroupRebalanceConfig buildRebalanceConfig(Optional<String> groupInstanceId) {
        return new GroupRebalanceConfig(sessionTimeoutMs,
                                        rebalanceTimeoutMs,
                                        heartbeatIntervalMs,
                                        groupId,
                                        groupInstanceId,
                                        retryBackoffMs,
                                        !groupInstanceId.isPresent());

// ---- an existing async-commit test, for style (lines 2100-2135) ----

    @Test
    public void testCommitOffsetAsyncCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // async commit with coordinator not available
        MockCommitCallback cb = new MockCommitCallback();
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.COORDINATOR_NOT_AVAILABLE);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), cb);
        coordinator.invokeCompletedOffsetCommitCallbacks();

        assertTrue(coordinator.coordinatorUnknown());
        assertEquals(1, cb.invoked);
        assertTrue(cb.exception instanceof RetriableCommitFailedException);
    }

    @Test
    public void testCommitOffsetAsyncNotCoordinator() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // async commit with not coordinator
        MockCommitCallback cb = new MockCommitCallback();
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.COORDINATOR_NOT_AVAILABLE);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), cb);
        coordinator.invokeCompletedOffsetCommitCallbacks();

        assertTrue(coordinator.coordinatorUnknown());
        assertEquals(1, cb.invoked);
        assertTrue(cb.exception instanceof RetriableCommitFailedException);
    }

    @Test
    public void testCommitOffsetAsyncDisconnected() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
--- END (C) TEST FIXTURE ---
