<!--
model: qwen2.5-coder:7b
options: {'temperature': 0.2, 'top_p': 0.9, 'seed': 42, 'num_ctx': 8192, 'repeat_penalty': 1.15, 'num_predict': 2048}
prompt_file: attempt-01.md
eval_count: 2048 tokens
prompt_eval_count: 3443 tokens
total_duration_s: 146.4
run_at: 2026-11-10 10:00:02
done_reason: length
-->

### PATCH

```diff
diff --git a/clients/src/test/java/org/apache/kafka/clients/producer/internals/SenderTest.java b/clients/src/test/java/org/apache/kafka/clients/producer/internals/SenderTest.java
index 364ca18171..c0701c5cb3 100644
--- a/clients/src/test/java/org/apache/kafka/clients/producer/internals/SenderTest.java
+++ b/clients/src/test/java/org/apache/kafka/clients/producer/internals/SenderTest.java
@@ -32,6 +32,7 @@ import org.apache.kafka.common.Node;
 import org.apache.kafka.common.TopicPartition;
 import org.apache.kafka.common.errors.ClusterAuthorizationException;
 import org.apache.kafka.common.errors.InvalidRequestException;
+import org.apache.kafka.common.errors.InvalidTxnStateException;
 import org.apache.kafka.common.errors.NetworkException;
 import org.apache.kafka.common.errors.RecordTooLargeException;
 import org.apache.kafka.common.errors.TimeoutException;
@@ -3085,7 +3086,7 @@ public class SenderTest {
 
         time.sleep(20);
 
-        sendIdempotentProducerResponse(0, tp0, Errors.INVALID_RECORD, -1);
+        sendIdempotentProducerResponse(0, tp0, Errors.INVALID_TXN_STATE, -1);
         sender.runOnce(); // receive late response
 
         // Loop once and confirm that the transaction manager does not enter a fatal error state
@@ -3107,6 +3108,45 @@ public class SenderTest {
         txnManager.beginTransaction();
     }
 
+    @Test
+    public void testInvalidTxnStateIsAnAbortableError() throws Exception {
+        ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(123456L, (short) 0);
+        apiVersions.update("0", NodeApiVersions.create(ApiKeys.INIT_PRODUCER_ID.id, (short) 0, (short) 3));
+        TransactionManager txnManager = new TransactionManager(logContext, "testInvalidTxnState", 60000, 100, apiVersions);
+
+        setupWithTransactionState(txnManager);
+        doInitTransactions(txnManager, producerIdAndEpoch);
+
+        txnManager.beginTransaction();
+        txnManager.maybeAddPartition(tp0);
+        client.prepareResponse(buildAddPartitionsToTxnResponseData(0, Collections.singletonMap(tp0, Errors.NONE)));
+        sender.runOnce();
+
+        Future<RecordMetadata> request = appendToAccumulator(tp0);
+        sender.runOnce();  // send request
+        sendIdempotentProducerResponse(0, tp0, Errors.INVALID_TXN_STATE, -1);
+
+        // Return InvalidTxnState error. It should be abortable.
+        sender.runOnce();
+        assertFutureFailure(request, InvalidTxnStateException.class);
+        assertTrue(txnManager.hasAbortableError());
+        TransactionalRequestResult result = txnManager.beginAbort();
+        sender.runOnce();
+
+        // Once the transaction is aborted, we should be able to begin a new one.
+        respondToEndTxn(Errors.NONE);
+        sender.runOnce();
+        assertTrue(txnManager::isInitializing);
+        prepareInitProducerResponse(Errors.NONE, producerIdAndEpoch.producerId, producerIdAndEpoch.epoch);
+        sender.runOnce();
+        assertTrue(txnManager::isReady);
+
+        assertTrue(result.isSuccessful());
+        result.await();
+
+        txnManager.beginTransaction();
+    }
+
     private void verifyErrorMessage(ProduceResponse response, String expectedMessage) throws Exception {
         Future<RecordMetadata> future = appendToAccumulator(tp0, 0L, "key", "value");
         sender.runOnce(); // connect
diff --git a/core/src/main/scala/kafka/log/UnifiedLog.scala b/core/src/main/scala/kafka/log/UnifiedLog.scala
index 2ddaa284fc..028f2fbbd5 100644
--- a/core/src/main/scala/kafka/log/UnifiedLog.scala
+++ b/core/src/main/scala/kafka/log/UnifiedLog.scala
@@ -1031,7 +1031,7 @@ class UnifiedLog(@volatile var logStartOffset: Long,
           // ongoing. If the transaction is expected to be ongoing, we will not set a verification guard. If the transaction is aborted, hasOngoingTransaction is false and
           // requestVerificationGuard is null, so we will throw an error. A subsequent produce request (retry) should create verification state and return to phase 1.
           if (batch.isTransactional && !hasOngoingTransaction(batch.producerId) && batchMissingRequiredVerification(batch, requestVerificationGuard))
-            throw new InvalidRecordException("Record was not part of an ongoing transaction")
+            throw new InvalidTxnStateException("Record was not part of an ongoing transaction")
         }
 
         // We cache offset metadata for the start of each transaction. This allows us to
diff --git a/core/src/main/scala/kafka/server/AddPartitionsToTxnManager.scala b/core/src/main/scala/kafka/server/AddPartitionsToTxnManager.scala
index fc5705042f..05e4001466 100644
--- a/core/src/main/scala/kafka/server/AddPartitionsToTxnManager.scala
+++ b/core/src/main/scala/kafka/server/AddPartitionsToTxnManager.scala
@@ -142,9 +142,9 @@ class AddPartitionsToTxnManager(config: KafkaConfig, client: NetworkClient, time
         if (addPartitionsToTxnResponseData.errorCode != 0) {
           error(s"AddPartitionsToTxnRequest for node ${response.destination} returned with error ${Errors.forCode(addPartitionsToTxnResponseData.errorCode)}.")
           // The client should not be exposed to CLUSTER_AUTHORIZATION_FAILED so modify the error to signify the verification did not complete.
-          // Older clients return with INVALID_RECORD and newer ones can return with INVALID_TXN_STATE.
+          // Return INVALID_TXN_STATE.
           val finalError = if (addPartitionsToTxnResponseData.errorCode == Errors.CLUSTER_AUTHORIZATION_FAILED.code)
-            Errors.INVALID_RECORD.code
+            Errors.INVALID_TXN_STATE.code
           else
             addPartitionsToTxnResponseData.errorCode
 
@@ -160,9 +160,6 @@ class AddPartitionsToTxnManager(config: KafkaConfig, client: NetworkClient, time
                   val code =
                     if (partitionResult.partitionErrorCode == Errors.PRODUCER_FENCED.code)
                       Errors.INVALID_PRODUCER_EPOCH.code
-                    // Older clients return INVALID_RECORD
-                    else if (partitionResult.partitionErrorCode == Errors.INVALID_TXN_STATE.code)
-                      Errors.INVALID_RECORD.code
                     else
                       partitionResult.partitionErrorCode
                   unverified.put(tp, Errors.forCode(code))
diff --git a/core/src/main/scala/kafka/server/ReplicaManager.scala b/core/src/main/scala/kafka/server/ReplicaManager.scala
index 69d291f84f..d56762a151 100644
--- a/core/src/main/scala/kafka/server/ReplicaManager.scala
+++ b/core/src/main/scala/kafka/server/ReplicaManager.scala
@@ -732,8 +732,7 @@ class ReplicaManager(val config: KafkaConfig,
         debug("Produce to local log in %d ms".format(time.milliseconds - sTime))
         
         val unverifiedResults = unverifiedEntries.map { case (topicPartition, error) =>
-          // NOTE: Older clients return INVALID_RECORD, but newer clients will return INVALID_TXN_STATE
-          val message = if (error.equals(Errors.INVALID_RECORD)) "Partition was not added to the transaction" else error.message()
+          val message = if (error == Errors.INVALID_TXN_STATE) "Partition was not added to the transaction" else error.message()
           topicPartition -> LogAppendResult(
             LogAppendInfo.UNKNOWN_LOG_APPEND_INFO,
             Some(error.exception(message))
diff --git a/core/src/test/scala/unit/kafka/cluster/PartitionTest.scala b/core/src/test/scala/unit/kafka/cluster/PartitionTest.scala
index 131db5e903..7e346badbb 100644
--- a/core/src/test/scala/unit/kafka/cluster/PartitionTest.scala
+++ b/core/src/test/scala/unit/kafka/cluster/PartitionTest.scala
@@ -24,7 +24,7 @@ import kafka.server._
 import kafka.server.checkpoints.OffsetCheckpoints
 import kafka.utils._
 import kafka.zk.KafkaZkClient
-import org.apache.kafka.common.errors.{ApiException, FencedLeaderEpochException, InconsistentTopicIdException, NotLeaderOrFollowerException, OffsetNotAvailableException, OffsetOutOfRangeException, UnknownLeaderEpochException}
+import org.apache.kafka.common.errors.{ApiException, FencedLeaderEpochException, InconsistentTopicIdException, InvalidTxnStateException, NotLeaderOrFollowerException, OffsetNotAvailableException, OffsetOutOfRangeException, UnknownLeaderEpochException}
 import org.apache.kafka.common.message.{AlterPartitionResponseData, FetchResponseData}
 import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
 import org.apache.kafka.common.protocol.{ApiKeys, Errors}
@@ -32,7 +