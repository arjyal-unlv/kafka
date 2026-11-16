You are generating regression tests for a successfully integrated Apache Kafka patch
adapted to linkedin/kafka 3.0-li.

PR: Apache Kafka #12486

PURPOSE OF THE CHANGE

The integrated change introduces RecordListConsumer, which splits a list of
ApiMessageAndVersion records into batches whose size does not exceed
maxRecordsInBatch before passing each batch to an append function.

The production adaptation has already been integrated successfully.

Your task is ONLY to generate additional tests. Do not modify production code.

TARGET TEST FRAMEWORK

The target project uses Scala tests. Follow the style, imports, assertions,
and conventions demonstrated by the existing test supplied below.

BEHAVIOR TO TEST

Generate focused regression tests for RecordListConsumer that add useful
behavioral coverage beyond the existing test.

Prioritize these boundary conditions:

1. The number of records is exactly equal to maxRecordsInBatch.
   Expected result: exactly one append call containing all records.

2. The number of records is maxRecordsInBatch + 1.
   Expected result: two append calls, with the first batch at the maximum size
   and the second batch containing exactly one record.

3. If useful and not redundant, test a larger non-even multiple such as
   2 * maxRecordsInBatch + 1 to verify repeated splitting.

Requirements:

- Use only APIs visible in the supplied target code and test example.
- Do not subclass SnapshotWriter.
- Do not invent helper methods or Kafka APIs.
- Keep the tests small and behavior-focused.
- Generate complete Scala test methods that can be inserted into
  BrokerMetadataSnapshotterTest.scala.
- Explain briefly what each generated test verifies.

PRODUCTION CODE

33-            lastContainedLogTime: Long): SnapshotWriter[ApiMessageAndVersion]
34-}
35-
36-/**
37- * The RecordListConsumer takes as input a potentially long list of records, and feeds the
38- * SnapshotWriter a series of smaller lists of records.
39- *
40- * Note: from the perspective of Kafka, the snapshot file is really just a list of records,
41- * and we don't care about batches. Batching is irrelevant to the meaning of the snapshot.
42- */
43:class RecordListConsumer(
44-  val maxRecordsInBatch: Int,
45-  val appendBatch: java.util.List[ApiMessageAndVersion] => Unit
46-) extends Consumer[java.util.List[ApiMessageAndVersion]] {
47-  override def accept(messages: java.util.List[ApiMessageAndVersion]): Unit = {
48-    var i = 0
49-    while (i < messages.size()) {
50-      appendBatch(messages.subList(i, Math.min(i + maxRecordsInBatch, messages.size())))
51-      i += maxRecordsInBatch
52-    }
53-  }
54-}
55-
56-class BrokerMetadataSnapshotter(
57-  brokerId: Int,
58-  val time: Time,
59-  threadNamePrefix: Option[String],
60-  writerBuilder: SnapshotWriterBuilder
61-) extends Logging with MetadataSnapshotter {
62-  /**
63-   * The maximum number of records we will put in each batch.
64-   */
65-  private val maxRecordsInBatch = 1024
66-
67-  private val logContext = new LogContext(s"[BrokerMetadataSnapshotter id=${brokerId}] ")
68-  logIdent = logContext.logPrefix()
69-
70-  /**
71-   * The offset of the snapshot in progress, or -1 if there isn't one. Accessed only under
72-   * the object lock.
73-   */
74-  private var _currentSnapshotOffset = -1L
75-
76-  /**
77-   * The event queue which runs this listener.
78-   */


EXISTING TARGET TEST EXAMPLE

93-    val writerBuilder = new MockSnapshotWriterBuilder()
94-    val snapshotter = new BrokerMetadataSnapshotter(0, Time.SYSTEM, None, writerBuilder)
95-    try {
96-      val blockingEvent = new BlockingEvent()
97-      snapshotter.eventQueue.append(blockingEvent)
98-      assertTrue(snapshotter.maybeStartSnapshot(123L, 12, 10000L, MetadataImageTest.IMAGE1))
99-      assertFalse(snapshotter.maybeStartSnapshot(124L, 12, 11000L, MetadataImageTest.IMAGE2))
100-      blockingEvent.latch.countDown()
101-      assertEquals(MetadataImageTest.IMAGE1, writerBuilder.image.get())
102-    } finally {
103-      snapshotter.close()
104-    }
105-  }
106-
107-  @Test
108:  def testRecordListConsumer(): Unit = {
109-    val batches = new util.ArrayList[util.List[ApiMessageAndVersion]]
110-
111-    val consumer = new RecordListConsumer(
112-      3,
113-      (batch: util.List[ApiMessageAndVersion]) =>
114-        batches.add(new util.ArrayList[ApiMessageAndVersion](batch))
115-    )
116-
117-    val m = new ApiMessageAndVersion(
118-      new FenceBrokerRecord().setId(1).setEpoch(1),
119-      0.toShort
120-    )
121-
122-    consumer.accept(asList(m, m))
123-    assertEquals(asList(asList(m, m)), batches)
124-
125-    consumer.accept(asList(m))
126-    assertEquals(asList(asList(m, m), asList(m)), batches)
127-
128-    consumer.accept(asList(m, m, m, m))
129-    assertEquals(
130-      asList(asList(m, m), asList(m), asList(m, m, m), asList(m)),
131-      batches
132-    )
133-
134-    consumer.accept(asList(m, m, m, m, m, m, m, m))
135-    assertEquals(
136-      asList(
137-        asList(m, m),
138-        asList(m),
139-        asList(m, m, m),
140-        asList(m),
141-        asList(m, m, m),
142-        asList(m, m, m),
143-        asList(m, m)
144-      ),
145-      batches
146-    )
147-  }
148-}


OUTPUT FORMAT

Return:

1. Test method(s) only in one Scala code block.
2. A short explanation of each test.
3. Do not modify production code.
