```scala
  @Test
  def testRecordListConsumerMaxRecordsInBatch(): Unit = {
    val batches = new util.ArrayList[util.List[ApiMessageAndVersion]]
    val consumer = new RecordListConsumer(
      1024,
      (batch: util.List[ApiMessageAndVersion]) =>
        batches.add(new util.ArrayList[ApiMessageAndVersion](batch))
    )
    val records = List.fill(1024)(new ApiMessageAndVersion(new FenceBrokerRecord().setId(1).setEpoch(1), 0.toShort))
    consumer.accept(records.asJava)
    assertEquals(1, batches.size())
    assertEquals(1024, batches.get(0).size())
  }

  @Test
  def testRecordListConsumerMaxRecordsInBatchPlusOne(): Unit = {
    val batches = new util.ArrayList[util.List[ApiMessageAndVersion]]
    val consumer = new RecordListConsumer(
      1024,
      (batch: util.List[ApiMessageAndVersion]) =>
        batches.add(new util.ArrayList[ApiMessageAndVersion](batch))
    )
    val records = List.fill(1025)(new ApiMessageAndVersion(new FenceBrokerRecord().setId(1).setEpoch(1), 0.toShort))
    consumer.accept(records.asJava)
    assertEquals(2, batches.size())
    assertEquals(1024, batches.get(0).size())
    assertEquals(1, batches.get(1).size())
  }

  @Test
  def testRecordListConsumerTwoMaxRecordsInBatchPlusOne(): Unit = {
    val batches = new util.ArrayList[util.List[ApiMessageAndVersion]]
    val consumer = new RecordListConsumer(
      1024,
      (batch: util.List[ApiMessageAndVersion]) =>
        batches.add(new util.ArrayList[ApiMessageAndVersion](batch))
    )
    val records = List.fill(2049)(new ApiMessageAndVersion(new FenceBrokerRecord().setId(1).setEpoch(1), 0.toShort))
    consumer.accept(records.asJava)
    assertEquals(3, batches.size())
    assertEquals(1024, batches.get(0).size())
    assertEquals(1024, batches.get(1).size())
    assertEquals(1, batches.get(2).size())
  }
```

### Explanation of Each Test

1. **testRecordListConsumerMaxRecordsInBatch**:
   - **Purpose**: Verifies that when the number of records is exactly equal to `maxRecordsInBatch`, the `RecordListConsumer` produces exactly one batch containing all records.
   - **Test**: Creates a list of 1024 records and passes it to the `RecordListConsumer`. Checks that the resulting list of batches contains exactly one batch with 1024 records.

2. **testRecordListConsumerMaxRecordsInBatchPlusOne**:
   - **Purpose**: Verifies that when the number of records is `maxRecordsInBatch + 1`, the `RecordListConsumer` produces two batches, with the first batch at the maximum size and the second batch containing exactly one record.
   - **Test**: Creates a list of 1025 records and passes it to the `RecordListConsumer`. Checks that the resulting list of batches contains two batches: one with 1024 records and another with 1 record.

3. **testRecordListConsumerTwoMaxRecordsInBatchPlusOne**:
   - **Purpose**: Verifies that when the number of records is `2 * maxRecordsInBatch + 1`, the `RecordListConsumer` produces three batches, with the first two batches at the maximum size and the third batch containing exactly one record.
   - **Test**: Creates a list of 2049 records and passes it to the `RecordListConsumer`. Checks that the resulting list of batches contains three batches: two with 1024 records each and another with 1 record.

