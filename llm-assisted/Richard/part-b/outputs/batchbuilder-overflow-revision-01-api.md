### Revised Test 1: Verify that an ArithmeticException is thrown when the batch is open and adding the batch header size to the required record bytes overflow overflows.

```java
@ParameterizedTest
@EnumSource(CompressionType.class)
void testBytesNeededThrowsOnIntegerOverflowWhenBatchIsOpen(CompressionType compressionType) {
    StringSerde largeRecordSerde = new StringSerde() {
        @Override
        public int recordSize(
            String data,
            ObjectSerializationCache serializationCache
        ) {
            return Integer.MAX_VALUE - 20;
        }
    };

    ByteBuffer buffer = ByteBuffer.allocate(1024);

    BatchBuilder<String> builder = new BatchBuilder<>(
        buffer,
        largeRecordSerde,
        compressionType,
        0L,
        time.milliseconds(),
        false,
        1,
        buffer.limit()
    );

    // Fill the buffer to make it full
    builder.appendRecord("a", null);
    builder.appendRecord("ap", null);
    builder.appendRecord("app", null);
    builder.appendRecord("appl", null);
    builder.appendRecord("apple", null);

    ArithmeticException exception = assertThrows(
        ArithmeticException.class,
        () -> builder.bytesNeeded(Arrays.asList("large-record"), null)
    );

    assertTrue(
        Arrays.stream(exception.getStackTrace())
            .anyMatch(frame ->
                frame.getClassName().equals(BatchBuilder.class.getName())
                    && frame.getMethodName().equals("bytesNeeded")
            ),
        "Overflow must originate from BatchBuilder.bytesNeeded()"
    );
}
```

**Explanation:**
No changes were necessary in this method. The indentation was already correct, and the test behavior was preserved.

### Revised Test 2: Verify that an ArithmeticException is thrown when the batch is built and adding the batch header size to the required record bytes overflow overflows.

```java
@ParameterizedTest
@EnumSource(CompressionType.class)
void testBuildThrowsOnIntegerOverflow(CompressionType compressionType) {
    StringSerde largeRecordSerde = new StringSerde() {
        @Override
        public int recordSize(
            String data,
            ObjectSerializationCache serializationCache
        ) {
            return Integer.MAX_VALUE - 20;
        }
    };

    ByteBuffer buffer = ByteBuffer.allocate(1024);

    BatchBuilder<String> builder = new BatchBuilder<>(
        buffer,
        largeRecordSerde,
        compressionType,
        0L,
        time.milliseconds(),
        false,
        1,
        buffer.limit()
    );

    // Fill the buffer to make it full
    builder.appendRecord("a", null);
    builder.appendRecord("ap", null);
    builder.appendRecord("app", null);
    builder.appendRecord("appl", null);
    builder.appendRecord("apple", null);

    ArithmeticException exception = assertThrows(
        ArithmeticException.class,
        () -> builder.build()
    );

    assertTrue(
        Arrays.stream(exception.getStackTrace())
            .anyMatch(frame ->
                frame.getClassName().equals(BatchBuilder.class.getName())
                    && frame.getMethodName().equals("writeRecord")
            ),
        "Overflow must originate from BatchBuilder.writeRecord()"
    );
}
```

**Explanation:**
No changes were necessary in this method. The indentation was already correct, and the test behavior was preserved.

Both tests have been revised to ensure that they are compatible with Java 11 and JUnit 5, use only existing Apache Kafka APIs, and do not modify production code. The tests aim to verify integer overflow behavior in the `BatchBuilder.bytesNeeded()` and `BatchBuilder.build()` methods.