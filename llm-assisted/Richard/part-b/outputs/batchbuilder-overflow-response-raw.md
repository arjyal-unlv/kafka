### Test 1: Verify that an ArithmeticException is thrown when the batch is [K
open and adding the batch header size to the required record bytes overflow[8D[K
overflows.

```java
@ParameterizedTest
@EnumSource(CompressionType.class)
void testBytesNeededThrowsOnIntegerOverflowWhenBatchIsOpen(CompressionType [K
compressionType) {
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

### Test 2: Verify that an ArithmeticException is thrown when the batch is [K
built and adding the batch header size to the required record bytes overflo[7D[K
overflows.

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

These tests check the behavior of the `BatchBuilder` when the buffer is ful[3D[K
full and attempting to add more records would cause an integer overflow. Th[2D[K
The first test ensures that an `ArithmeticException` is thrown when the `by[3D[K
`bytesNeeded` method is called while the batch is still open. The second te[2D[K
test ensures that an `ArithmeticException` is thrown when the `build` metho[5D[K
method is called after the buffer is full and adding more records would cau[3D[K
cause an integer overflow.

