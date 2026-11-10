```java
@Test
void testBytesNeededThrowsOnIntegerOverflowWhenBatchIsClosed() {
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
        CompressionType.NONE,
        0L,
        time.milliseconds(),
        false,
        1,
        buffer.limit()
    );

    builder.build();

    ArithmeticException exception = assertThrows(
        ArithmeticException.class,
        () -> builder.bytesNeeded(
            Arrays.asList("large-record"), null
        )
    );

    assertTrue(
        Arrays.stream(exception.getStackTrace()).anyMatch(frame ->
            frame.getClassName().equals(BatchBuilder.class.getName())
                && frame.getMethodName().equals("bytesNeeded")
        ),
        "Overflow must originate from BatchBuilder.bytesNeeded()"
    );
}
```

### Correction Made:
- Renamed the test method from `testBytesNeededThrowsOnIntegerOverflow` to `testBytesNeededThrowsOnIntegerOverflowWhenBatchIsClosed`.