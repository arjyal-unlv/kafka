```java
@Test
void testBytesNeededThrowsOnIntegerOverflow() {
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

### Explanation:

1. **Why `build()` completes successfully:**
   - The `build()` method is called before `bytesNeeded()`. This method transitions the builder into its closed state by finalizing the batch header and preparing for no further appends.

2. **Why `bytesNeeded()` enters the closed-batch branch:**
   - After `build()` is called, the `isOpenForAppends` flag is set to `false`, indicating that the batch is now closed. When `bytesNeeded()` is called, it checks this flag and proceeds to the closed-batch branch where it calculates the required bytes for the batch header and the additional records.

3. **How the record size triggers `Math.addExact` overflow:**
   - The `recordSize()` method of the `largeRecordSerde` returns `Integer.MAX_VALUE - 20`. When `bytesNeeded()` is called with this record, it attempts to calculate the total bytes needed by adding the batch header size and the record size using `Math.addExact()`. Since the record size is very close to `Integer.MAX_VALUE`, adding it to the batch header size causes an integer overflow, which is then caught by the `ArithmeticException`.