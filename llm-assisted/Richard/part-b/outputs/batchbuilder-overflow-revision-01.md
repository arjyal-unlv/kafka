### Revised Test 1: Verify that an ArithmeticException is thrown when the b
batch is 
open and adding the batch header size to the required record bytes overflow
overflow
overflows.

```java
@ParameterizedTest
@EnumSource(CompressionType.class)
void testBytesNeededThrowsOnIntegerOverflowWhenBatchIsOpen(CompressionType 

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

**Modifications:**
1. Fixed the indentation error by aligning the method body with the method 
signature.
2. Kept the original test logic and structure.

### Revised Test 2: Verify that an ArithmeticException is thrown when the b
batch is 
built and adding the batch header size to the required record bytes overflo
overflo
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

**Modifications:**
1. Fixed the indentation error by aligning the method body with the method 
signature.
2. Kept the original test logic and structure.

These modifications ensure that both tests adhere to the Checkstyle indenta
indentation requirements while preserving their original functionality and 
behavior.

