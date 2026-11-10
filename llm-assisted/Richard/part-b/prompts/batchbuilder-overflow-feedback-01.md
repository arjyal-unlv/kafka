# LLM-Assisted Regression Test Generation — Feedback Round 1

You previously generated two JUnit 5 regression tests for
Apache Kafka PR #12660.

## Validation results

The generated tests were inserted into BatchBuilderTest.java.

Gradle successfully completed:
- :raft:compileJava
- :raft:compileTestJava
- :raft:testClasses

However, the build failed during :raft:checkstyleTest.

Actual error:

BatchBuilderTest.java:47:5:
'compressionType' has incorrect indentation level 4,
expected level should be 8.

The test methods have NOT yet been executed.

## Your task

Revise BOTH generated tests.

Requirements:

1. Fix the reported Checkstyle indentation error.
2. Review both methods for similar formatting problems.
3. Preserve the original intended test behavior.
4. Keep the tests compatible with Java 11 and JUnit 5.
5. Use only existing Apache Kafka APIs.
6. Do not modify production code.
7. Do not claim that the revised tests compile or pass.
8. Return both complete revised Java test methods.
9. Briefly explain every modification.

Important:
The objective is to test integer overflow in
BatchBuilder.bytesNeeded(), including open-batch and
closed-batch behavior.

Do not introduce unrelated changes.

## Original generated response
### Test 1: Verify that an ArithmeticException is thrown when the batch is 
open and adding the batch header size to the required record bytes overflow
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

### Test 2: Verify that an ArithmeticException is thrown when the batch is 
built and adding the batch header size to the required record bytes overflo
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

These tests check the behavior of the `BatchBuilder` when the buffer is ful
full and attempting to add more records would cause an integer overflow. Th
The first test ensures that an `ArithmeticException` is thrown when the `by
`bytesNeeded` method is called while the batch is still open. The second te
test ensures that an `ArithmeticException` is thrown when the `build` metho
method is called after the buffer is full and adding more records would cau
cause an integer overflow.

