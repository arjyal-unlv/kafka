# Kafka PR #12660 — Targeted Feedback Round 4

You are revising ONE JUnit 5 regression test for Apache Kafka
3.0.1-SNAPSHOT, Java 11.

## Objective

Test integer overflow in BatchBuilder.bytesNeeded() AFTER the
batch has been closed by a successful builder.build() call.

The production fix uses:

Math.addExact(batchHeaderSizeInBytes(), bytesNeeded)

The test must exercise the CLOSED-batch branch containing
this operation.

## Previous experimental results

- Original open-batch test: 6/6 parameterized cases passed.
- Original build-overflow test: 0/6 passed.
- Round 3 identified the correct sequence but generated
  incompatible Kafka APIs.
- Round 3 has NOT been compiled or executed.

## Errors in Round 3

1. You generated an entire BatchBuilderTest class instead of
   a single test method.

2. You used an incompatible constructor:

   new BatchBuilder<>(buffer, 1024, serializer, 0)

3. You introduced Serializer<SimpleRecord> and TestSerializer
   instead of the existing StringSerde infrastructure.

4. You used:

   buffer.putInt(Integer.MAX_VALUE);

   This writes four bytes into the buffer and does not cause
   integer overflow in bytesNeeded().

## Required implementation

1. Generate exactly ONE JUnit 5 test method.

2. Use the existing BatchBuilderTest infrastructure:
   - StringSerde
   - CompressionType.NONE
   - ObjectSerializationCache
   - ByteBuffer
   - time.milliseconds()
   - assertThrows

3. You may override StringSerde.recordSize() to return a very
   large integer without allocating a huge record or buffer.

4. Use the actual eight-argument BatchBuilder constructor
   shown in the working example below.

5. Successfully call builder.build() BEFORE invoking
   builder.bytesNeeded().

6. Do not append huge records before build().

7. Assert that builder.bytesNeeded() throws ArithmeticException.

8. Do not assert that builder.build() throws an exception.

9. Do not modify production code.

10. Follow Kafka Checkstyle indentation.

11. Do not claim the test passes until it has been executed.

## Important clarification

The purpose of calling build() is ONLY to transition the
builder into its closed state.

The overflow must occur when bytesNeeded() calculates the
required bytes for an additional record.

The intended sequence is:

1. Construct the builder.
2. Successfully call builder.build().
3. Call builder.bytesNeeded() with a record whose reported size
   causes integer overflow when combined with the batch header.
4. Assert that bytesNeeded() throws ArithmeticException.

## Actual production method

```java
public OptionalInt bytesNeeded(Collection<T> records, ObjectSerializationCache serializationCache) {
        int bytesNeeded = bytesNeededForRecords(
            records,
            serializationCache
        );

        if (!isOpenForAppends) {
            return OptionalInt.of(Math.addExact(batchHeaderSizeInBytes(), bytesNeeded));
        }

        int approxUnusedSizeInBytes = maxBytes - approximateSizeInBytes();
        if (approxUnusedSizeInBytes >= bytesNeeded) {
            return OptionalInt.empty();
        } else if (unflushedBytes > 0) {
            recordOutput.flush();
            unflushedBytes = 0;
            int unusedSizeInBytes = maxBytes - flushedSizeInBytes();
            if (unusedSizeInBytes >= bytesNeeded) {
                return OptionalInt.empty();
            }
        }

        return OptionalInt.of(Math.addExact(batchHeaderSizeInBytes(), bytesNeeded));
    }
```

## Existing project-compatible test example

The following manually written test compiles in this Kafka
project. Use its constructor and serialization approach,
but create a DISTINCT closed-batch test.

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

## Required response

Return exactly ONE complete Java test method.

Requirements:
- The method must be compatible with the existing BatchBuilderTest.
- Do not generate imports or a new test class.
- Do not introduce new serializer classes.
- Use the existing eight-argument constructor.
- Do not use buffer.putInt(Integer.MAX_VALUE).
- Do not expect build() to throw.
- The assertion must invoke bytesNeeded().
- Follow Kafka Checkstyle formatting.

After the method, briefly explain:
1. Why build() completes successfully.
2. Why bytesNeeded() enters the closed-batch branch.
3. How the record size triggers Math.addExact overflow.

Do not claim the test passes without executing it.
