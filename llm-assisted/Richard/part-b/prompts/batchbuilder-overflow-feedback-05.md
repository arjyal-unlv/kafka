# Kafka PR #12660 — Targeted Feedback Round 5

You are correcting ONE remaining issue in a JUnit 5 regression
test generated for Apache Kafka BatchBuilder.

## Validation status

The Round 4 test has NOT yet been compiled or executed.

Its implementation appears to follow the intended sequence:
1. Construct the BatchBuilder.
2. Successfully call builder.build().
3. Invoke builder.bytesNeeded() after the batch is closed.
4. Assert that bytesNeeded() throws ArithmeticException.

However, the generated method name conflicts with an existing
manually written test in BatchBuilderTest.java.

## Actual problem

The existing manually written test is named:

testBytesNeededThrowsOnIntegerOverflow

Your generated test uses the same method name.

This would cause a duplicate Java method declaration.

## Required correction

Rename ONLY the generated test method to:

testBytesNeededThrowsOnIntegerOverflowWhenBatchIsClosed

## Strict constraints

1. Preserve the existing test implementation exactly.
2. Do not change the serializer or reported record size.
3. Do not change the BatchBuilder constructor.
4. Do not change the order of build() and bytesNeeded().
5. Do not modify the exception assertion.
6. Do not change the stack-trace assertion.
7. Do not introduce new tests, imports, or classes.
8. Return exactly ONE complete Java test method.
9. Do not claim that the test passes.
10. Follow Kafka Checkstyle indentation.

## Previous Round 4 response

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

## Final instruction

Return the complete corrected test method with ONLY its method
name changed. Briefly identify the correction made.
