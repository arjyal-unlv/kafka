# Kafka PR #12660 — Feedback Round 3

You are correcting ONE failing JUnit 5 regression test
for BatchBuilder.bytesNeeded().

## Previous results

Test 1 passed all six compression-type cases.
Do not regenerate Test 1.

Test 2 failed all six cases.

Actual error:
Expected java.lang.ArithmeticException to be thrown,
but nothing was thrown.

## Why Test 2 is incorrect

Your test currently asserts:

assertThrows(ArithmeticException.class, () -> builder.build());

But the PR modifies integer overflow handling inside
BatchBuilder.bytesNeeded(), NOT inside build().

The intended behavior is to test bytesNeeded() after
the batch has already been built.

## Required test sequence

1. Construct a BatchBuilder.
2. Successfully call builder.build().
3. Call builder.bytesNeeded() with a record whose reported
   size causes integer overflow when added to the batch header.
4. Assert that bytesNeeded() throws ArithmeticException.

Important constraints:
- The serializer is configured when the builder is constructed.
- Do not append huge records before build().
- Use a small ByteBuffer.
- Use JUnit 5 and Java 11.
- Follow Kafka Checkstyle formatting.
- Do not modify production code.
- Do not assert that build() throws ArithmeticException.
- Do not claim the test passes without executing it.

Return exactly ONE complete Java test method.
Explain why your revised test reaches the closed-batch
branch of bytesNeeded().

## Relevant production method
     * @return empty {@link OptionalInt} if there is room for the records to be appended, otherwise
     *         returns the number of bytes needed
     */
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

    private int flushedSizeInBytes() {
        return batchOutput.position() - initialPosition;
    }

    /**
     * Get an estimate of the current size of the appended data. This estimate
     * is precise if no compression is in use.
     *

## Important clarification

The PR changes integer overflow handling in bytesNeeded().

The build() method is used ONLY to close the batch.

Correct sequence:

1. Construct the builder.
2. Successfully execute builder.build().
3. Call builder.bytesNeeded() with a record whose reported size
   causes integer overflow.
4. Assert that bytesNeeded() throws ArithmeticException.

Do not assert that build() throws ArithmeticException.

The previous model incorrectly tested build() and failed all
six parameterized cases with:

Expected java.lang.ArithmeticException to be thrown,
but nothing was thrown.

Generate exactly ONE corrected test method.
Do not reproduce or justify the previous incorrect test.
