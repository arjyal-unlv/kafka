```java
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Serializer;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class BatchBuilderTest {

    private Serializer<SimpleRecord> serializer;
    private ByteBuffer buffer;
    private BatchBuilder<SimpleRecord> builder;

    @BeforeEach
    public void setUp() {
        serializer = new TestSerializer();
        buffer = ByteBuffer.allocate(1024);
        builder = new BatchBuilder<>(buffer, 1024, serializer, 0);
    }

    @Test
    public void testBytesNeededThrowsArithmeticExceptionForIntegerOverflow() {
        // Step 1: Construct the builder
        // Step 2: Successfully execute builder.build()
        builder.build();

        // Step 3: Call builder.bytesNeeded() with a record whose reported size causes integer overflow
        Record record = new SimpleRecord(0, Collections.singletonList("large".getBytes()));
        buffer.putInt(Integer.MAX_VALUE); // Simulate integer overflow
        assertThrows(ArithmeticException.class, () -> builder.bytesNeeded(Collections.singletonList(record), new ObjectSerializationCache()));
    }

    // Mock serializer for testing purposes
    private static class TestSerializer implements Serializer<SimpleRecord> {
        @Override
        public byte[] serialize(String topic, SimpleRecord data) {
            return Utils.toArray(data.value());
        }
    }
}
```

### Explanation
1. **Setup**: 
   - We initialize a `BatchBuilder` with a small `ByteBuffer` to simulate integer overflow scenarios.
   - We use a mock `Serializer` to ensure the test does not depend on external dependencies.

2. **Test Method**:
   - We first construct the `BatchBuilder`.
   - We then successfully call `builder.build()` to close the batch.
   - We simulate a record that would cause integer overflow when added to the batch header.
   - We call `builder.bytesNeeded()` with this record and assert that it throws an `ArithmeticException`.

This test ensures that the `bytesNeeded()` method behaves correctly when the batch is already closed and integer overflow occurs.