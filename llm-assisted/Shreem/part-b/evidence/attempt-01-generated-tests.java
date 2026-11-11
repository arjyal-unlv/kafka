@Test
public void testCommitEmptyOffsetsLocally() {
    // Prepare an empty offsets map
    Map<TopicPartition, OffsetAndMetadata> emptyOffsets = Collections.emptyMap();

    // Commit the empty offsets asynchronously
    coordinator.commitOffsetsAsync(emptyOffsets, mockOffsetCommitCallback);

    // Verify that no network request was made to the coordinator
    assertEquals(0, client.numSendRequests());

    // Verify that the callback is invoked locally without errors
    assertTrue(mockOffsetCommitCallback.invoked);
    assertNull(mockOffsetCommitCallback.exception);
}

@Test
public void testCommitEmptyOffsetsWithCoordinatorUnknown() {
    // Prepare an empty offsets map
    Map<TopicPartition, OffsetAndMetadata> emptyOffsets = Collections.emptyMap();

    // Simulate the coordinator being unknown
    when(coordinator.coordinatorUnknown()).thenReturn(true);

    // Commit the empty offsets asynchronously
    coordinator.commitOffsetsAsync(emptyOffsets, mockOffsetCommitCallback);

    // Verify that no network request was made to the coordinator
    assertEquals(0, client.numSendRequests());

    // Verify that the callback is invoked locally without errors
    assertTrue(mockOffsetCommitCallback.invoked);
    assertNull(mockOffsetCommitCallback.exception);
}

@Test
public void testCommitNonEmptyOffsetsWithCoordinatorUnknown() {
    // Prepare a non-empty offsets map
    Map<TopicPartition, OffsetAndMetadata> nonEmptyOffsets = singletonMap(t1p, new OffsetAndMetadata(100L));

    // Simulate the coordinator being unknown
    when(coordinator.coordinatorUnknown()).thenReturn(true);

    // Commit the non-empty offsets asynchronously
    coordinator.commitOffsetsAsync(nonEmptyOffsets, mockOffsetCommitCallback);

    // Verify that a network request was made to discover the coordinator
    assertEquals(1, client.numSendRequests());

    // Verify that the callback is invoked after discovering the coordinator without errors
    assertTrue(mockOffsetCommitCallback.invoked);
    assertNull(mockOffsetCommitCallback.exception);
}
