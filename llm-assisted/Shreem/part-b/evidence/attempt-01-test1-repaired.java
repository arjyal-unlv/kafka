    @Test
    public void testCommitEmptyOffsetsLocally() {
        // The coordinator is deliberately NOT discovered. The empty-offsets path added by
        // commit 1597d78733 must complete locally without ever needing one.
        assertTrue(coordinator.coordinatorUnknown());

        int before = mockOffsetCommitCallback.invoked;
        coordinator.commitOffsetsAsync(Collections.emptyMap(), mockOffsetCommitCallback);
        coordinator.invokeCompletedOffsetCommitCallbacks();

        assertEquals(before + 1, mockOffsetCommitCallback.invoked);
        assertNull(mockOffsetCommitCallback.exception);
        assertTrue(coordinator.coordinatorUnknown());
    }
