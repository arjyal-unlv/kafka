    /**
     * Handle the revoked partitions and prepare for closing the associated tasks in {@link #handleAssignment(Map, Map)}
     * We should commit the revoking tasks first before suspending them as we will not officially own them anymore when
     * {@link #handleAssignment(Map, Map)} is called. Note that only active task partitions are passed in from the
     * rebalance listener, so we only need to consider/commit active tasks here
     *
     * If eos-v2 is used, we must commit ALL tasks. Otherwise, we can just commit those (active) tasks which are revoked
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void handleRevocation(final Collection<TopicPartition> revokedPartitions) {
        final Set<TopicPartition> remainingRevokedPartitions = new HashSet<>(revokedPartitions);

        final Set<Task> revokedActiveTasks = new HashSet<>();
        final Set<Task> commitNeededActiveTasks = new HashSet<>();
        final Map<Task, Map<TopicPartition, OffsetAndMetadata>> consumedOffsetsPerTask = new HashMap<>();
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        for (final Task task : activeTaskIterable()) {
            if (remainingRevokedPartitions.containsAll(task.inputPartitions())) {
                // when the task input partitions are included in the revoked list,
                // this is an active task and should be revoked
                revokedActiveTasks.add(task);
                remainingRevokedPartitions.removeAll(task.inputPartitions());
            } else if (task.commitNeeded()) {
                commitNeededActiveTasks.add(task);
            }
        }

        if (!remainingRevokedPartitions.isEmpty()) {
            log.warn("The following partitions {} are missing from the task partitions. It could potentially " +
                         "due to race condition of consumer detecting the heartbeat failure, or the tasks " +
                         "have been cleaned up by the handleAssignment callback.", remainingRevokedPartitions);
        }

        prepareCommitAndAddOffsetsToMap(revokedActiveTasks, consumedOffsetsPerTask);

        // if we need to commit any revoking task then we just commit all of those needed committing together
        final boolean shouldCommitAdditionalTasks = !consumedOffsetsPerTask.isEmpty();
        if (shouldCommitAdditionalTasks) {
            prepareCommitAndAddOffsetsToMap(commitNeededActiveTasks, consumedOffsetsPerTask);
        }

        // even if commit failed, we should still continue and complete suspending those tasks, so we would capture
        // any exception and rethrow it at the end. some exceptions may be handled immediately and then swallowed,
        // as such we just need to skip those dirty tasks in the checkpoint
        final Set<Task> dirtyTasks = new HashSet<>();
        try {
            // in handleRevocation we must call commitOffsetsOrTransaction() directly rather than
            // commitAndFillInConsumedOffsetsAndMetadataPerTaskMap() to make sure we don't skip the
            // offset commit because we are in a rebalance
            commitOffsetsOrTransaction(consumedOffsetsPerTask);
        } catch (final TaskCorruptedException e) {
            log.warn("Some tasks were corrupted when trying to commit offsets, these will be cleaned and revived: {}",
                     e.corruptedTasks());

            // If we hit a TaskCorruptedException it must be EOS, just handle the cleanup for those corrupted tasks right here
            dirtyTasks.addAll(tasks.tasks(e.corruptedTasks()));
            closeDirtyAndRevive(dirtyTasks, true);
        } catch (final TimeoutException e) {
            log.warn("Timed out while trying to commit all tasks during revocation, these will be cleaned and revived");

            // If we hit a TimeoutException it must be ALOS, just close dirty and revive without wiping the state
            dirtyTasks.addAll(consumedOffsetsPerTask.keySet());
            closeDirtyAndRevive(dirtyTasks, false);
        } catch (final RuntimeException e) {
            log.error("Exception caught while committing those revoked tasks " + revokedActiveTasks, e);
            firstException.compareAndSet(null, e);
            dirtyTasks.addAll(consumedOffsetsPerTask.keySet());
        }

        // we enforce checkpointing upon suspending a task: if it is resumed later we just proceed normally, if it is
        // going to be closed we would checkpoint by then
        for (final Task task : revokedActiveTasks) {
            if (!dirtyTasks.contains(task)) {
                try {
                    task.postCommit(true);
                } catch (final RuntimeException e) {
                    log.error("Exception caught while post-committing task " + task.id(), e);
                    firstException.compareAndSet(null, e);
                }
            }
        }

        if (shouldCommitAdditionalTasks) {
            for (final Task task : commitNeededActiveTasks) {
                if (!dirtyTasks.contains(task)) {
                    try {
                        // for non-revoking active tasks, we should not enforce checkpoint
                        // since if it is EOS enabled, no checkpoint should be written while
                        // the task is in RUNNING tate
                        task.postCommit(false);
                    } catch (final RuntimeException e) {
                        log.error("Exception caught while post-committing task " + task.id(), e);
                        firstException.compareAndSet(null, e);
                    }
                }
            }
        }

        for (final Task task : revokedActiveTasks) {
            try {
                task.suspend();
            } catch (final RuntimeException e) {
                log.error("Caught the following exception while trying to suspend revoked task " + task.id(), e);
                firstException.compareAndSet(null, new StreamsException("Failed to suspend " + task.id(), e));
            }
        }

        if (firstException.get() != null) {
            throw firstException.get();
