     * Closes active tasks as zombies, as these partitions have been lost and are no longer owned.
     * NOTE this method assumes that when it is called, EVERY task/partition has been lost and must
     * be closed as a zombie.
     *
     * @throws TaskMigratedException if the task producer got fenced (EOS only)
     */
    void handleLostAll() {
        log.debug("Closing lost active tasks as zombies.");

        final Set<Task> allTask = new HashSet<>(tasks.allTasks());
        for (final Task task : allTask) {
            // Even though we've apparently dropped out of the group, we can continue safely to maintain our
            // standby tasks while we rejoin.
            if (task.isActive()) {
                closeTaskDirty(task);

                tasks.cleanUpTaskProducerAndRemoveTask(task.id(), new HashMap<>());
            }
        }

        if (processingMode == EXACTLY_ONCE_V2) {
            tasks.reInitializeThreadProducer();
        }
    }

    /**
