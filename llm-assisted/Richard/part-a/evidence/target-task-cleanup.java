                }
            }
        }

        return offsetSum;
    }

    private void closeTaskDirty(final Task task) {
        try {
            // we call this function only to flush the case if necessary
            // before suspending and closing the topology
            task.prepareCommit();
        } catch (final RuntimeException swallow) {
            log.error("Error flushing caches of dirty task {} ", task.id(), swallow);
        }

        try {
            task.suspend();
        } catch (final RuntimeException swallow) {
            log.error("Error suspending dirty task {} ", task.id(), swallow);
        }
        tasks.removeTaskBeforeClosing(task.id());
        task.closeDirty();
    }

    private void completeTaskCloseClean(final Task task) {
        tasks.removeTaskBeforeClosing(task.id());
        task.closeClean();
    }

    void shutdown(final boolean clean) {
        final AtomicReference<RuntimeException> firstException = new AtomicReference<>(null);

        final Set<Task> tasksToCloseDirty = new HashSet<>();
        // TODO: change type to `StreamTask`
        final Set<Task> activeTasks = new TreeSet<>(Comparator.comparing(Task::id));
        activeTasks.addAll(tasks.activeTasks());
        tasksToCloseDirty.addAll(tryCloseCleanAllActiveTasks(clean, firstException));
        tasksToCloseDirty.addAll(tryCloseCleanAllStandbyTasks(clean, firstException));

        for (final Task task : tasksToCloseDirty) {
            closeTaskDirty(task);
        }

        // TODO: change type to `StreamTask`
        for (final Task activeTask : activeTasks) {
            executeAndMaybeSwallow(
                clean,
                () -> tasks.closeAndRemoveTaskProducerIfNeeded(activeTask),
                e -> firstException.compareAndSet(null, e),
                e -> log.warn("Ignoring an exception while closing task " + activeTask.id() + " producer.", e)
            );
        }

        executeAndMaybeSwallow(
            clean,
