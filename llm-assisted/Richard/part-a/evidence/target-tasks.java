class Tasks {
    private final Logger log;
    private final InternalTopologyBuilder builder;
    private final StreamsMetricsImpl streamsMetrics;

    private final Map<TaskId, Task> allTasksPerId = new TreeMap<>();
    private final Map<TaskId, Task> readOnlyTasksPerId = Collections.unmodifiableMap(allTasksPerId);
    private final Collection<Task> readOnlyTasks = Collections.unmodifiableCollection(allTasksPerId.values());

    // TODO: change type to `StreamTask`
    private final Map<TaskId, Task> activeTasksPerId = new TreeMap<>();
    // TODO: change type to `StreamTask`
    private final Map<TopicPartition, Task> activeTasksPerPartition = new HashMap<>();
    // TODO: change type to `StreamTask`
    private final Map<TaskId, Task> readOnlyActiveTasksPerId = Collections.unmodifiableMap(activeTasksPerId);
    private final Set<TaskId> readOnlyActiveTaskIds = Collections.unmodifiableSet(activeTasksPerId.keySet());
    // TODO: change type to `StreamTask`
    private final Collection<Task> readOnlyActiveTasks = Collections.unmodifiableCollection(activeTasksPerId.values());

    // TODO: change type to `StandbyTask`
    private final Map<TaskId, Task> standbyTasksPerId = new TreeMap<>();
    // TODO: change type to `StandbyTask`
    private final Map<TaskId, Task> readOnlyStandbyTasksPerId = Collections.unmodifiableMap(standbyTasksPerId);
    private final Set<TaskId> readOnlyStandbyTaskIds = Collections.unmodifiableSet(standbyTasksPerId.keySet());

    private final ActiveTaskCreator activeTaskCreator;
    private final StandbyTaskCreator standbyTaskCreator;

    private Consumer<byte[], byte[]> mainConsumer;

    Tasks(final String logPrefix,
          final InternalTopologyBuilder builder,
          final StreamsMetricsImpl streamsMetrics,
          final ActiveTaskCreator activeTaskCreator,
          final StandbyTaskCreator standbyTaskCreator) {

        final LogContext logContext = new LogContext(logPrefix);
        log = logContext.logger(getClass());

        this.builder = builder;
        this.streamsMetrics = streamsMetrics;
        this.activeTaskCreator = activeTaskCreator;
        this.standbyTaskCreator = standbyTaskCreator;
    }

    void setMainConsumer(final Consumer<byte[], byte[]> mainConsumer) {
        this.mainConsumer = mainConsumer;
    }

    void createTasks(final Map<TaskId, Set<TopicPartition>> activeTasksToCreate,
                     final Map<TaskId, Set<TopicPartition>> standbyTasksToCreate) {
        for (final Map.Entry<TaskId, Set<TopicPartition>> taskToBeCreated : activeTasksToCreate.entrySet()) {
            final TaskId taskId = taskToBeCreated.getKey();

            if (activeTasksPerId.containsKey(taskId)) {
                throw new IllegalStateException("Attempted to create an active task that we already own: " + taskId);
            }
        }

        for (final Map.Entry<TaskId, Set<TopicPartition>> taskToBeCreated : standbyTasksToCreate.entrySet()) {
            final TaskId taskId = taskToBeCreated.getKey();

            if (standbyTasksPerId.containsKey(taskId)) {
                throw new IllegalStateException("Attempted to create a standby task that we already own: " + taskId);
            }
        }

        // keep this check to simplify testing (ie, no need to mock `activeTaskCreator`)
        if (!activeTasksToCreate.isEmpty()) {
            // TODO: change type to `StreamTask`
            for (final Task activeTask : activeTaskCreator.createTasks(mainConsumer, activeTasksToCreate)) {
                activeTasksPerId.put(activeTask.id(), activeTask);
                allTasksPerId.put(activeTask.id(), activeTask);
                for (final TopicPartition topicPartition : activeTask.inputPartitions()) {
                    activeTasksPerPartition.put(topicPartition, activeTask);
                }
            }
        }

        // keep this check to simplify testing (ie, no need to mock `standbyTaskCreator`)
        if (!standbyTasksToCreate.isEmpty()) {
            // TODO: change type to `StandbyTask`
            for (final Task standbyTask : standbyTaskCreator.createTasks(standbyTasksToCreate)) {
                standbyTasksPerId.put(standbyTask.id(), standbyTask);
                allTasksPerId.put(standbyTask.id(), standbyTask);
            }
        }
    }

    void convertActiveToStandby(final StreamTask activeTask,
                                final Set<TopicPartition> partitions,
                                final Map<TaskId, RuntimeException> taskCloseExceptions) {
        if (activeTasksPerId.remove(activeTask.id()) == null) {
            throw new IllegalStateException("Attempted to convert unknown active task to standby task: " + activeTask.id());
        }
        final Set<TopicPartition> toBeRemoved = activeTasksPerPartition.entrySet().stream()
            .filter(e -> e.getValue().id().equals(activeTask.id()))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        toBeRemoved.forEach(activeTasksPerPartition::remove);

        cleanUpTaskProducerAndRemoveTask(activeTask.id(), taskCloseExceptions);

        final StandbyTask standbyTask = standbyTaskCreator.createStandbyTaskFromActive(activeTask, partitions);
        standbyTasksPerId.put(standbyTask.id(), standbyTask);
        allTasksPerId.put(standbyTask.id(), standbyTask);
    }

    void convertStandbyToActive(final StandbyTask standbyTask, final Set<TopicPartition> partitions) {
        if (standbyTasksPerId.remove(standbyTask.id()) == null) {
            throw new IllegalStateException("Attempted to convert unknown standby task to stream task: " + standbyTask.id());
        }

        final StreamTask activeTask = activeTaskCreator.createActiveTaskFromStandby(standbyTask, partitions, mainConsumer);
        activeTasksPerId.put(activeTask.id(), activeTask);
        for (final TopicPartition topicPartition : activeTask.inputPartitions()) {
            activeTasksPerPartition.put(topicPartition, activeTask);
        }
        allTasksPerId.put(activeTask.id(), activeTask);
    }

    void updateInputPartitionsAndResume(final Task task, final Set<TopicPartition> topicPartitions) {
        final boolean requiresUpdate = !task.inputPartitions().equals(topicPartitions);
        if (requiresUpdate) {
            log.debug("Update task {} inputPartitions: current {}, new {}", task, task.inputPartitions(), topicPartitions);
            for (final TopicPartition inputPartition : task.inputPartitions()) {
                activeTasksPerPartition.remove(inputPartition);
            }
            if (task.isActive()) {
                for (final TopicPartition topicPartition : topicPartitions) {
                    activeTasksPerPartition.put(topicPartition, task);
                }
            }
            task.updateInputPartitions(topicPartitions, builder.nodeToSourceTopics());
        }
        task.resume();
    }

    void cleanUpTaskProducerAndRemoveTask(final TaskId taskId,
                                          final Map<TaskId, RuntimeException> taskCloseExceptions) {
        try {
            activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(taskId);
        } catch (final RuntimeException e) {
            final String uncleanMessage = String.format("Failed to close task %s cleanly. Attempting to close remaining tasks before re-throwing:", taskId);
            log.error(uncleanMessage, e);
            taskCloseExceptions.putIfAbsent(taskId, e);
        }
        removeTaskBeforeClosing(taskId);
    }

    void reInitializeThreadProducer() {
        activeTaskCreator.reInitializeThreadProducer();
    }

    void closeThreadProducerIfNeeded() {
        activeTaskCreator.closeThreadProducerIfNeeded();
    }

    // TODO: change type to `StreamTask`
    void closeAndRemoveTaskProducerIfNeeded(final Task activeTask) {
        activeTaskCreator.closeAndRemoveTaskProducerIfNeeded(activeTask.id());
    }

    void removeTaskBeforeClosing(final TaskId taskId) {
        activeTasksPerId.remove(taskId);
        final Set<TopicPartition> toBeRemoved = activeTasksPerPartition.entrySet().stream()
            .filter(e -> e.getValue().id().equals(taskId))
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        toBeRemoved.forEach(activeTasksPerPartition::remove);
        standbyTasksPerId.remove(taskId);
        allTasksPerId.remove(taskId);
    }
