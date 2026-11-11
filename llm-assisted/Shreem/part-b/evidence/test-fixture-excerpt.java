// ---- fields (lines 150-172) ----
    private final Map<String, MockPartitionAssignor> assignorMap;
    private final String consumerId = "consumer";
    private final String consumerId2 = "consumer2";

    private MockClient client;
    private MetadataResponse metadataResponse = RequestTestUtils.metadataUpdateWith(1, new HashMap<String, Integer>() {
        {
            put(topic1, 1);
            put(topic2, 1);
        }
    });
    private Node node = metadataResponse.brokers().iterator().next();
    private SubscriptionState subscriptions;
    private ConsumerMetadata metadata;
    private Metrics metrics;
    private ConsumerNetworkClient consumerClient;
    private MockRebalanceListener rebalanceListener;
    private MockCommitCallback mockOffsetCommitCallback;
    private ConsumerCoordinator coordinator;

    public ConsumerCoordinatorTest(final ConsumerPartitionAssignor.RebalanceProtocol protocol) {
        this.protocol = protocol;


// ---- @BeforeEach setUp (lines 186-215) ----
    @BeforeEach
    public void setup() {
        LogContext logContext = new LogContext();
        this.subscriptions = new SubscriptionState(logContext, OffsetResetStrategy.EARLIEST);
        this.metrics = new Metrics(time);
        this.metadata = new ConsumerMetadata(0, Long.MAX_VALUE, false,
            false, subscriptions, logContext, new ClusterResourceListeners(), metrics);
        this.client = new MockClient(time, metadata);
        this.client.updateMetadata(metadataResponse);
        this.consumerClient = new ConsumerNetworkClient(logContext, client, metadata, time, 100,
                requestTimeoutMs, Integer.MAX_VALUE);
        this.rebalanceListener = new MockRebalanceListener();
        this.mockOffsetCommitCallback = new MockCommitCallback();
        this.partitionAssignor.clear();
        this.rebalanceConfig = buildRebalanceConfig(Optional.empty());
        this.coordinator = buildCoordinator(rebalanceConfig,
                                            metrics,
                                            assignors,
                                            false,
                                            subscriptions);
    }

    private GroupRebalanceConfig buildRebalanceConfig(Optional<String> groupInstanceId) {
        return new GroupRebalanceConfig(sessionTimeoutMs,
                                        rebalanceTimeoutMs,
                                        heartbeatIntervalMs,
                                        groupId,
                                        groupInstanceId,
                                        retryBackoffMs,
                                        !groupInstanceId.isPresent());

// ---- an existing async-commit test, for style (lines 2100-2135) ----

    @Test
    public void testCommitOffsetAsyncCoordinatorNotAvailable() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // async commit with coordinator not available
        MockCommitCallback cb = new MockCommitCallback();
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.COORDINATOR_NOT_AVAILABLE);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), cb);
        coordinator.invokeCompletedOffsetCommitCallbacks();

        assertTrue(coordinator.coordinatorUnknown());
        assertEquals(1, cb.invoked);
        assertTrue(cb.exception instanceof RetriableCommitFailedException);
    }

    @Test
    public void testCommitOffsetAsyncNotCoordinator() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
        coordinator.ensureCoordinatorReady(time.timer(Long.MAX_VALUE));

        // async commit with not coordinator
        MockCommitCallback cb = new MockCommitCallback();
        prepareOffsetCommitRequest(singletonMap(t1p, 100L), Errors.COORDINATOR_NOT_AVAILABLE);
        coordinator.commitOffsetsAsync(singletonMap(t1p, new OffsetAndMetadata(100L)), cb);
        coordinator.invokeCompletedOffsetCommitCallbacks();

        assertTrue(coordinator.coordinatorUnknown());
        assertEquals(1, cb.invoked);
        assertTrue(cb.exception instanceof RetriableCommitFailedException);
    }

    @Test
    public void testCommitOffsetAsyncDisconnected() {
        client.prepareResponse(groupCoordinatorResponse(node, Errors.NONE));
