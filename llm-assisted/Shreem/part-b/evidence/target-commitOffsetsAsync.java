        }
        while (true) {
            OffsetCommitCompletion completion = completedOffsetCommits.poll();
            if (completion == null) {
                break;
            }
            completion.invoke();
        }
    }

    private boolean coordinatorUnknownAndUnready(Timer timer) {
        return coordinatorUnknown() && !ensureCoordinatorReady(timer);
    }

    public void commitOffsetsAsync(final Map<TopicPartition, OffsetAndMetadata> offsets, final OffsetCommitCallback callback) {
        invokeCompletedOffsetCommitCallbacks();

        if (offsets.isEmpty()) {
            // No need to check coordinator if offsets is empty since commit of empty offsets is completed locally.
            doCommitOffsetsAsync(offsets, callback);
        } else if (!coordinatorUnknownAndUnready(time.timer(Duration.ZERO))) {
            // we need to make sure coordinator is ready before committing, since
            // this is for async committing we do not try to block, but just try once to
            // clear the previous discover-coordinator future, resend, or get responses;
            // if the coordinator is not ready yet then we would just proceed and put that into the
            // pending requests, and future poll calls would still try to complete them.
            //
            // the key here though is that we have to try sending the discover-coordinator if
            // it's not known or ready, since this is the only place we can send such request
            // under manual assignment (there we would not have heartbeat thread trying to auto-rediscover
            // the coordinator).
            doCommitOffsetsAsync(offsets, callback);
        } else {
            // we don't know the current coordinator, so try to find it and then send the commit
            // or fail (we don't want recursive retries which can cause offset commits to arrive
            // out of order). Note that there may be multiple offset commits chained to the same
            // coordinator lookup request. This is fine because the listeners will be invoked in
            // the same order that they were added. Note also that AbstractCoordinator prevents
            // multiple concurrent coordinator lookup requests.
            pendingAsyncCommits.incrementAndGet();
            lookupCoordinator().addListener(new RequestFutureListener<Void>() {
                @Override
                public void onSuccess(Void value) {
                    pendingAsyncCommits.decrementAndGet();
                    doCommitOffsetsAsync(offsets, callback);
                    client.pollNoWakeup();
                }

                @Override
                public void onFailure(RuntimeException e) {
                    pendingAsyncCommits.decrementAndGet();
                    completedOffsetCommits.add(new OffsetCommitCompletion(callback, offsets,
                            new RetriableCommitFailedException(e)));
                }
            });
        }
