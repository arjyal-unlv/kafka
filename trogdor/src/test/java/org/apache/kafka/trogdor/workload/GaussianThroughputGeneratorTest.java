/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.trogdor.workload;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * Tests for {@link GaussianThroughputGenerator}.
 *
 * <p>This class reads the clock through {@code Time.SYSTEM} rather than an injected
 * {@code Time}, so the clock cannot be mocked the way {@code TimeIntervalTransactionsGeneratorTest}
 * mocks it. The tests below therefore use real elapsed time with short windows and
 * deliberately loose bounds, so that they assert pacing behaviour without becoming
 * sensitive to scheduler jitter.
 *
 * <p>Passing a {@code messagesPerWindowDeviation} of {@code 0.0} removes the Gaussian
 * term from the throttle calculation, which makes {@code throttleMessages} exactly equal
 * to {@code messagesPerWindowAverage}. That is what makes deterministic assertions
 * possible against an otherwise randomised class.
 */
public class GaussianThroughputGeneratorTest {

    @Test
    public void testConstructorStoresConfiguredValues() {
        GaussianThroughputGenerator generator = new GaussianThroughputGenerator(50, 2.5, 4, 250);

        assertEquals(50, generator.messagesPerWindowAverage());
        assertEquals(2.5, generator.messagesPerWindowDeviation());
        assertEquals(4, generator.windowsUntilRateChange());
        assertEquals(250, generator.windowSizeMs());
    }

    @Test
    public void testNonPositiveWindowSizeFallsBackToDefault() {
        assertEquals(100, new GaussianThroughputGenerator(10, 0.0, 1, 0).windowSizeMs());
        assertEquals(100, new GaussianThroughputGenerator(10, 0.0, 1, -250).windowSizeMs());
    }

    @Test
    public void testThrottleMessagesIsAtLeastOne() throws Exception {
        // A negative average would compute a non-positive allowance; the implementation
        // clamps it to one, so a single throttle() call must still return promptly.
        GaussianThroughputGenerator generator = new GaussianThroughputGenerator(-5, 0.0, 1, 50);
        generator.throttle();
    }

    @Test
    @Timeout(30)
    public void testThrottleDoesNotBlockBelowWindowAllowance() throws Exception {
        // Allowance is 5 per window; four calls stay under it and must not wait.
        GaussianThroughputGenerator generator = new GaussianThroughputGenerator(5, 0.0, 1, 2000);

        long start = Time.SYSTEM.milliseconds();
        for (int i = 0; i < 4; i++) {
            generator.throttle();
        }
        long elapsed = Time.SYSTEM.milliseconds() - start;

        assertTrue(elapsed < 1000,
            "throttle() blocked before the window allowance was reached, elapsed=" + elapsed + "ms");
    }

    @Test
    @Timeout(30)
    public void testThrottleBlocksOnceWindowAllowanceIsReached() throws Exception {
        long windowSizeMs = 400;
        int messagesPerWindow = 3;
        GaussianThroughputGenerator generator =
            new GaussianThroughputGenerator(messagesPerWindow, 0.0, 1, windowSizeMs);

        long start = Time.SYSTEM.milliseconds();
        for (int i = 0; i < messagesPerWindow; i++) {
            generator.throttle();
        }
        long elapsed = Time.SYSTEM.milliseconds() - start;

        // The final call exhausts the allowance and must wait for the window boundary.
        assertTrue(elapsed >= windowSizeMs / 2,
            "throttle() did not wait for the window boundary, elapsed=" + elapsed + "ms");
    }

    @Test
    @Timeout(60)
    public void testThrottlePacesAcrossConsecutiveWindows() throws Exception {
        long windowSizeMs = 200;
        int messagesPerWindow = 2;
        int windows = 3;
        GaussianThroughputGenerator generator =
            new GaussianThroughputGenerator(messagesPerWindow, 0.0, 1, windowSizeMs);

        long start = Time.SYSTEM.milliseconds();
        for (int i = 0; i < messagesPerWindow * windows; i++) {
            generator.throttle();
        }
        long elapsed = Time.SYSTEM.milliseconds() - start;

        // Spanning three windows requires the window start to advance each time it is
        // reached. If the boundary comparison failed to move nextWindowStarts forward,
        // the later windows would not throttle and this would finish far too quickly.
        assertTrue(elapsed >= windowSizeMs * (windows - 1),
            "throttle() stopped pacing across windows, elapsed=" + elapsed + "ms");
    }

    @Test
    @Timeout(30)
    public void testWindowIsRecalculatedAfterAnIdlePeriod() throws Exception {
        // windowsUntilRateChange is deliberately larger than the number of windows this
        // test advances through, so the allowance is not recomputed and the rate-change
        // branch is exercised in its negative form.
        long windowSizeMs = 100;
        GaussianThroughputGenerator generator = new GaussianThroughputGenerator(5, 0.0, 5, windowSizeMs);

        // Stay idle long enough for several window boundaries to pass. The next call to
        // throttle() therefore starts outside the current window and must realign it
        // before counting the message.
        Thread.sleep(windowSizeMs * 3);

        long start = Time.SYSTEM.milliseconds();
        generator.throttle();
        long elapsed = Time.SYSTEM.milliseconds() - start;

        assertTrue(elapsed < windowSizeMs * 2,
            "throttle() should realign the window rather than wait out a stale one, elapsed="
                + elapsed + "ms");
    }

    @Test
    @Timeout(60)
    public void testRateIsRecalculatedAfterConfiguredNumberOfWindows() throws Exception {
        // windowsUntilRateChange of 1 means the allowance is recomputed on every window,
        // which exercises the rate-change branch repeatedly rather than only on construction.
        long windowSizeMs = 150;
        GaussianThroughputGenerator generator =
            new GaussianThroughputGenerator(2, 0.0, 1, windowSizeMs);

        long start = Time.SYSTEM.milliseconds();
        for (int i = 0; i < 6; i++) {
            generator.throttle();
        }
        long elapsed = Time.SYSTEM.milliseconds() - start;

        assertTrue(elapsed >= windowSizeMs,
            "expected at least one window of throttling, elapsed=" + elapsed + "ms");
    }
}
