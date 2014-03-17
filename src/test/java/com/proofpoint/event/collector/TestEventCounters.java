/*
 * Copyright 2011-2014 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.proofpoint.event.collector;

import com.proofpoint.event.collector.EventCounters.Counter;
import com.proofpoint.event.collector.EventCounters.CounterState;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestEventCounters
{
    private Counter counter;
    private EventCounters<String> counters;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        counter = new Counter();
        counters = new EventCounters<>();
    }

    @Test
    public void testCounterInitiallyZero()
            throws Exception
    {
        assertCounterValues(counter.getState(), 0, 0);
    }

    @Test
    public void testTransferredCounter()
            throws Exception
    {
        counter.recordReceived(1);
        assertCounterValues(counter.getState(), 1, 0);

        counter.recordReceived(10);
        assertCounterValues(counter.getState(), 11, 0);
    }

    @Test
    public void testLostCounter()
            throws Exception
    {
        counter.recordLost(1);
        assertCounterValues(counter.getState(), 0, 1);

        counter.recordLost(10);
        assertCounterValues(counter.getState(), 0, 11);
    }

    private void assertCounterValues(CounterState counterState, long transferred, long lost)
    {
        assertEquals(counterState.getReceived(), transferred);
        assertEquals(counterState.getLost(), lost);
    }

    @Test
    public void testTransferredCount()
            throws Exception
    {
        counters.recordReceived("foo", 10);
        assertCounterValues(counters.getCounts().get("foo"), 10, 0);

        counters.recordReceived("foo", 0);
        assertCounterValues(counters.getCounts().get("foo"), 10, 0);

        counters.recordReceived("foo", 50);
        assertCounterValues(counters.getCounts().get("foo"), 60, 0);

        counters.recordReceived("bar", 1);
        assertCounterValues(counters.getCounts().get("foo"), 60, 0);
        assertCounterValues(counters.getCounts().get("bar"), 1, 0);
    }

    @Test
    public void testLostCount()
            throws Exception
    {
        counters.recordLost("foo", 10);
        assertCounterValues(counters.getCounts().get("foo"), 0, 10);

        counters.recordLost("foo", 0);
        assertCounterValues(counters.getCounts().get("foo"), 0, 10);

        counters.recordLost("foo", 50);
        assertCounterValues(counters.getCounts().get("foo"), 0, 60);

        counters.recordLost("bar", 1);
        assertCounterValues(counters.getCounts().get("foo"), 0, 60);
        assertCounterValues(counters.getCounts().get("bar"), 0, 1);
    }

    @Test
    public void testClearCounters()
            throws Exception
    {
        counters.recordReceived("foo", 1);
        counters.recordReceived("bar", 1);
        counters.recordLost("foo", 1);
        counters.recordLost("bar", 1);

        assertCounterValues(counters.getCounts().get("foo"), 1, 1);
        assertCounterValues(counters.getCounts().get("bar"), 1, 1);

        counters.resetCounts();
        assertEquals(counters.getCounts().size(), 0);
    }
}
