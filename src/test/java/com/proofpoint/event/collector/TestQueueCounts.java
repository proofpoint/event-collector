/*
 * Copyright 2011-2012 Proofpoint, Inc.
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

import com.proofpoint.event.collector.BatchProcessor.BatchHandler;
import com.proofpoint.event.collector.EventCounters.CounterState;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Collections;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

public class TestQueueCounts
{
    @Test
    public void testEnqueue()
            throws Exception
    {
        BatchProcessor<Event> processor = new BatchProcessor<Event>("foo", handler(), 100, 100);
        processor.start();

        processor.put(event("foo"));
        assertEquals(processor.getCounterState().getReceived(), 1);

        processor.put(event("foo"));
        processor.put(event("foo"));
        assertCounterValues(processor.getCounterState(), 3, 0);
    }

    @Test
    public void testFullQueue()
            throws Exception
    {
        Object monitor = new Object();
        BatchHandler<Event> blockingHandler = blockingHandler(monitor);

        synchronized (monitor) {
            BatchProcessor<Event> processor = new BatchProcessor<Event>("foo", blockingHandler, 100, 1);

            processor.start();

            // This will be processed, and its processing will block the handler
            processor.put(event("foo"));

            // Wait for the handler to pick up the item from the queue
            monitor.wait();

            // This will remain in the queue and be discarded when we post the next event
            processor.put(event("foo"));

            processor.put(event("foo"));

            assertCounterValues(processor.getCounterState(), 3, 1);
        }
    }

    private void assertCounterValues(CounterState counterState, long transferred, long lost)
    {
        assertEquals(counterState.getReceived(), transferred);
        assertEquals(counterState.getLost(), lost);
    }

    private static Event event(String type)
    {
        return new Event(type, UUID.randomUUID().toString(), "localhost", DateTime.now(), Collections.<String, Object>emptyMap());
    }

    private static BatchHandler<Event> handler()
    {
        return new BatchHandler<Event>()
        {
            @Override
            public void processBatch(List<Event> entries)
            {
            }
        };
    }

    private static BatchHandler<Event> blockingHandler(final Object monitor)
    {
        return new BatchHandler<Event>()
        {
            @Override
            public void processBatch(List<Event> entries)
            {
                // Wait for the right time to run
                synchronized (monitor)
                {
                    // Signal that we've started running
                    monitor.notify();
                    try {
                        // Block
                        monitor.wait();
                    }
                    catch (InterruptedException ignored) {
                    }
                }
            }
        };
    }
}
