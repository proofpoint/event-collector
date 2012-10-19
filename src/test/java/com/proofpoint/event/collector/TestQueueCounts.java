/*
 * Copyright 2011 Proofpoint, Inc.
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

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

public class TestQueueCounts
{
    @Test
    public void testEnqueue()
            throws Exception
    {
        BatchProcessor<Event> processor = new BatchProcessor<Event>("test", handler(), 100, 100);
        processor.start();

        processor.put(event("foo"));
        assertEquals(processor.getCounters().get("foo").getReceived(), 1);

        processor.put(event("bar"));
        processor.put(event("bar"));
        assertCounterValues(processor.getCounters().get("foo"), 1, 0);
        assertCounterValues(processor.getCounters().get("bar"), 2, 0);
    }

    @Test
    public void testFullQueue()
            throws Exception
    {
        BatchHandler<Event> blockingHandler = blockingHandler();
        BatchProcessor<Event> processor = new BatchProcessor<Event>("test", blockingHandler, 100, 1);

        processor.start();

        // This will be processed, and its processing will block the handler
        processor.put(event("foo"));

        // This will remain in the queue and be discarded when we post the next event
        processor.put(event("foo"));

        processor.put(event("bar"));

        assertCounterValues(processor.getCounters().get("foo"), 2, 1);
        assertCounterValues(processor.getCounters().get("bar"), 1, 0);
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
            public void processBatch(Collection<Event> entries)
            {
            }
        };
    }

    private static BatchHandler<Event> blockingHandler()
    {
        return new BatchHandler<Event>()
        {
            @Override
            public void processBatch(Collection<Event> entries)
            {
                try {
                    wait();
                }
                catch (InterruptedException ignored) {
                }
            }
        };
    }
}
