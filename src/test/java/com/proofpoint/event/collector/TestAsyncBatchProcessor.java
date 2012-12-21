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
import com.proofpoint.event.collector.BatchProcessor.Observer;
import org.joda.time.DateTime;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;

public class TestAsyncBatchProcessor
{
    private Observer observer;

    @BeforeMethod
    public void setup()
    {
        observer = mock(Observer.class);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "name is null")
    public void testConstructorNullName()
    {
        new AsyncBatchProcessor(null, handler(), new BatchProcessorConfig(), observer);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "handler is null")
    public void testConstructorNullHandler()
    {
        new AsyncBatchProcessor("name", null, new BatchProcessorConfig(), observer);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "observer is null")
    public void testConstructorNullObserver()
    {
        new AsyncBatchProcessor("name", handler(), new BatchProcessorConfig(), null);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "config is null")
    public void testConstructorNullConfig()
    {
        new AsyncBatchProcessor("name", handler(), null, observer);
    }

    @Test
    public void testEnqueue()
            throws Exception
    {
        BatchProcessor<Event> processor = new AsyncBatchProcessor<Event>(
                "foo", handler(), new BatchProcessorConfig().setMaxBatchSize(100).setQueueSize(100), observer
        );
        processor.start();

        processor.put(event("foo"));

        assertCounterValues(1, 0);

        processor.put(event("foo"));
        processor.put(event("foo"));
        assertCounterValues(3, 0);
    }

    @Test
    public void testFullQueue()
            throws Exception
    {
        Object monitor = new Object();
        BatchHandler<Event> blockingHandler = blockingHandler(monitor);

        synchronized (monitor) {
            BatchProcessor<Event> processor = new AsyncBatchProcessor<Event>(
                    "foo", blockingHandler, new BatchProcessorConfig().setMaxBatchSize(100).setQueueSize(1), observer
            );

            processor.start();

            // This will be processed, and its processing will block the handler
            processor.put(event("foo"));

            // Wait for the handler to pick up the item from the queue
            monitor.wait();

            // This will remain in the queue and be discarded when we post the next event
            processor.put(event("foo"));

            processor.put(event("foo"));

            assertCounterValues(3, 1);
        }
    }

    private void assertCounterValues(long transferred, long lost)
    {
        ArgumentCaptor<Integer> receivedCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(observer, atLeast(0)).onRecordsReceived(receivedCaptor.capture());
        assertEquals(totalCount(receivedCaptor.getAllValues()), transferred);

        ArgumentCaptor<Integer> lostCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(observer, atLeast(0)).onRecordsLost(lostCaptor.capture());
        assertEquals(totalCount(lostCaptor.getAllValues()), lost);
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
                synchronized (monitor) {
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

    private int totalCount(Collection<Integer> integers)
    {
        int result = 0;
        for (int i : integers) {
            result += i;
        }
        return result;
    }
}
