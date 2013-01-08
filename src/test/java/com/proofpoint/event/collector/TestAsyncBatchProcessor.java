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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestAsyncBatchProcessor
{
    private Observer observer;
    private BatchHandler<Event> handler;

    @BeforeMethod
    public void setup()
    {
        observer = mock(Observer.class);
        handler = mock(BatchHandler.class);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "name is null")
    public void testConstructorNullName()
    {
        new AsyncBatchProcessor(null, handler, new BatchProcessorConfig(), observer);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "handler is null")
    public void testConstructorNullHandler()
    {
        new AsyncBatchProcessor("name", null, new BatchProcessorConfig(), observer);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "observer is null")
    public void testConstructorNullObserver()
    {
        new AsyncBatchProcessor("name", handler, new BatchProcessorConfig(), null);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "config is null")
    public void testConstructorNullConfig()
    {
        new AsyncBatchProcessor("name", handler, null, observer);
    }

    @Test
    public void testEnqueue()
            throws Exception
    {
        BatchProcessor<Event> processor = new AsyncBatchProcessor<Event>(
                "foo", handler, new BatchProcessorConfig().setMaxBatchSize(100).setQueueSize(100), observer
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
        BlockingBatchHandler blockingHandler = blockingHandler();
        BatchProcessor<Event> processor = new AsyncBatchProcessor<Event>(
                "foo", blockingHandler, new BatchProcessorConfig().setMaxBatchSize(100).setQueueSize(1), observer
        );

        processor.start();

        blockingHandler.lock();
        try {
            // This will be processed, and its processing will block the handler
            processor.put(event("foo"));
            assertEquals(blockingHandler.getDroppedEntries(), 0);

            // Wait for the handler to pick up the item from the queue
            assertTrue(blockingHandler.waitForProcessor(10));

            // This will remain in the queue because the processing
            // thread has not yet been resumed
            processor.put(event("foo"));
            assertCounterValues(2, 0);
            assertEquals(blockingHandler.getDroppedEntries(), 0);

            // The queue is now full, this message will be dropped.
            processor.put(event("foo"));
            assertCounterValues(3, 1);
            assertEquals(blockingHandler.getDroppedEntries(), 1);
        }
        finally {
            blockingHandler.resumeProcessor();
            blockingHandler.unlock();
        }
    }

    @Test
    public void testContinueOnHandlerException()
            throws InterruptedException
    {
        BlockingBatchHandler blockingHandler = blockingHandlerThatThrowsException(new RuntimeException());
        BatchProcessor<Event> processor = new AsyncBatchProcessor<Event>(
                "foo", blockingHandler, new BatchProcessorConfig().setMaxBatchSize(100).setQueueSize(100), observer
        );

        processor.start();

        blockingHandler.lock();
        try {
            processor.put(event("foo"));

            assertTrue(blockingHandler.waitForProcessor(10));
            assertEquals(blockingHandler.getCallsToProcessBatch(), 1);
        }
        finally {
            blockingHandler.resumeProcessor();
            blockingHandler.unlock();
        }

        // When the processor unblocks, an exception (in its thread) will be thrown.
        // This should not affect the processor.
        blockingHandler.lock();
        try {
            processor.put(event("bar"));
            assertTrue(blockingHandler.waitForProcessor(10));
            assertEquals(blockingHandler.getCallsToProcessBatch(), 2);
        }
        finally {
            blockingHandler.resumeProcessor();
            blockingHandler.unlock();
        }
    }

    @Test
    public void testStopsWhenStopCalled()
            throws InterruptedException
    {
        BlockingBatchHandler blockingHandler = blockingHandler();
        BatchProcessor<Event> processor = new AsyncBatchProcessor<Event>(
                "foo", blockingHandler, new BatchProcessorConfig().setMaxBatchSize(100).setQueueSize(100), observer
        );

        processor.start();

        blockingHandler.lock();
        try {
            processor.put(event("foo"));

            assertTrue(blockingHandler.waitForProcessor(10));

            // The processor hasn't been resumed. Stop it!
            processor.stop();
        }
        finally {
            blockingHandler.unlock();
        }

        try {
            processor.put(event("bar"));
            fail();
        }
        catch (IllegalStateException ex) {
            assertEquals(ex.getMessage(), "Processor is not running");
        }
    }

    @Test
    public void testIgnoresExceptionsInHandler()
            throws InterruptedException
    {
        BlockingBatchHandler blockingHandler = blockingHandlerThatThrowsException(new NullPointerException());
        BatchProcessor<Event> processor = new AsyncBatchProcessor<Event>(
                "foo", blockingHandler, new BatchProcessorConfig().setMaxBatchSize(100).setQueueSize(100), observer
        );
        processor.start();

        blockingHandler.lock();
        try {
            processor.put(event("foo"));
            assertTrue(blockingHandler.waitForProcessor(10));
            blockingHandler.resumeProcessor();
        }
        finally {
            blockingHandler.unlock();
        }
        assertEquals(blockingHandler.getCallsToProcessBatch(), 1);

        blockingHandler.lock();
        try {
            processor.put(event("foo"));
            assertTrue(blockingHandler.waitForProcessor(10));
            blockingHandler.resumeProcessor();
        }
        finally {
            blockingHandler.unlock();
        }
        assertEquals(blockingHandler.getCallsToProcessBatch(), 2);
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

    private int totalCount(Collection<Integer> integers)
    {
        int result = 0;
        for (int i : integers) {
            result += i;
        }
        return result;
    }

    private static BlockingBatchHandler blockingHandler()
    {
        return new BlockingBatchHandler(new Runnable()
        {
            @Override
            public void run()
            {
            }
        });
    }

    private static BlockingBatchHandler blockingHandlerThatThrowsException(final RuntimeException exception)
    {
        return new BlockingBatchHandler(new Runnable()
        {
            @Override
            public void run()
            {
                throw exception;
            }
        });
    }

    private static class BlockingBatchHandler implements BatchHandler<Event>
    {
        private final Lock lock = new ReentrantLock();
        private final Condition externalCondition = lock.newCondition();
        private final Condition internalCondition = lock.newCondition();
        private final Runnable onProcess;
        private long droppedEntries = 0;
        private long callsToProcessBatch = 0;

        public BlockingBatchHandler(Runnable onProcess)
        {
            this.onProcess = onProcess;
        }

        @Override
        public void processBatch(List<Event> entries)
        {
            // Wait for the right time to run
            lock.lock();
            callsToProcessBatch += 1;
            try {
                // Signal that we've started running
                externalCondition.signal();
                try {
                    // Block
                    internalCondition.await();
                }
                catch (InterruptedException ignored) {
                }
                onProcess.run();
            }
            finally {
                lock.unlock();
            }
        }

        @Override
        public void notifyEntriesDropped(int count)
        {
            droppedEntries += count;
        }

        public void lock()
        {
            lock.lock();
        }

        public void unlock()
        {
            lock.unlock();
        }

        public boolean waitForProcessor(long seconds)
                throws InterruptedException
        {
            return externalCondition.await(seconds, TimeUnit.SECONDS);
        }

        public void resumeProcessor()
        {
            internalCondition.signal();
        }

        public long getDroppedEntries()
        {
            return droppedEntries;
        }

        public long getCallsToProcessBatch()
        {
            return callsToProcessBatch;
        }
    }
}
