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

import com.proofpoint.event.collector.BatchProcessor.BatchHandler;
import com.proofpoint.event.collector.queue.Queue;
import com.proofpoint.event.collector.queue.QueueFactory;
import com.proofpoint.reporting.ReportExporter;
import com.proofpoint.testing.FileUtils;
import org.joda.time.DateTime;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestAsyncBatchProcessor
{
    private static final String DATA_DIRECTORY = "target/queue/events";

    private BatchHandler<Event> handler;
    private BatchProcessorConfig config;
    private Queue<Event> queue;
    private QueueFactory queueFactory;
    private ReportExporter mockReporter;

    @BeforeMethod
    public void setup()
            throws IOException
    {
        FileUtils.deleteRecursively(new File(DATA_DIRECTORY));

        config = new BatchProcessorConfig().setDataDirectory(DATA_DIRECTORY);
        handler = mock(BatchHandler.class);
        mockReporter = mock(ReportExporter.class);
        queueFactory = new QueueFactory(config, mockReporter);
        queue = queueFactory.create("queue");
    }

    @AfterMethod
    public void tearDown()
            throws IOException
    {
        queue.close();
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "name is null")
    public void testConstructorNullName()
            throws IOException
    {
        new AsyncBatchProcessor<>(null, handler, new BatchProcessorConfig(), queue);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "handler is null")
    public void testConstructorNullHandler()
            throws IOException
    {
        new AsyncBatchProcessor<>("name", null, new BatchProcessorConfig(), queue);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "config is null")
    public void testConstructorNullConfig()
            throws IOException
    {
        new AsyncBatchProcessor<>("name", handler, null, queue);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "queue is null")
    public void testConstructorNullQueueFactory()
            throws IOException
    {
        new AsyncBatchProcessor<>("name", handler, new BatchProcessorConfig(), null);
    }

    @Test
    public void testEnqueue()
            throws Exception
    {
        config.setMaxBatchSize(100).setQueueSize(100);
        BatchProcessor<Event> processor = new AsyncBatchProcessor<>(
                "foo", handler, config, queueFactory.create("queue"));
        processor.start();

        try {
            processor.put(event("foo"));
            processor.put(event("foo"));
            processor.put(event("foo"));
        }
        finally {
            processor.stop();
        }
    }

    @Test
    public void testFullQueue()
            throws Exception
    {
        config.setMaxBatchSize(100).setQueueSize(1);
        BlockingBatchHandler blockingHandler = blockingHandler();
        BatchProcessor<Event> processor = new AsyncBatchProcessor<>(
                "foo", blockingHandler, config, new QueueFactory(config, mockReporter).create("queue"));
        processor.start();

        blockingHandler.lock();
        try {
            // This will be processed, and its processing will block the handler
            processor.put(event("foo"));

            // Wait for the handler to pick up the item from the queue
            assertTrue(blockingHandler.waitForProcessor(10));
            assertEquals(blockingHandler.getDroppedEntries(), 0);

            // This will remain in the queue because the processing
            // thread has not yet been resumed
            processor.put(event("foo"));
            assertEquals(blockingHandler.getDroppedEntries(), 0);

            // The queue is now full, this message will be dropped.
            processor.put(event("foo"));
            assertEquals(blockingHandler.getDroppedEntries(), 1);
        }
        finally {
            blockingHandler.resumeProcessor();
            blockingHandler.unlock();
            processor.stop();
            queue.close();
        }
    }

    @Test
    public void testContinueOnHandlerException()
            throws InterruptedException, IOException
    {
        config.setMaxBatchSize(100).setQueueSize(100);
        BlockingBatchHandler blockingHandler = blockingHandlerThatThrowsException(new RuntimeException("Expected Exception"));
        BatchProcessor<Event> processor = new AsyncBatchProcessor<>(
                "foo", blockingHandler, config, new QueueFactory(config, mockReporter).create("queue"));

        processor.start();

        blockingHandler.lock();
        try {
            processor.put(event("foo"));

            assertTrue(blockingHandler.waitForProcessor(10));
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
        }
        finally {
            blockingHandler.resumeProcessor();
            blockingHandler.unlock();
            processor.stop();
        }
    }

    @Test
    public void testStopsWhenStopCalled()
            throws InterruptedException, IOException
    {
        config.setMaxBatchSize(100).setQueueSize(100);
        BlockingBatchHandler blockingHandler = blockingHandler();
        BatchProcessor<Event> processor = new AsyncBatchProcessor<>(
                "foo", blockingHandler, config, new QueueFactory(config, mockReporter).create("queue"));

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
            throws InterruptedException, IOException
    {
        config.setMaxBatchSize(100).setQueueSize(100);
        BlockingBatchHandler blockingHandler = blockingHandlerThatThrowsException(new NullPointerException("Expected Exception"));
        BatchProcessor<Event> processor = new AsyncBatchProcessor<>(
                "foo", blockingHandler, config, new QueueFactory(config, mockReporter).create("queue"));
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

        blockingHandler.lock();
        try {
            processor.put(event("foo"));
            assertTrue(blockingHandler.waitForProcessor(10));
            blockingHandler.resumeProcessor();
        }
        finally {
            blockingHandler.unlock();
            processor.stop();
        }
    }

    private static Event event(String type)
    {
        return new Event(type, UUID.randomUUID().toString(), "localhost", DateTime.now(), Collections.<String, Object>emptyMap());
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

        public BlockingBatchHandler(Runnable onProcess)
        {
            this.onProcess = onProcess;
        }

        @Override
        public void processBatch(List<Event> entries)
        {
            // Wait for the right time to run
            lock.lock();
            try {
                // Signal that we've started running
                if (entries.size() > 0) {
                    externalCondition.signal();

                    try {
                        // Block
                        internalCondition.await();
                    }
                    catch (InterruptedException ignored) {
                    }
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
    }
}
