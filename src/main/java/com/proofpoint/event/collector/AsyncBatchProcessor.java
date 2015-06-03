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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proofpoint.event.collector.queue.Queue;
import com.proofpoint.event.collector.queue.QueueFactory;
import com.proofpoint.log.Logger;
import com.proofpoint.units.Duration;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.weakref.jmx.internal.guava.base.Preconditions.checkState;

public class AsyncBatchProcessor<T> implements BatchProcessor<T>
{
    private static final Logger log = Logger.get(AsyncBatchProcessor.class);
    private final BatchHandler<T> handler;
    private final int maxBatchSize;
    private final QueueFactory<T> queueFactory;
    private final Queue<T> queue;
    private final ScheduledExecutorService executor;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final Duration throttleTime;

    public AsyncBatchProcessor(String name, BatchHandler<T> handler, BatchProcessorConfig config, QueueFactory<T> queueFactory)
            throws IOException
    {
        checkNotNull(name, "name is null");
        checkNotNull(handler, "handler is null");
        checkNotNull(queueFactory, "queueFactory is null");

        this.handler = handler;
        this.queueFactory = queueFactory;
        this.maxBatchSize = checkNotNull(config, "config is null").getMaxBatchSize();
        this.executor = newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat(format("batch-processor-%s", name)).build());
        throttleTime = checkNotNull(config.getThrottleTime(), "throttle time is null");

        queue = queueFactory.create(name);
    }

    @Override
    public void start()
    {
        executor.scheduleAtFixedRate(new EventPoster(), (long) throttleTime.getValue(), (long) throttleTime.getValue(), throttleTime.getUnit());
        running.set(true);
    }

    @Override
    public void stop()
    {
        executor.shutdownNow();
        running.set(false);
        try {
            queue.close();
        }
        catch (IOException e) {
            log.error(e, "Could not close queue");
        }
    }

    @Override
    public void terminateQueue()
            throws IOException
    {
        queueFactory.terminate(queue.getName());
    }

    @Override
    public void put(T entry)
    {
        checkState(running.get(), "Processor is not running");
        checkNotNull(entry, "entry is null");
        try {
            if (!queue.offer(entry)) {
                // queue is full: drop current message
                handler.notifyEntriesDropped(1);
            }
        }
        catch (IOException e) {
            handler.notifyEntriesDropped(1);
            log.error(e, "failed to add entry");
        }
    }

    private class EventPoster implements Runnable
    {
        @Override
        public void run()
        {
            try {
                // re-enqueue entries in case of failure
                List<T> entries = queue.dequeue(maxBatchSize);
                if (entries.isEmpty()) {
                    return;
                }

                if (!handler.processBatch(entries)) {
                    queue.enqueueAllOrDrop(entries);
                    log.debug("Failed to send %s events. Re-queued events for later delivery.", entries.size());
                }
                else {
                    log.debug("Successfully sent %s events", entries.size());
                }
            }
            catch (Exception e) {
                log.error(e, "error occurred during batch processing");
            }
        }
    }
}
