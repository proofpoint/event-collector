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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proofpoint.event.collector.EventCounters.Counter;
import com.proofpoint.event.collector.EventCounters.CounterState;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.String.format;

public class BatchProcessor<T extends Event>
{
    private final BatchHandler<T> handler;
    private final int maxBatchSize;
    private final BlockingQueue<T> queue;
    private final ExecutorService executor;
    private final AtomicReference<Future<?>> future = new AtomicReference<Future<?>>();

    private final AtomicReference<Counter> counter = new AtomicReference<Counter>(new Counter());

    public BatchProcessor(String name, BatchHandler<T> handler, int maxBatchSize, int queueSize)
    {
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkNotNull(handler, "handler is null");
        Preconditions.checkArgument(queueSize > 0, "queue size needs to be a positive integer");
        Preconditions.checkArgument(maxBatchSize > 0, "max batch size needs to be a positive integer");

        this.handler = handler;
        this.maxBatchSize = maxBatchSize;
        this.queue = new ArrayBlockingQueue<T>(queueSize);

        this.executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(format("batch-processor-%s", name)).build());
    }

    @PostConstruct
    public void start()
    {
        future.set(executor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                while (!Thread.interrupted()) {
                    final List<T> entries = new ArrayList<T>(maxBatchSize);

                    try {
                        T first = queue.take();
                        entries.add(first);
                        queue.drainTo(entries, maxBatchSize - 1);

                        handler.processBatch(entries);
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }));
    }

    @PreDestroy
    public void stop()
    {
        future.get().cancel(true);
        executor.shutdownNow();
    }

    public void put(T entry)
    {
        Preconditions.checkState(future != null && !future.get().isCancelled(), "Processor is not running");
        Preconditions.checkNotNull(entry, "entry is null");

        while (!queue.offer(entry)) {
            // throw away oldest and try again
            queue.poll();
            counter.get().recordLost(1);
        }

        counter.get().recordReceived(1);
    }

    public CounterState getCounterState()
    {
        return counter.get().getState();
    }

    public void resetCounter()
    {
        counter.set(new Counter());
    }

    public static interface BatchHandler<T>
    {
        void processBatch(Collection<T> entries);
    }

}
