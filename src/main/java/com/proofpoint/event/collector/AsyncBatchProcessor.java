/*
 * Copyright 2011-2013 Proofpoint, Inc.
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
import com.proofpoint.log.Logger;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

public class AsyncBatchProcessor<T>
        implements BatchProcessor<T>
{
    private static final Logger log = Logger.get(AsyncBatchProcessor.class);
    private final BatchHandler<T> handler;
    private final EventQueue<T> queue;
    private final ExecutorService executor;
    private final Observer observer;
    private final AtomicReference<Future<?>> future = new AtomicReference<>();
    private final int batchSize;
    private final AtomicBoolean shutDownFlag = new AtomicBoolean(false);

    public AsyncBatchProcessor(String name, BatchHandler<T> handler, BatchProcessorConfig config, Observer observer)
    {
        checkNotNull(name, "name is null");
        checkNotNull(handler, "handler is null");

        this.handler = handler;
        this.queue = new EventQueue(config);
        batchSize = checkNotNull(config, "config is null").getMaxBatchSize();

        this.executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat(format("batch-processor-%s", name)).build());
        this.observer = checkNotNull(observer, "observer is null");
    }

    @Override
    public void start()
    {
        future.set(executor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                while (true) {
                    try {
                        List<T> entries = queue.take(batchSize);
                        handler.processBatch(entries);
                    }
                    catch (InterruptedException e) {
                        break;
                    }
                    catch (Exception e) {
                        log.error(e, "error occurred during batch processing");
                    }
                }
            }
        }));
    }

    @Override
    public void stop()
    {
        shutDownFlag.set(true);
        queue.stop();
    }

    @Override
    public void put(T entry)
    {
        checkState(!shutDownFlag.get(), "Processor is not running");
        checkNotNull(entry, "entry is null");

        if (!queue.put(entry)) {
            // queue is full: drop current message
            handler.notifyEntriesDropped(1);
            observer.onRecordsLost(1);
        }
        observer.onRecordsReceived(1);
    }
}
