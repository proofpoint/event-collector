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

import com.proofpoint.log.Logger;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.Math.min;

public class EventQueue<T>
{
    private static final Logger log = Logger.get(EventQueue.class);
    private final int maxQueueSize;
    private final Queue<T> queue;
    private boolean shutdownFlag = false;

    public EventQueue(BatchProcessorConfig config)
    {
        maxQueueSize = checkNotNull(config, "config is null").getQueueSize();
        queue = new LinkedList<>();
    }

    public synchronized List<T> take(int batchSize) throws InterruptedException
    {
        checkArgument(batchSize > 0, "batchSize must be positive");
        while (queue.isEmpty()) {
            if (shutdownFlag) {
                throw new InterruptedException("shutdown called");
            }
            try {
                wait();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.debug(e, "InterruptedException in take");
            }
        }
        final List<T> entries = new ArrayList<>(min(batchSize, queue.size()));

        int itemsRemoved = 0;
        T entry;
        while ((entry = queue.poll()) != null && itemsRemoved++ < batchSize) {
            entries.add(entry);
        }
        return entries;
    }

    public synchronized boolean put(T entry)
    {
        if (queue.size() >= maxQueueSize) {
            return false;
        }

        if (queue.isEmpty()) {
            notifyAll();
        }
        queue.add(entry);
        return true;
    }

    public synchronized void stop()
    {
        shutdownFlag = true;
        notifyAll();
    }
}
