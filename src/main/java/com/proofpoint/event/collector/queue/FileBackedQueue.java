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
package com.proofpoint.event.collector.queue;


import com.google.common.collect.Lists;
import com.leansoft.bigqueue.BigQueueImpl;
import com.leansoft.bigqueue.IBigQueue;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.log.Logger;
import com.proofpoint.reporting.Reported;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class FileBackedQueue<T> implements Queue<T>
{
    private static final Logger log = Logger.get(FileBackedQueue.class);

    private final IBigQueue queue;
    private final JsonCodec<T> codec;
    private final long capacity;
    private final String name;

    private final AtomicLong itemsEnqueued = new AtomicLong(0);
    private final AtomicLong itemsDequeued = new AtomicLong(0);

    public FileBackedQueue(String name, String dataDirectory, JsonCodec<T> codec, long capacity)
            throws IOException
    {
        this.codec = checkNotNull(codec, "codec is null");

        checkArgument(name != null && !name.isEmpty(), "name cannot be null or empty");
        checkNotNull(dataDirectory, "dataDirectory is null");
        checkArgument(!dataDirectory.isEmpty(), "dataDirectory is empty");
        checkArgument(capacity > 0, "capacity must be greater than zero");

        // create data directory if it's missing
        Path dataDirPath = Paths.get(dataDirectory);
        Files.createDirectories(dataDirPath); // omitting FileAttributes parameter in order to avoid dealing with POSIX vs ACL file permissions

        this.name = name;
        this.queue = new BigQueueImpl(dataDirectory, name);
        this.capacity = capacity;

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.schedule(new FileCleaner(), 1, TimeUnit.MINUTES);
    }

    @Reported
    public long getItemsEnqueued()
    {
        return itemsEnqueued.get();
    }

    @Reported
    public long getItemsDequeued()
    {
        return itemsDequeued.get();
    }

    @Override
    @Reported
    public long getSize()
    {
        return queue.size();
    }

    @Override
    public boolean offer(T item)
            throws IOException
    {
        checkNotNull(item, "item is null");

        if (queue.size() < capacity) {
            queue.enqueue(codec.toJson(item).getBytes("UTF-8"));
            itemsEnqueued.getAndAdd(1);
            return true;
        }
        else {
            return false;
        }
    }

    @Override
    public void enqueue(T item)
            throws IOException, QueueFullException
    {
        checkNotNull(item, "item is null");
        if (queue.size() >= capacity) {
            throw new QueueFullException("queue is full");
        }

        queue.enqueue(codec.toJson(item).getBytes("UTF-8"));
        itemsEnqueued.getAndAdd(1);
    }

    @Override
    public void enqueueAll(List<T> items)
            throws IOException, QueueFullException
    {
        checkNotNull(items, "items are null");
        for (T item : items) {
            enqueue(item);
        }
    }

    @Override
    public List<T> dequeue(int numItems)
            throws IOException
    {
        checkArgument(numItems > 0, "chunk size must be greater than zero");
        List<T> items = Lists.newArrayList();
        int chunks = 0;
        while (chunks < numItems) {
            byte[] item = queue.dequeue();
            if (item == null) {
                break;
            }
            items.add(codec.fromJson(item));
            chunks++;
        }

        itemsDequeued.getAndAdd(items.size());
        return items;
    }

    @Override
    public void close()
            throws IOException
    {
        queue.close();
    }

    @Override
    public void removeAll()
            throws IOException
    {
        queue.removeAll();
    }

    @Override
    public String getName()
    {
        return name;
    }

    private class FileCleaner implements Runnable
    {
        @Override
        public void run()
        {
            try {
                queue.gc();
            }
            catch (IOException e) {
                log.error(e, "Could not remove old queue files.");
            }
        }
    }
}
