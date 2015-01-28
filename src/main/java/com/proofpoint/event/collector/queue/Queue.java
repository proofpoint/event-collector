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

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

public interface Queue<T> extends Closeable
{
    /**
     * Returns the number of items in the queue.
     *
     * @return number of items in the queue
     */
    long getSize();

    /**
     * Inserts the specified element at the tail of this queue if it is possible to do so immediately without exceeding the queue's capacity,
     * returning true upon success and false if this queue is full.
     *
     * @param item item to add
     * @return true if successful
     * @throws IOException could not serialize item
     */
    boolean offer(T item)
            throws IOException;

    /**
     * Inserts the item at the tail of this queue.
     *
     * @param item item to add
     * @throws IOException could not serialize item
     * @throws QueueFullException if the queue is at capacity
     */
    void enqueue(T item)
            throws IOException, QueueFullException;

    /**
     * Inserts the items at the tail of this queue.
     *
     * @param items items to add
     * @throws IOException could not serialize an item
     * @throws QueueFullException if the queue is at capacity
     */
    void enqueueAll(List<T> items)
            throws IOException, QueueFullException;

    /**
     * Returns up to chunkSize items from the front of the queue.
     *
     * @param numItems maximum number of items to return
     * @return items returns
     * @throws IOException failed to dequeue items
     */
    List<T> dequeue(int numItems)
            throws IOException;

    /**
     * Release resources used by this Queue.
     *
     * @throws IOException if fails to release resources
     */
    void close()
            throws IOException;

    /**
     * Empties the queue.
     *
     * @throws IOException failed to remove items
     */
    void removeAll()
            throws IOException;

    String getName();
}
