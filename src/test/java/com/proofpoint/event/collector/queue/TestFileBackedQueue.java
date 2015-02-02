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

import com.beust.jcommander.internal.Lists;
import com.google.common.collect.ImmutableList;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.log.Logging;
import com.proofpoint.testing.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.proofpoint.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

public class TestFileBackedQueue
{
    static {
        Logging.initialize().setLevel("com.leansoft.bigqueue", Logging.Level.INFO);
    }

    private static final JsonCodec<String> EVENT_CODEC = jsonCodec(String.class).withoutPretty();
    private static final String DATA_DIRECTORY = "target/queue";
    private FileBackedQueue<String> queue;

    @BeforeMethod
    public void setup()
            throws IOException
    {
        FileUtils.deleteRecursively(new File(DATA_DIRECTORY));
        queue = new FileBackedQueue<>("queue", DATA_DIRECTORY, EVENT_CODEC, Long.MAX_VALUE);
    }

    @AfterMethod
    public void teardown()
            throws IOException
    {
        queue.close();
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "name cannot be null or empty")
    public void testConstructorNullNameInvalid()
            throws IOException
    {
        new FileBackedQueue<>(null, DATA_DIRECTORY, EVENT_CODEC, 10);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "name cannot be null or empty")
    public void testConstructorEmptyNameInvalid()
            throws IOException
    {
        new FileBackedQueue<>("", DATA_DIRECTORY, EVENT_CODEC, 10);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "dataDirectory is null")
    public void testConstructorDataDirectoryNullFails()
            throws IOException
    {
        new FileBackedQueue<>("queue", null, EVENT_CODEC, 10);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "codec is null")
    public void testConstructorCodecNullFails()
            throws IOException
    {
        new FileBackedQueue<>("queue", DATA_DIRECTORY, null, 10);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "dataDirectory is empty")
    public void testConstructorDataDirectoryEmptyFails()
            throws IOException
    {
        new FileBackedQueue<>("queue", "", EVENT_CODEC, 10);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "capacity must be greater than zero")
    public void testConstructorCapacityLessThanOneInvalid()
            throws IOException
    {
        new FileBackedQueue<>("queue", "data", EVENT_CODEC, 0);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "item is null")
    public void testAddItemNullFails()
            throws IOException
    {
        queue.enqueueOrDrop(null);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "items are null")
    public void testAddItemsNullFails()
            throws IOException
    {
        queue.enqueueAllOrDrop(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "numItems must be greater than zero")
    public void testDequeueNumItemsLessThanOneFails()
            throws IOException
    {
        queue.dequeue(0);
    }

    @Test
    public void testQueueDequeueIndividual()
            throws IOException
    {
        queue.enqueueOrDrop("foo");
        queue.enqueueOrDrop("fi");
        queue.enqueueOrDrop("bar");

        List<String> take = queue.dequeue(3);

        assertEquals(take.size(), 3);
        assertEquals(queue.getItemsEnqueued(), 3);
        assertEquals(queue.getItemsDequeued(), 3);
        assertEquals(queue.getSize(), 0);
        assertEquals(queue.getItemsDropped(), 0);
        assertEquals(take, ImmutableList.of("foo", "fi", "bar"));
    }

    @Test
    public void testQueueDequeueList()
            throws IOException
    {
        queue.enqueueAllOrDrop(ImmutableList.of("foo", "fi", "bar"));

        List<String> take = queue.dequeue(3);

        assertEquals(take.size(), 3);
        assertEquals(queue.getItemsEnqueued(), 3);
        assertEquals(queue.getItemsDequeued(), 3);
        assertEquals(queue.getItemsDropped(), 0);
        assertEquals(queue.getSize(), 0);
        assertEquals(take, ImmutableList.of("foo", "fi", "bar"));

        take = queue.dequeue(3);

        assertEquals(take.size(), 0);
    }

    @Test
    public void testDequeueChunkSize()
            throws IOException
    {
        queue.enqueueAllOrDrop(ImmutableList.of("foo", "fi", "bar", "fum", "far"));

        List<String> take = queue.dequeue(3);

        assertEquals(take.size(), 3);
        assertEquals(queue.getItemsEnqueued(), 5);
        assertEquals(queue.getItemsDequeued(), 3);
        assertEquals(queue.getSize(), 2);
        assertEquals(take, ImmutableList.of("foo", "fi", "bar"));

        take = queue.dequeue(3);

        assertEquals(take.size(), 2);
        assertEquals(queue.getItemsDequeued(), 2);
        assertEquals(queue.getSize(), 0);
        assertEquals(take, ImmutableList.of("fum", "far"));
        assertEquals(queue.getItemsDropped(), 0);
    }

    @Test
    public void testRemoveAll()
            throws IOException
    {
        queue.enqueueAllOrDrop(ImmutableList.of("foo", "fi", "bar", "fum", "far"));

        assertEquals(queue.getSize(), 5);

        queue.removeAll();

        assertEquals(queue.getItemsEnqueued(), 5);
        assertEquals(queue.getItemsDequeued(), 0);
        assertEquals(queue.getSize(), 0);
        assertEquals(queue.getItemsDropped(), 0);
    }

    @Test
    public void testOfferQueueFull()
            throws IOException
    {
        queue = new FileBackedQueue<>("queue", DATA_DIRECTORY, EVENT_CODEC, 3);

        assertTrue(queue.offer("foo"));
        assertTrue(queue.offer("fi"));
        assertTrue(queue.offer("fo"));
        assertFalse(queue.offer("fum"));
        assertEquals(queue.getItemsEnqueued(), 3);
        assertEquals(queue.getItemsDequeued(), 0);
        assertEquals(queue.getItemsDropped(), 0);
    }

    @Test
    public void testEnqueueQueueFull()
            throws IOException
    {
        queue = new FileBackedQueue<>("queue", DATA_DIRECTORY, EVENT_CODEC, 3);

        queue.enqueueOrDrop("foo");
        queue.enqueueOrDrop("fi");
        queue.enqueueOrDrop("fo");

        boolean enqueued = queue.enqueueOrDrop("fum");

        assertFalse(enqueued);
        assertEquals(queue.getItemsEnqueued(), 3);
        assertEquals(queue.getItemsDequeued(), 0);
        assertEquals(queue.getItemsDropped(), 1);
    }

    @Test
    public void testEnqueueAllQueueFull()
            throws IOException
    {
        queue = new FileBackedQueue<>("queue", DATA_DIRECTORY, EVENT_CODEC, 3);

        List<String> enqueued = queue.enqueueAllOrDrop(ImmutableList.of("foo", "fi", "bar", "fum", "far"));

        assertEquals(enqueued, ImmutableList.of("foo", "fi", "bar"));
        assertEquals(queue.getSize(), 3);
        assertEquals(queue.getItemsEnqueued(), 3);
        assertEquals(queue.getItemsDequeued(), 0);
        assertEquals(queue.getItemsDropped(), 2);
    }

    @Test
    public void testMultipleWriters()
            throws InterruptedException, IOException
    {
        Writer writer1 = new Writer(queue, 10000);
        Writer writer2 = new Writer(queue, 10000);
        writer1.join();
        writer2.join();

        List<String> take = queue.dequeue(30000);

        assertEquals(take.size(), 20000);
    }

    @Test
    public void testWritersAndReaders()
            throws InterruptedException, IOException
    {
        Writer writer1 = new Writer(queue, 1000);
        Writer writer2 = new Writer(queue, 1000);
        Writer writer3 = new Writer(queue, 1000);
        Reader reader1 = new Reader(queue);
        Reader reader2 = new Reader(queue);
        writer1.join();
        writer2.join();
        writer3.join();

        reader1.setWritingDone(true);
        reader2.setWritingDone(true);
        reader1.join();
        reader2.join();

        List<String> items = Lists.newArrayList();
        items.addAll(reader1.getItems());
        items.addAll(reader2.getItems());

        assertEquals(items.size(), 3000);
    }

    private class Writer extends Thread
    {
        private int itemCount;
        private Queue<String> queue;

        private Writer(Queue<String> queue, int itemCount)
        {
            this.itemCount = itemCount;
            this.queue = queue;
            start();
        }

        @Override
        public void run()
        {
            for (int i = 0; i < itemCount; i++) {
                try {
                    queue.enqueueOrDrop(i + "");
                }
                catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private class Reader extends Thread
    {
        private Queue<String> queue;
        private List<String> items = Lists.newArrayList();
        private boolean done = false;

        private Reader(Queue<String> queue)
        {
            this.queue = queue;
            start();
        }

        @Override
        public void run()
        {
            try {
                while (true) {
                    List<String> list = queue.dequeue(1);
                    if (done && list.size() < 1) {
                        break;
                    }

                    items.addAll(list);
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        public List<String> getItems()
        {
            return items;
        }

        public void setWritingDone(boolean done)
        {
            this.done = done;
        }
    }
}