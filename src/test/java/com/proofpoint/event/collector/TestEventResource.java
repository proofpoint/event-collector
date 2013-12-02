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

import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import com.proofpoint.event.client.InMemoryEventClient;
import com.proofpoint.testing.FileUtils;
import org.joda.time.DateTime;
import org.logicalshift.concurrent.SerialScheduledExecutorService;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.proofpoint.event.collector.ProcessStats.HourlyEventCount;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestEventResource
{
    private InMemoryEventWriter writer;
    private InMemoryEventClient eventClient;
    private SerialScheduledExecutorService executor;

    @BeforeSuite
    public void ensureCleanWorkingDirectory()
    {
        File statsDirectory = new File("var/stats");
        if (statsDirectory.exists()) {
            FileUtils.deleteDirectoryContents(statsDirectory);
        }
    }

    @BeforeMethod
    public void setup()
    {
        writer = new InMemoryEventWriter();
        eventClient = new InMemoryEventClient();
        executor = new SerialScheduledExecutorService();
    }

    @Test
    public void testPost()
            throws IOException
    {
        try {
            EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes("Test"),
                    executor, eventClient);

            ImmutableMap<String, String> data = ImmutableMap.of("foo", "bar", "hello", "world");
            Event event = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), data);

            Response response = resource.post(ImmutableList.of(event));

            assertEquals(response.getStatus(), Status.ACCEPTED.getStatusCode());
            assertNull(response.getEntity());
            assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces

            assertEquals(writer.getEvents(), ImmutableList.of(event));
            checkProcessStats("Test", writer.getEvents().size());
        }
        finally {
            ensureCleanWorkingDirectory();
        }
    }

    @Test
    public void testPostInvalidType()
            throws IOException
    {
        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes("Test"),
                executor, eventClient);
        ImmutableMap<String, String> data = ImmutableMap.of("foo", "bar", "hello", "world");
        Event badEvent1 = new Event("TestBad1", UUID.randomUUID().toString(), "test.local", new DateTime(), data);
        Event badEvent2 = new Event("TestBad2", UUID.randomUUID().toString(), "test.local", new DateTime(), data);
        Response response = resource.post(ImmutableList.of(badEvent2, badEvent1));

        assertEquals(response.getStatus(), Status.BAD_REQUEST.getStatusCode());
        assertNotNull(response.getEntity());
        assertTrue(response.getEntity().toString().startsWith("Invalid event type(s): "));
        assertTrue(response.getEntity().toString().contains("TestBad1"));
        assertTrue(response.getEntity().toString().contains("TestBad2"));
    }

    @Test
    public void testAcceptAllEvents()
            throws IOException
    {
        String eventType = UUID.randomUUID().toString();
        try {
            EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig(),
                    executor, eventClient);

            ImmutableMap<String, String> data = ImmutableMap.of("foo", "bar", "hello", "world");
            Event event = new Event(eventType, UUID.randomUUID().toString(), "test.local", new DateTime(), data);

            Response response = resource.post(ImmutableList.of(event));

            assertEquals(response.getStatus(), Status.ACCEPTED.getStatusCode());
            assertNull(response.getEntity());
            assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces

            assertEquals(writer.getEvents(), ImmutableList.of(event));
            checkProcessStats(eventType, writer.getEvents().size());
        }
        finally {
            ensureCleanWorkingDirectory();
        }
    }

    private void checkProcessStats(String eventType, int count)
            throws IOException
    {
        executor.elapseTime(1, TimeUnit.MINUTES);
        checkStatsInFile("var/stats/" + eventType + "/current.txt", count);

        executor.elapseTime(1, TimeUnit.HOURS);
        checkStatsInFile("var/stats/" + eventType + "/hourly.txt", count);

        executor.elapseTime(1, TimeUnit.HOURS);
        checkStatsInFile("var/stats/" + eventType + "/hourly.txt", 0);

        executor.elapseTime(1, TimeUnit.DAYS);
        List<Object> events = eventClient.getEvents();
        assertEquals(((HourlyEventCount) events.get(0)).getCount(), count);
        assertEquals(((HourlyEventCount) events.get(0)).getEventType(), eventType);

        assertEquals(((HourlyEventCount) events.get(1)).getCount(), 0);
        assertEquals(((HourlyEventCount) events.get(1)).getEventType(), eventType);
    }

    private void checkStatsInFile(String fileName, int count)
            throws IOException
    {
        File file = new File(fileName);
        assertTrue(file.exists());

        List<String> lines = Files.readLines(file, Charsets.UTF_8);

        assertTrue(lines != null && !lines.isEmpty(), String.format("lines of %s are unexpectedly '%s'", fileName, lines));
        String line = lines.get(lines.size() - 1);

        Iterator<String> iterator = Splitter.on(" ").split(line).iterator();
        String date = iterator.next();
        long value = Long.parseLong(iterator.next());

        assertEquals(value, count);
    }
}
