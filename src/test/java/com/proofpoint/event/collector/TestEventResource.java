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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.reporting.Key;
import com.proofpoint.stats.CounterStat;
import com.proofpoint.testing.FileUtils;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestEventResource
{
    private static final CounterStat MOCK_COUNTER_STAT = mock(CounterStat.class);
    private InMemoryEventWriter writer;
    private EventResourceStats eventResourceStats;

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
        eventResourceStats = new EventResourceStats()
        {
            @Override
            public CounterStat incomingEvent(@Key("eventType") String eventType, @Key("eventStatus") EventStatus eventStatus)
            {
                return MOCK_COUNTER_STAT;
            }
        };
    }

    @Test
    public void testPost()
            throws IOException
    {
        try {
            EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes("Test"), eventResourceStats);

            ImmutableMap<String, String> data = ImmutableMap.of("foo", "bar", "hello", "world");
            Event event = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), data);

            Response response = resource.post(ImmutableList.of(event));

            assertEquals(response.getStatus(), Status.ACCEPTED.getStatusCode());
            assertNull(response.getEntity());
            assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces

            assertEquals(writer.getEvents(), ImmutableList.of(event));
        }
        finally {
            ensureCleanWorkingDirectory();
        }
    }

    @Test
    public void testPostUnsupportedType()
            throws IOException
    {
        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes("Test"), eventResourceStats);

        ImmutableMap<String, String> data = ImmutableMap.of("foo", "bar", "hello", "world");
        Event badEvent1 = new Event("TestBad1", UUID.randomUUID().toString(), "test.local", new DateTime(), data);
        Event badEvent2 = new Event("TestBad2", UUID.randomUUID().toString(), "test.local", new DateTime(), data);
        Response response = resource.post(ImmutableList.of(badEvent2, badEvent1));

        assertEquals(response.getStatus(), Status.BAD_REQUEST.getStatusCode());
        assertNotNull(response.getEntity());
        assertTrue(response.getEntity().toString().startsWith("Unsupported event type(s): "));
        assertTrue(response.getEntity().toString().contains("TestBad1"));
        assertTrue(response.getEntity().toString().contains("TestBad2"));
    }

    @Test
    public void testAcceptAllEvents()
            throws IOException
    {
        String eventType = UUID.randomUUID().toString();
        try {
            EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig(), eventResourceStats);

            ImmutableMap<String, String> data = ImmutableMap.of("foo", "bar", "hello", "world");
            Event event = new Event(eventType, UUID.randomUUID().toString(), "test.local", new DateTime(), data);

            Response response = resource.post(ImmutableList.of(event));

            assertEquals(response.getStatus(), Status.ACCEPTED.getStatusCode());
            assertNull(response.getEntity());
            assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces

            assertEquals(writer.getEvents(), ImmutableList.of(event));
        }
        finally {
            ensureCleanWorkingDirectory();
        }
    }
}
