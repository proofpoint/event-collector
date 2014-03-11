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
import com.proofpoint.event.collector.EventResourceStats.EventStatus;
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
import java.util.List;
import java.util.UUID;

import static com.proofpoint.event.collector.EventResourceStats.EventStatus.VALID;
import static com.proofpoint.event.collector.EventResourceStats.EventStatus.UNSUPPORTED;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestEventResource
{
    private CounterStat counterStatForValidType;
    private CounterStat counterStatForUnsupportedType;
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
        counterStatForValidType = new CounterStat();
        counterStatForUnsupportedType = new CounterStat();
        writer = new InMemoryEventWriter();
        eventResourceStats = mock(EventResourceStats.class);

        when(eventResourceStats.incomingEvent(anyString(), eq(VALID))).thenReturn(counterStatForValidType);
        when(eventResourceStats.incomingEvent(anyString(), eq(UNSUPPORTED))).thenReturn(counterStatForUnsupportedType);
    }

    @Test
    public void testPost()
            throws IOException
    {
        try {
            EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes("Test"), eventResourceStats);

            ImmutableMap<String, String> data = ImmutableMap.of("foo", "bar", "hello", "world");
            Event event = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), data);

            List<Event> events = ImmutableList.of(event);
            Response response = resource.post(events);

            assertEquals(response.getStatus(), Status.ACCEPTED.getStatusCode());
            assertNull(response.getEntity());
            assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces

            assertEquals(writer.getEvents(), ImmutableList.of(event));

            assertNotNull(counterStatForValidType);
            assertEquals(counterStatForValidType.getTotalCount(), events.size());

            verifyMetric("Test", VALID, counterStatForValidType, events.size());
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
        Event event = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), data);
        Event badEvent = new Event("TestBad", UUID.randomUUID().toString(), "test.local", new DateTime(), data);

        List<Event> events = ImmutableList.of(event, badEvent);
        Response response = resource.post(events);

        assertEquals(response.getStatus(), Status.BAD_REQUEST.getStatusCode());
        assertNotNull(response.getEntity());
        assertTrue(response.getEntity().toString().startsWith("Unsupported event type(s): "));
        assertTrue(response.getEntity().toString().contains("TestBad"));

        verifyMetric("Test", VALID, counterStatForValidType, 1);
        verifyMetric("TestBad", UNSUPPORTED, counterStatForUnsupportedType, 1);
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

            List<Event> events = ImmutableList.of(event);
            Response response = resource.post(events);

            assertEquals(response.getStatus(), Status.ACCEPTED.getStatusCode());
            assertNull(response.getEntity());
            assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces

            assertEquals(writer.getEvents(), ImmutableList.of(event));

            verifyMetric(eventType, VALID, counterStatForValidType, events.size() );
        }
        finally {
            ensureCleanWorkingDirectory();
        }
    }

    private void verifyMetric(String eventType, EventStatus eventStatus, CounterStat counterStat, int count)
    {
        assertNotNull(counterStat);
        assertEquals(counterStat.getTotalCount(), count);
        verify(eventResourceStats).incomingEvent(eventType, eventStatus);
    }
}
