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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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

import static com.proofpoint.event.collector.EventCollectorStats.EventStatus.UNSUPPORTED;
import static com.proofpoint.event.collector.EventCollectorStats.EventStatus.VALID;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
    private EventCollectorStats eventCollectorStats;

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
        eventCollectorStats = mock(EventCollectorStats.class);

        when(eventCollectorStats.incomingEvents(anyString(), eq(VALID))).thenReturn(counterStatForValidType);
        when(eventCollectorStats.incomingEvents(anyString(), eq(UNSUPPORTED))).thenReturn(counterStatForUnsupportedType);
    }

    @Test
    public void testPost()
            throws IOException
    {
        try {
            EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes("Test"), eventCollectorStats);

            ImmutableMap<String, String> data = ImmutableMap.of("foo", "bar", "hello", "world");
            Event event = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), data);

            List<Event> events = ImmutableList.of(event);
            Response response = resource.post(events);

            assertEquals(response.getStatus(), Status.ACCEPTED.getStatusCode());
            assertNull(response.getEntity());
            assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces

            assertEquals(writer.getEvents(), events);

            assertEquals(counterStatForValidType.getTotalCount(), 1);
            assertEquals(counterStatForUnsupportedType.getTotalCount(), 0);
            verify(eventCollectorStats).incomingEvents("Test", VALID);
            verify(eventCollectorStats, never()).incomingEvents("Test", UNSUPPORTED);

            verifyNoMoreInteractions(eventCollectorStats);
        }
        finally {
            ensureCleanWorkingDirectory();
        }
    }

    @Test
    public void testPostUnsupportedType()
            throws IOException
    {
        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes("Test"), eventCollectorStats);

        ImmutableMap<String, String> data = ImmutableMap.of("foo", "bar", "hello", "world");
        Event event1 = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), data);
        Event event2 = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), data);
        Event badEvent = new Event("TestBad", UUID.randomUUID().toString(), "test.local", new DateTime(), data);

        List<Event> events = ImmutableList.of(event1, event2, badEvent);
        Response response = resource.post(events);

        assertEquals(response.getStatus(), Status.BAD_REQUEST.getStatusCode());
        assertNotNull(response.getEntity());
        assertTrue(response.getEntity().toString().startsWith("Unsupported event type(s): "));
        assertTrue(response.getEntity().toString().contains("TestBad"));

        assertEquals(counterStatForValidType.getTotalCount(), 2);
        assertEquals(counterStatForUnsupportedType.getTotalCount(), 1);
        verify(eventCollectorStats, times(2)).incomingEvents("Test", VALID);
        verify(eventCollectorStats).incomingEvents("TestBad", UNSUPPORTED);

        verifyNoMoreInteractions(eventCollectorStats);
    }

    @Test
    public void testAcceptAllEvents()
            throws IOException
    {
        String eventTypeA = UUID.randomUUID().toString();
        String eventTypeB = UUID.randomUUID().toString();
        try {
            EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig(), eventCollectorStats);

            ImmutableMap<String, String> data = ImmutableMap.of("foo", "bar", "hello", "world");
            Event eventWithTypeA = new Event(eventTypeA, UUID.randomUUID().toString(), "test.local", new DateTime(), data);
            Event eventWithTypeB = new Event(eventTypeB, UUID.randomUUID().toString(), "test.local", new DateTime(), data);

            List<Event> events = ImmutableList.of(eventWithTypeA, eventWithTypeB);
            Response response = resource.post(events);

            assertEquals(response.getStatus(), Status.ACCEPTED.getStatusCode());
            assertNull(response.getEntity());
            assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces

            assertEquals(writer.getEvents(), events);

            assertEquals(counterStatForValidType.getTotalCount(), 2);
            assertEquals(counterStatForUnsupportedType.getTotalCount(), 0);
            verify(eventCollectorStats).incomingEvents(eventTypeA, VALID);
            verify(eventCollectorStats).incomingEvents(eventTypeB, VALID);

            verifyNoMoreInteractions(eventCollectorStats);
        }
        finally {
            ensureCleanWorkingDirectory();
        }
    }
}