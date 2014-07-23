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
import com.proofpoint.reporting.testing.TestingReportCollectionFactory;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.proofpoint.event.collector.EventCollectorStats.EventStatus.UNSUPPORTED;
import static com.proofpoint.event.collector.EventCollectorStats.EventStatus.VALID;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestEventResource
{
    private static final Map<String,String> ARBITRARY_DATA = ImmutableMap.of("foo", "bar", "hello", "world");

    private InMemoryEventWriter writer;
    private EventCollectorStats eventCollectorStats;
    private TestingReportCollectionFactory testingReportCollectionFactory;

    @BeforeMethod
    public void setup()
    {
        writer = new InMemoryEventWriter();
        testingReportCollectionFactory = new TestingReportCollectionFactory();
        eventCollectorStats = testingReportCollectionFactory.createReportCollection(EventCollectorStats.class);
    }

    @Test
    public void testPost()
            throws IOException
    {
        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes("Test"), eventCollectorStats);

        ImmutableMap<String, String> data = ImmutableMap.of("foo", "bar", "hello", "world");
        Event event = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), data);

        List<Event> events = ImmutableList.of(event);
        Response response = resource.post(events);

        assertEquals(response.getStatus(), Status.ACCEPTED.getStatusCode());
        assertNull(response.getEntity());
        assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces

        assertEquals(writer.getEvents(), events);

        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        verify(argumentVerifier).incomingEvents("Test", VALID);
        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        verify(reportCollection.incomingEvents("Test", VALID)).add(1);
        verifyNoMoreInteractions(reportCollection.incomingEvents("Test", VALID));
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

        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        verify(argumentVerifier, times(2)).incomingEvents("Test", VALID);
        verify(argumentVerifier).incomingEvents("TestBad", UNSUPPORTED);
        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        verify(reportCollection.incomingEvents("Test", VALID), times(2)).add(1);
        verify(reportCollection.incomingEvents("TestBad", UNSUPPORTED)).add(1);
        verifyNoMoreInteractions(reportCollection.incomingEvents("Test", VALID));
        verifyNoMoreInteractions(reportCollection.incomingEvents("TestBad", UNSUPPORTED));
    }

    @Test
    public void testAcceptAllEvents()
            throws IOException
    {
        String eventTypeA = UUID.randomUUID().toString();
        String eventTypeB = UUID.randomUUID().toString();

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

        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        verify(argumentVerifier).incomingEvents(eventTypeA, VALID);
        verify(argumentVerifier).incomingEvents(eventTypeB, VALID);
        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        verify(reportCollection.incomingEvents(eventTypeA, VALID)).add(1);
        verify(reportCollection.incomingEvents(eventTypeB, VALID)).add(1);
        verifyNoMoreInteractions(reportCollection.incomingEvents(eventTypeA, VALID));
        verifyNoMoreInteractions(reportCollection.incomingEvents(eventTypeB, VALID));
    }

    @Test
    public void testDistribute()
            throws IOException
    {
        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes("Test"), eventCollectorStats);

        Event event = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);

        List<Event> events = ImmutableList.of(event);
        Response response = resource.distribute(events);

        assertEquals(response.getStatus(), Status.ACCEPTED.getStatusCode());
        assertNull(response.getEntity());
        assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces

        assertEquals(writer.getDistributedEvents(), events);

        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        verify(argumentVerifier).incomingEvents("Test", VALID);
        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        verify(reportCollection.incomingEvents("Test", VALID)).add(1);
        verifyNoMoreInteractions(reportCollection.incomingEvents("Test", VALID));
    }

    @Test
    public void testDistributeUnsupportedType()
            throws IOException
    {
        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes("Test"), eventCollectorStats);

        Event event1 = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);
        Event event2 = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);
        Event badEvent = new Event("TestBad", UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);

        List<Event> events = ImmutableList.of(event1, event2, badEvent);
        Response response = resource.distribute(events);

        assertEquals(response.getStatus(), Status.BAD_REQUEST.getStatusCode());
        assertNotNull(response.getEntity());
        assertTrue(response.getEntity().toString().startsWith("Unsupported event type(s): "));
        assertTrue(response.getEntity().toString().contains("TestBad"));

        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        verify(argumentVerifier, times(2)).incomingEvents("Test", VALID);
        verify(argumentVerifier).incomingEvents("TestBad", UNSUPPORTED);
        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        verify(reportCollection.incomingEvents("Test", VALID), times(2)).add(1);
        verify(reportCollection.incomingEvents("TestBad", UNSUPPORTED)).add(1);
        verifyNoMoreInteractions(reportCollection.incomingEvents("Test", VALID));
        verifyNoMoreInteractions(reportCollection.incomingEvents("TestBad", UNSUPPORTED));
    }

    @Test
    public void testDistributeAcceptAllEvents()
            throws IOException
    {
        String eventTypeA = UUID.randomUUID().toString();
        String eventTypeB = UUID.randomUUID().toString();

        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig(), eventCollectorStats);

        Event eventWithTypeA = new Event(eventTypeA, UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);
        Event eventWithTypeB = new Event(eventTypeB, UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);

        List<Event> events = ImmutableList.of(eventWithTypeA, eventWithTypeB);
        Response response = resource.distribute(events);

        assertEquals(response.getStatus(), Status.ACCEPTED.getStatusCode());
        assertNull(response.getEntity());
        assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces

        assertEquals(writer.getDistributedEvents(), events);

        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        verify(argumentVerifier).incomingEvents(eventTypeA, VALID);
        verify(argumentVerifier).incomingEvents(eventTypeB, VALID);
        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        verify(reportCollection.incomingEvents(eventTypeA, VALID)).add(1);
        verify(reportCollection.incomingEvents(eventTypeB, VALID)).add(1);
        verifyNoMoreInteractions(reportCollection.incomingEvents(eventTypeA, VALID));
        verifyNoMoreInteractions(reportCollection.incomingEvents(eventTypeB, VALID));
    }
}