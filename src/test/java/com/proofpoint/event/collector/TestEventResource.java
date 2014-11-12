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
import com.proofpoint.event.collector.EventCollectorStats.EventStatus;
import com.proofpoint.event.collector.EventCollectorStats.ProcessType;
import com.proofpoint.reporting.testing.TestingReportCollectionFactory;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.proofpoint.event.collector.EventCollectorStats.EventStatus.UNSUPPORTED;
import static com.proofpoint.event.collector.EventCollectorStats.EventStatus.VALID;
import static com.proofpoint.event.collector.EventCollectorStats.ProcessType.DISTRIBUTE;
import static com.proofpoint.event.collector.EventCollectorStats.ProcessType.WRITE;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static javax.ws.rs.core.Response.Status.ACCEPTED;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;

public class TestEventResource
{
    private static final Map<String, String> ARBITRARY_DATA = ImmutableMap.of("foo", "bar", "hello", "world");

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
    public void testWrite()
            throws IOException
    {
        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes("Test"), eventCollectorStats);

        Event event = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);

        List<Event> events = ImmutableList.of(event);
        Response response = resource.write(events);

        verifyAcceptedResponse(response);

        verifyWrittenAndDistributedEvents(events, ImmutableList.<Event>of());

        verifyMetrics(WRITE, ImmutableList.of(new EventMetric("Test", VALID, 1)));
    }

    @Test
    public void testWriteSmile()
            throws IOException
    {
        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes("Test"), eventCollectorStats);

        Event event = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);

        List<Event> events = ImmutableList.of(event);
        Response response = resource.writeSmile(events);

        verifyAcceptedResponse(response);

        verifyWrittenAndDistributedEvents(events, ImmutableList.<Event>of());

        verifyMetrics(WRITE, ImmutableList.of(new EventMetric("Test", VALID, 1)));
    }

    @Test
    public void testWriteUnsupportedType()
            throws IOException
    {
        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes("Test"), eventCollectorStats);

        Event event1 = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);
        Event badEvent1 = new Event("TestBad", UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);
        Event badEvent2 = new Event("TestBad", UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);

        List<Event> events = ImmutableList.of(event1, badEvent1, badEvent2);
        Response response = resource.write(events);

        verifyBadRequestResponse(response);

        verifyWrittenAndDistributedEvents(ImmutableList.of(event1), ImmutableList.<Event>of());

        verifyMetrics(WRITE, ImmutableList.of(new EventMetric("Test", VALID, 1), new EventMetric("TestBad", UNSUPPORTED, 2)));
    }

    @Test
    public void testWriteAcceptAllEvents()
            throws IOException
    {
        String eventTypeA = UUID.randomUUID().toString();
        String eventTypeB = UUID.randomUUID().toString();

        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig(), eventCollectorStats);

        Event event1WithTypeA = new Event(eventTypeA, UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);
        Event event2WithTypeA = new Event(eventTypeA, UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);
        Event eventWithTypeB = new Event(eventTypeB, UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);

        List<Event> events = ImmutableList.of(event1WithTypeA, event2WithTypeA, eventWithTypeB);
        Response response = resource.write(events);

        verifyAcceptedResponse(response);

        verifyWrittenAndDistributedEvents(events, ImmutableList.<Event>of());

        verifyMetrics(WRITE, ImmutableList.of(new EventMetric(eventTypeA, VALID, 2), new EventMetric(eventTypeB, VALID, 1)));
    }

    @Test
    public void testDistribute()
            throws IOException
    {
        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes("Test"), eventCollectorStats);

        Event event = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);

        List<Event> events = ImmutableList.of(event);
        Response response = resource.distribute(events);

        verifyAcceptedResponse(response);

        verifyWrittenAndDistributedEvents(ImmutableList.<Event>of(), events);

        verifyMetrics(DISTRIBUTE, ImmutableList.of(new EventMetric("Test", VALID, 1)));
    }

    @Test
    public void testDistributeSmile()
            throws IOException
    {
        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes("Test"), eventCollectorStats);

        Event event = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);

        List<Event> events = ImmutableList.of(event);
        Response response = resource.distributeSmile(events);

        verifyAcceptedResponse(response);

        verifyWrittenAndDistributedEvents(ImmutableList.<Event>of(), events);

        verifyMetrics(DISTRIBUTE, ImmutableList.of(new EventMetric("Test", VALID, 1)));
    }

    @Test
    public void testDistributeUnsupportedType()
            throws IOException
    {
        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig().setAcceptedEventTypes("Test"), eventCollectorStats);

        Event event1 = new Event("Test", UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);
        Event badEvent1 = new Event("TestBad", UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);
        Event badEvent2 = new Event("TestBad", UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);

        List<Event> events = ImmutableList.of(event1, badEvent1, badEvent2);
        Response response = resource.distribute(events);

        verifyBadRequestResponse(response);

        verifyWrittenAndDistributedEvents(ImmutableList.<Event>of(), ImmutableList.of(event1));

        verifyMetrics(DISTRIBUTE, ImmutableList.of(new EventMetric("Test", VALID, 1), new EventMetric("TestBad", UNSUPPORTED, 2)));
    }

    @Test
    public void testDistributeAcceptAllEvents()
            throws IOException
    {
        String eventTypeA = UUID.randomUUID().toString();
        String eventTypeB = UUID.randomUUID().toString();

        EventResource resource = new EventResource(ImmutableSet.<EventWriter>of(writer), new ServerConfig(), eventCollectorStats);

        Event event1WithTypeA = new Event(eventTypeA, UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);
        Event event2WithTypeA = new Event(eventTypeA, UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);
        Event eventWithTypeB = new Event(eventTypeB, UUID.randomUUID().toString(), "test.local", new DateTime(), ARBITRARY_DATA);

        List<Event> events = ImmutableList.of(event1WithTypeA, event2WithTypeA, eventWithTypeB);
        Response response = resource.distribute(events);

        verifyAcceptedResponse(response);

        verifyWrittenAndDistributedEvents(ImmutableList.<Event>of(), events);

        verifyMetrics(DISTRIBUTE, ImmutableList.of(new EventMetric(eventTypeA, VALID, 2), new EventMetric(eventTypeB, VALID, 1)));
    }

    private void verifyAcceptedResponse(Response response)
    {
        assertEquals(response.getStatus(), ACCEPTED.getStatusCode());
        assertNull(response.getEntity());
        assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces
    }

    private void verifyBadRequestResponse(Response response)
    {
        assertEquals(response.getStatus(), BAD_REQUEST.getStatusCode());
        assertNotNull(response.getEntity());
        assertTrue(response.getEntity().toString().startsWith("Unsupported event type(s): "));
        assertTrue(response.getEntity().toString().contains("TestBad"));
    }

    private void verifyWrittenAndDistributedEvents(List<Event> writtenEvents, List<Event> distributedEvents)
    {
        assertEquals(writer.getWrittenEvents(), writtenEvents);
        assertEquals(writer.getDistributedEvents(), distributedEvents);
    }

    private void verifyMetrics(ProcessType processType, List<EventMetric> metrics)
    {
        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        for (EventMetric metric : metrics) {
            verify(argumentVerifier, times(metric.getEventCount())).inboundEvents(metric.getEventType(), metric.getStatus(), processType);
        }

        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        for (EventMetric metric : metrics) {
            String eventType = metric.getEventType();
            EventStatus eventStatus = metric.getStatus();
            verify(reportCollection.inboundEvents(eventType, eventStatus, processType), times(metric.getEventCount())).add(1);
            verifyNoMoreInteractions(reportCollection.inboundEvents(eventType, eventStatus, processType));
        }
    }

    private class EventMetric
    {
        private String eventType;
        private EventStatus status;
        private int eventCount;

        public EventMetric(String eventType, EventStatus status, int eventCount)
        {
            this.eventType = eventType;
            this.status = status;
            this.eventCount = eventCount;
        }

        public String getEventType()
        {
            return eventType;
        }

        public EventStatus getStatus()
        {
            return status;
        }

        public int getEventCount()
        {
            return eventCount;
        }
    }
}