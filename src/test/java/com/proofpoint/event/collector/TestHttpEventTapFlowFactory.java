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
import com.proofpoint.event.collector.EventCollectorStats.Status;
import com.proofpoint.http.client.Request;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.stats.CounterStat;
import com.proofpoint.units.Duration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static java.util.UUID.randomUUID;

public class TestHttpEventTapFlowFactory
{
    private final String eventTypeA = "eventTypeA";
    private final String flowIdA = "flowIdA";
    private final Set<URI> tapsA = ImmutableSet.of(URI.create("http://foo.bar"), URI.create("http://bar.foo"));
    private final String eventTypeB = "eventTypeB";
    private final String flowIdB = "flowIdB";
    private final Set<URI> tapsB = ImmutableSet.of(URI.create("http://erehw.on"), URI.create("http://aol.com"));
    private final int qosRetryCount = 2;

    private final EventTapConfig config = new EventTapConfig().setEventTapQosRetryCount(qosRetryCount).setEventTapQosRetryDelay(new Duration(1, TimeUnit.MILLISECONDS));
    private MockHttpClient httpClient;
    private JsonCodec<List<Event>> jsonCodec;
    private HttpEventTapFlowFactory factory;
    private EventCollectorStats eventCollectorStats;
    private CounterStat counterForDroppedEvents;
    private CounterStat counterForLost;
    private CounterStat counterForDelivered;

    @BeforeMethod
    public void setup()
    {
        httpClient = new MockHttpClient();
        jsonCodec = JsonCodec.listJsonCodec(Event.class);
        counterForDroppedEvents = mock(CounterStat.class);
        counterForLost = mock(CounterStat.class);
        counterForDelivered = mock(CounterStat.class);
        CounterStat counterForRejected = mock(CounterStat.class);

        eventCollectorStats = mock(EventCollectorStats.class);
        when(eventCollectorStats.outboundEvents(anyString(), anyString(), eq(Status.DROPPED))).thenReturn(counterForDroppedEvents);
        when(eventCollectorStats.outboundEvents(anyString(), anyString(), eq(Status.DELIVERED))).thenReturn(counterForDelivered);
        when(eventCollectorStats.outboundEvents(anyString(), anyString(), eq(Status.LOST))).thenReturn(counterForLost);
        when(eventCollectorStats.outboundEvents(anyString(), anyString(), anyString(), eq(Status.REJECTED))).thenReturn(counterForRejected);

        factory = new HttpEventTapFlowFactory(httpClient, jsonCodec, config, eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "httpClient is null")
    public void testConstructorNullHttpClient()
    {
        new HttpEventTapFlowFactory(null, jsonCodec, config, eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventCodec is null")
    public void testConstructorNullEventCodec()
    {
        new HttpEventTapFlowFactory(httpClient, null, config, eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "config is null")
    public void testConstructorNullConfig()
    {
        new HttpEventTapFlowFactory(httpClient, jsonCodec, null, eventCollectorStats);
    }

    @Test
    public void testNonQosCreate()
            throws Exception
    {
        testCreatedEventTapFlow(factory.createEventTapFlow(eventTypeA, flowIdA, tapsA),
                eventTypeA, flowIdA, tapsA, 0);
    }

    @Test
    public void testNonQosCreateWithoutObserver()
            throws Exception
    {
        testCreatedEventTapFlow(factory.createEventTapFlow(eventTypeB, flowIdB, tapsB),
                eventTypeB, flowIdB, tapsB, 0);
    }

    @Test
    public void testQosCreate()
            throws Exception
    {
        testCreatedEventTapFlow(factory.createQosEventTapFlow(eventTypeA, flowIdA, tapsA),
                eventTypeA, flowIdA, tapsA,  qosRetryCount);
    }

    @Test
    public void testQosCreateWithoutObserver()
            throws Exception
    {
        testCreatedEventTapFlow(factory.createQosEventTapFlow(eventTypeB, flowIdB, tapsB),
                eventTypeB, flowIdB, tapsB, qosRetryCount);
    }

    private void testCreatedEventTapFlow(EventTapFlow eventTapFlow, String eventType, String flowId, Set<URI> taps, int retryCount)
            throws Exception
    {
        List<Event> events = ImmutableList.of(createEvent(eventType));
        assertEquals(eventTapFlow.getClass(), HttpEventTapFlow.class);
        HttpEventTapFlow httpEventTapFlow = (HttpEventTapFlow) eventTapFlow;

        assertEquals(httpEventTapFlow.getEventType(), eventType);
        assertEquals(httpEventTapFlow.getFlowId(), flowId);
        assertEquals(httpEventTapFlow.getTaps(), taps);

        eventTapFlow.processBatch(events);
        List<Request> requests = httpClient.getRequests();
        assertEquals(requests.size(), 1);

        httpClient.clearRequests();
        httpClient.respondWithException();
        eventTapFlow.processBatch(events);

        requests = httpClient.getRequests();
        assertEquals(requests.size(), taps.size() * (retryCount + 1));

    }

    private Event createEvent(String eventType)
    {
        return new Event(eventType, randomUUID().toString(), "http://host.com",
                DateTime.now(DateTimeZone.UTC), ImmutableMap.of("foo", "bar"));
    }
}
