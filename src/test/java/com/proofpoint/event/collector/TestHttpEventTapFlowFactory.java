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
import com.proofpoint.event.collector.EventTapFlow.Observer;
import com.proofpoint.http.client.Request;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.units.Duration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.proofpoint.event.collector.EventTapFlow.NULL_OBSERVER;
import static java.util.UUID.randomUUID;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;

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
    private Observer observer;

    @BeforeMethod
    public void setup()
    {
        httpClient = new MockHttpClient();
        jsonCodec = JsonCodec.listJsonCodec(Event.class);
        factory = new HttpEventTapFlowFactory(httpClient, jsonCodec, config);
        observer = mock(Observer.class);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "httpClient is null")
    public void testConsturctorNullHttpClient()
    {
        new HttpEventTapFlowFactory(null, jsonCodec, config);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventCodec is null")
    public void testConstructorNullEventCodec()
    {
        new HttpEventTapFlowFactory(httpClient, null, config);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "config is null")
    public void testConstructorNullConfig()
    {
        new HttpEventTapFlowFactory(httpClient, jsonCodec, null);
    }

    @Test
    public void testNonQosCreate()
            throws Exception
    {
        testCreatedEventTapFlow(factory.createEventTapFlow(eventTypeA, flowIdA, tapsA, observer),
                eventTypeA, flowIdA, tapsA, 0, observer);
    }

    @Test
    public void testNonQosCreateWithoutObserver()
            throws Exception
    {
        testCreatedEventTapFlow(factory.createEventTapFlow(eventTypeB, flowIdB, tapsB),
                eventTypeB, flowIdB, tapsB, 0, NULL_OBSERVER);
    }

    @Test
    public void testQosCreate()
            throws Exception
    {
        testCreatedEventTapFlow(factory.createQosEventTapFlow(eventTypeA, flowIdA, tapsA, observer),
                eventTypeA, flowIdA, tapsA,  qosRetryCount, observer);
    }

    @Test
    public void testQosCreateWithoutObserver()
            throws Exception
    {
        testCreatedEventTapFlow(factory.createQosEventTapFlow(eventTypeB, flowIdB, tapsB),
                eventTypeB, flowIdB, tapsB, qosRetryCount, NULL_OBSERVER);
    }

    private void testCreatedEventTapFlow(EventTapFlow eventTapFlow, String eventType, String flowId, Set<URI> taps, int retryCount, Observer observer)
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
        if (observer == this.observer) {
            verify(observer).onRecordsDelivered(anyInt());
            verifyNoMoreInteractions(observer);
        }

        httpClient.clearRequests();
        httpClient.respondWithException();
        eventTapFlow.processBatch(events);

        requests = httpClient.getRequests();
        assertEquals(requests.size(), taps.size() * (retryCount + 1));

        if (observer == this.observer) {
            verify(observer).onRecordsLost(anyInt());
            verifyNoMoreInteractions(observer);
        }
    }

    private Event createEvent(String eventType)
    {
        return new Event(eventType, randomUUID().toString(), "http://host.com",
                DateTime.now(DateTimeZone.UTC), ImmutableMap.of("foo", "bar"));
    }
}
