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
import com.proofpoint.http.client.BodyGenerator;
import com.proofpoint.http.client.Request;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.reporting.testing.TestingReportCollectionFactory;
import com.proofpoint.stats.SparseCounterStat;
import com.proofpoint.units.Duration;
import org.joda.time.DateTime;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Objects.firstNonNull;
import static com.proofpoint.event.collector.EventCollectorStats.Status.DELIVERED;
import static com.proofpoint.event.collector.EventCollectorStats.Status.DROPPED;
import static com.proofpoint.event.collector.EventCollectorStats.Status.LOST;
import static com.proofpoint.event.collector.EventCollectorStats.Status.REJECTED;
import static java.lang.String.format;
import static java.net.URI.create;
import static java.util.UUID.randomUUID;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestHttpEventTapFlow
{
    private static final JsonCodec<List<Event>> EVENT_LIST_JSON_CODEC = JsonCodec.listJsonCodec(Event.class);
    private static final String X_PROOFPOINT_QOS = "X-Proofpoint-QoS";
    private static final Set<URI> singleTap = ImmutableSet.of(create("http://n1.event.tap/post"));
    private static final Set<URI> multipleTaps = ImmutableSet.of(create("http://n2.event.tap/post"), create("http://n3.event.tap/post"));
    private static final Set<URI> taps = multipleTaps;
    private static final int retryCount = 10;
    private static final String ARBITRARY_EVENT_TYPE = "EventType";
    private static final String ARBITRARY_FLOW_ID = "FlowId";
    private final List<Event> events = ImmutableList.of(
            new Event(ARBITRARY_EVENT_TYPE, randomUUID().toString(), "foo.com", DateTime.now(), ImmutableMap.<String, Object>of()),
            new Event("EventTYpe", randomUUID().toString(), "foo.com", DateTime.now(), ImmutableMap.<String, Object>of()));
    private MockHttpClient httpClient;
    private HttpEventTapFlow singleEventTapFlow;
    private HttpEventTapFlow multipleEventTapFlow;
    private HttpEventTapFlow multipleEventTapFlowWithRetry;
    private HttpEventTapFlow eventTapFlow;              // Tests that don't care if they are single or multiple.
    private EventCollectorStats eventCollectorStats;
    private TestingReportCollectionFactory testingReportCollectionFactory;

    @BeforeMethod
    private void setup()
    {
        httpClient = new MockHttpClient();

        testingReportCollectionFactory = new TestingReportCollectionFactory();
        eventCollectorStats = testingReportCollectionFactory.createReportCollection(EventCollectorStats.class);

        singleEventTapFlow = new HttpEventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, ARBITRARY_EVENT_TYPE, "FlowId", singleTap, 0, null, eventCollectorStats);
        multipleEventTapFlow = new HttpEventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, ARBITRARY_EVENT_TYPE, "FlowId", multipleTaps, 0, null, eventCollectorStats);
        multipleEventTapFlowWithRetry = new HttpEventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, ARBITRARY_EVENT_TYPE, "FlowId", multipleTaps, retryCount, new Duration(1, TimeUnit.MILLISECONDS), eventCollectorStats);
        eventTapFlow = multipleEventTapFlow;
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "httpClient is null")
    public void testConstructorNullHttpClient()
    {
        new HttpEventTapFlow(null, EVENT_LIST_JSON_CODEC, ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, taps, 0, null, eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventsCodec is null")
    public void testConstructorNullEventsCodec()
    {
        new HttpEventTapFlow(httpClient, null, ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, taps, 0, null, eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventType is null")
    public void testConstructorNullEventType()
    {
        new HttpEventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, null, ARBITRARY_FLOW_ID, taps, 0, null, eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "flowId is null")
    public void testConstructorNullFlowId()
    {
        new HttpEventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, ARBITRARY_EVENT_TYPE, null, taps, 0, null, eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "taps is null")
    public void testConstructorNullTaps()
    {
        new HttpEventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, null, 0, null, eventCollectorStats);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "taps is empty")
    public void testConstructorEmptyTaps()
    {
        new HttpEventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, ImmutableSet.<URI>of(), 0, null, eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "retryDelay is null")
    public void testConstructorNullRetryDelay()
    {
        new HttpEventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, ImmutableSet.<URI>of(), 1, null, eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventCollectorStats is null")
    public void testConstructorNullEventCollectorStats()
    {
        new HttpEventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, taps, 0, null, null);
    }

    @Test
    public void testProcessBatch()
            throws Exception
    {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        eventTapFlow.processBatch(events);
        List<Request> requests = httpClient.getRequests();
        assertEquals(requests.size(), 1);

        Request request = requests.get(0);
        BodyGenerator bodyGenerator = request.getBodyGenerator();
        assertTrue(taps.contains(request.getUri()));

        bodyGenerator.write(byteArrayOutputStream);
        assertEquals(byteArrayOutputStream.toString(), EVENT_LIST_JSON_CODEC.toJson(events));

        assertEquals(request.getHeaders().get(CONTENT_TYPE), ImmutableList.of(APPLICATION_JSON));
    }

    @Test
    public void testFirstProcessBatchContainsQosHeader()
            throws Exception
    {
        // The first time a message is sent to a tap, the firstBatch flag is set.
        // Subsequent times, the firstBatch flag is clear. This is true of each of the taps
        // within the same flow.
        Set<URI> remainingFirstTaps = new HashSet<>(multipleTaps);
        Set<URI> remainingSecondTaps = new HashSet<>(multipleTaps);

        for (int i = 0; !remainingSecondTaps.isEmpty(); ++i) {
            assertTrue(i < 10000);
            multipleEventTapFlow.processBatch(events);
            List<Request> requests = httpClient.getRequests();
            assertEquals(requests.size(), i + 1);
            Request request = requests.get(i);
            URI uri = request.getUri();

            assertTrue(multipleTaps.contains(uri));
            if (remainingFirstTaps.remove(uri)) {
                assertQosHeadersFirstBatch(request);
            }
            else if (remainingSecondTaps.remove(uri)) {
                assertNoQosHeaders(request);
            }
        }
    }

    @Test
    public void testProcessDroppedMessagesBatch()
            throws Exception
    {
        singleEventTapFlow.processBatch(events);
        singleEventTapFlow.notifyEntriesDropped(10);
        httpClient.clearRequests();
        singleEventTapFlow.processBatch(events);
        List<Request> requests = httpClient.getRequests();
        assertEquals(requests.size(), 1);
        Request request = requests.get(0);

        assertQosHeadersDroppedEntries(request, 10);
    }

    @Test
    public void testProcessDroppedMessagesFirstBatch()
            throws Exception
    {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        singleEventTapFlow.notifyEntriesDropped(10);
        singleEventTapFlow.processBatch(events);
        List<Request> requests = httpClient.getRequests();
        assertEquals(requests.size(), 1);

        Request request = requests.get(0);
        BodyGenerator bodyGenerator = request.getBodyGenerator();
        assertTrue(singleTap.contains(request.getUri()));

        bodyGenerator.write(byteArrayOutputStream);
        assertEquals(byteArrayOutputStream.toString(), EVENT_LIST_JSON_CODEC.toJson(events));

        assertEquals(request.getHeaders().get(CONTENT_TYPE), ImmutableList.of("application/json"));
        assertQosHeaders(request, true, 10);
    }

    @Test
    public void testNoDroppedMessagesOnSuccess()
            throws Exception
    {
        singleEventTapFlow.processBatch(events);

        // The number of records lost will be reported in the next message.
        httpClient.clearRequests();
        singleEventTapFlow.processBatch(events);
        List<Request> requests = httpClient.getRequests();
        assertEquals(requests.size(), 1);
        Request request = requests.get(0);
        assertNoQosHeaders(request);
    }

    @Test
    public void testNoDroppedMessagesOnRetrySuccess()
    {
        clearFirstBatchHeaders(multipleEventTapFlow, multipleTaps);
        httpClient.respondWithException(new Exception(), multipleTaps.size() - 1);

        multipleEventTapFlow.processBatch(events);
        List<Request> requests = httpClient.getRequests();
        assertEquals(requests.size(), multipleTaps.size());
        assertEquals(extractUris(requests).keySet(), multipleTaps);
        for (Request request : requests) {
            assertNoQosHeaders(request);
        }

        // Since the LAST request was successful, there should be no dropped requests.
        httpClient.clearRequests();
        multipleEventTapFlow.processBatch(events);
        requests = httpClient.getRequests();
        assertEquals(requests.size(), 1);
        assertNoQosHeaders(requests.get(0));
    }

    @Test
    public void testRetries()
    {
        httpClient.respondWithException();

        // The number of requests should be equal to retryCount for each tap destination.
        multipleEventTapFlowWithRetry.processBatch(events);
        List<Request> requests = httpClient.getRequests();
        assertEquals(requests.size(), multipleTaps.size() * (retryCount + 1));
        Map<URI, Integer> uris = extractUris(requests);
        assertEquals(uris.keySet(), multipleTaps);
        for (Map.Entry<URI, Integer> entry : uris.entrySet()) {
            assertEquals(entry.getValue().intValue(), retryCount + 1, format("retry count wrong for URI %s", entry.getKey()));
        }

        // And even though multiple attempts were made, the events should only be counted once.
        httpClient.respondWithOk();
        httpClient.clearRequests();
        multipleEventTapFlowWithRetry.processBatch(events);
        requests = httpClient.getRequests();
        assertEquals(requests.size(), 1);
        assertQosHeaders(requests.get(0), true, events.size());
    }

    @Test
    public void testDroppedMessagesOnFailure()
            throws Exception
    {
        // This these requires at least 2 events, to make sure the HttpEventTapFlow
        // sets dropped messages to the number of events dropped.
        httpClient.respondWithException();
        assertTrue(events.size() > 1);
        singleEventTapFlow.processBatch(events);

        // The records lost will be reported in the next message.
        singleEventTapFlow.processBatch(events);
        List<Request> requests = httpClient.getRequests();
        assertEquals(requests.size(), 2);

        Request request = requests.get(1);
        assertQosHeaders(request, true, events.size());
    }

    @Test
    public void testDroppedMessagesOnServerError()
            throws Exception
    {
        httpClient.respondWithServerError();
        multipleEventTapFlowWithRetry.notifyEntriesDropped(10);
        multipleEventTapFlowWithRetry.processBatch(events);

        // A server error triggers a retry; retryCount retries
        // per tap.
        List<Request> requests = httpClient.getRequests();
        assertEquals(requests.size(), multipleTaps.size() * (retryCount + 1));
        for (Request request : requests) {
            assertQosHeaders(request, true, 10);
        }

        // The records lost will be reported in the next message, because they
        // are considers to *NOT* have been successfully reported in the rejected message.
        httpClient.respondWithOk();
        httpClient.clearRequests();
        multipleEventTapFlowWithRetry.processBatch(events);
        requests = httpClient.getRequests();
        assertEquals(requests.size(), 1);
        Request request = requests.get(0);
        assertQosHeaders(request, true, 10 + events.size());
    }

    @Test
    public void testDroppedMessagesOnClientError()
            throws Exception
    {
        httpClient.respondWithClientError();
        multipleEventTapFlowWithRetry.notifyEntriesDropped(10);
        multipleEventTapFlowWithRetry.processBatch(events);

        // A client error does not trigger a retry, the first such error
        // will cause the requests to be immediately dropped.
        List<Request> requests = httpClient.getRequests();
        assertEquals(requests.size(), 1);
        Request request = requests.get(0);
        assertQosHeaders(request, true, 10);

        // The records lost will be reported in the next message, because they
        // are considers to *NOT* have been successfully reported in the rejected message.
        httpClient.respondWithOk();
        httpClient.clearRequests();
        multipleEventTapFlowWithRetry.processBatch(events);
        requests = httpClient.getRequests();
        assertEquals(requests.size(), 1);
        request = requests.get(0);
        assertQosHeaders(request, true, 10 + events.size());
    }

    @Test
    public void testGetTaps()
    {
        assertEqualsNoOrder(singleEventTapFlow.getTaps().toArray(), singleTap.toArray());
        assertEqualsNoOrder(multipleEventTapFlow.getTaps().toArray(), multipleTaps.toArray());
    }

    @Test
    public void testSetTaps()
    {
        Set<URI> taps = ImmutableSet.of(
                create("http://n1.event.tap/post"),
                create("http://n2.event.tap/post"),
                create("http://n3.event.tap/post"));

        // Make sure we are actually changing the taps
        assertNotEquals(taps, eventTapFlow.getTaps());

        eventTapFlow.setTaps(taps);
        assertEquals(eventTapFlow.getTaps(), taps);
    }

    @Test
    public void testMetricsOnSuccessRecordsDeliveredEvents()
            throws Exception
    {
        eventTapFlow.processBatch(events);

        ArgumentCaptor<URI> uriArgumentCaptor = ArgumentCaptor.forClass(URI.class);
        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        verify(argumentVerifier).outboundEvents(eq(ARBITRARY_EVENT_TYPE), eq(ARBITRARY_FLOW_ID), uriArgumentCaptor.capture(), eq(DELIVERED));
        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        SparseCounterStat counterStat = reportCollection.outboundEvents(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, uriArgumentCaptor.getValue(), DELIVERED);
        verify(counterStat).add(events.size());
        verifyNoMoreInteractions(counterStat);
    }

    @Test
    public void testMetricsOnExceptionRecordsLostEvents()
            throws Exception
    {
        httpClient.respondWithException();
        eventTapFlow.processBatch(events);

        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        verify(argumentVerifier).outboundEvents(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, LOST);
        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        SparseCounterStat counterStat = reportCollection.outboundEvents(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, LOST);
        verify(counterStat).add(events.size());
        verifyNoMoreInteractions(counterStat);
    }

    @Test
    public void testMetricsOnServerErrorRecordsLostEvents()
            throws Exception
    {
        httpClient.respondWithServerError();
        eventTapFlow.processBatch(events);

        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        verify(argumentVerifier).outboundEvents(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, LOST);
        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        SparseCounterStat counterStat = reportCollection.outboundEvents(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, LOST);
        verify(counterStat).add(events.size());
        verifyNoMoreInteractions(counterStat);
    }

    @Test
    public void testMetricsOnClientErrorRecordsRejectedEvents()
    {
        httpClient.respondWithClientError();
        eventTapFlow.processBatch(events);

        ArgumentCaptor<URI> uriArgumentCaptor = ArgumentCaptor.forClass(URI.class);
        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        verify(argumentVerifier).outboundEvents(eq(ARBITRARY_EVENT_TYPE), eq(ARBITRARY_FLOW_ID), uriArgumentCaptor.capture(), eq(REJECTED));
        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        SparseCounterStat counterStat = reportCollection.outboundEvents(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, uriArgumentCaptor.getValue(), REJECTED);
        verify(counterStat).add(events.size());
        verifyNoMoreInteractions(counterStat);
    }

    @Test
    public void testMetricsOnQueueOverflowRecordsDroppedEvents()
    {
        multipleEventTapFlowWithRetry.notifyEntriesDropped(10);

        EventCollectorStats argumentVerifier = testingReportCollectionFactory.getArgumentVerifier(EventCollectorStats.class);
        verify(argumentVerifier).outboundEvents(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, DROPPED);
        verifyNoMoreInteractions(argumentVerifier);

        EventCollectorStats reportCollection = testingReportCollectionFactory.getReportCollection(EventCollectorStats.class);
        SparseCounterStat counterStat = reportCollection.outboundEvents(ARBITRARY_EVENT_TYPE, ARBITRARY_FLOW_ID, DROPPED);
        verify(counterStat).add(10);
        verifyNoMoreInteractions(counterStat);
    }

    private void clearFirstBatchHeaders(HttpEventTapFlow eventTapFlow, Set<URI> taps)
    {
        // Keep sending events until each tap receives a message.
        Set<URI> remainingTaps = new HashSet<>(taps);
        httpClient.clearRequests();
        for (int i = 0; !remainingTaps.isEmpty(); ++i) {
            assertTrue(i < 10000);
            eventTapFlow.processBatch(events);
            List<Request> requests = httpClient.getRequests();
            httpClient.clearRequests();
            for (Request request : requests) {
                remainingTaps.remove(request.getUri());
            }
        }
    }

    private static Map<URI, Integer> extractUris(Collection<Request> requests)
    {
        Map<URI, Integer> results = new HashMap<>();
        for (Request request : requests) {
            URI uri = request.getUri();
            results.put(uri, firstNonNull(results.get(uri), Integer.valueOf(0)) + 1);
        }
        return ImmutableMap.copyOf(results);
    }

    private void assertNoQosHeaders(Request request)
    {
        assertQosHeaders(request, false, -1);
    }

    private void assertQosHeadersFirstBatch(Request request)
    {
        assertQosHeaders(request, true, -1);
    }

    private void assertQosHeadersDroppedEntries(Request request, int droppedEntries)
    {
        assertTrue(droppedEntries >= 0);
        assertQosHeaders(request, false, droppedEntries);
    }

    private void assertQosHeaders(Request request, boolean firstBatch, int droppedEntries)
    {
        ImmutableList.Builder<String> headerBuilder = ImmutableList.builder();
        if (firstBatch) {
            headerBuilder.add("firstBatch");
        }
        if (droppedEntries >= 0) {
            headerBuilder.add(format("droppedMessages=%d", droppedEntries));
        }

        assertEqualsNoOrder(request.getHeaders().get(X_PROOFPOINT_QOS).toArray(), headerBuilder.build().toArray());
    }
}