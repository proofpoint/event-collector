/*
 * Copyright 2011-2012 Proofpoint, Inc.
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
import com.proofpoint.http.client.BodyGenerator;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.Request;
import com.proofpoint.http.client.Response;
import com.proofpoint.http.client.ResponseHandler;
import com.proofpoint.json.JsonCodec;
import org.joda.time.DateTime;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static java.lang.String.format;
import static java.net.URI.create;
import static java.util.UUID.randomUUID;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
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
    private final List<Event> events = ImmutableList.of(
            new Event("EventType", randomUUID().toString(), "foo.com", DateTime.now(), ImmutableMap.<String, Object>of()),
            new Event("EventTYpe", randomUUID().toString(), "foo.com", DateTime.now(), ImmutableMap.<String, Object>of()));
    private HttpClient httpClient;
    private Observer observer;
    private HttpEventTapFlow singleEventTapFlow;
    private HttpEventTapFlow multipleEventTapFlow;
    private HttpEventTapFlow eventTapFlow;              // Tests that don't care if they are single or multiple.

    @BeforeMethod
    private void setup()
    {
        httpClient = mock(HttpClient.class);
        observer = mock(Observer.class);
        singleEventTapFlow = new HttpEventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, "EventType", "FlowID", singleTap, observer);
        multipleEventTapFlow = new HttpEventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, "EventTYpe", "FlowID", multipleTaps, observer);
        eventTapFlow = multipleEventTapFlow;
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "httpClient is null")
    public void testConstructorNullHttpClient()
    {
        new HttpEventTapFlow(null, EVENT_LIST_JSON_CODEC, "EventType", "FlowID", taps, observer);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventsCodec is null")
    public void testConstructorNullEventsCodec()
    {
        new HttpEventTapFlow(httpClient, null, "EventType", "FlowID", taps, observer);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventType is null")
    public void testConstructorNullEventType()
    {
        new HttpEventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, null, "FlowID", taps, observer);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "flowId is null")
    public void testConstructorNullFlowId()
    {
        new HttpEventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, "EventType", null, taps, observer);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "taps is null")
    public void testConstructorNullTaps()
    {
        new HttpEventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, "EventType", "FlowID", null, observer);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "taps is empty")
    public void testCustructorEmptyTaps()
    {
        new HttpEventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, "EventType", "FlowID", ImmutableSet.<URI>of(), observer);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "observer is null")
    public void testConstructorNullObserver()
    {
        new HttpEventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, "EventType", "FlowID", taps, null);
    }

    @Test
    public void testProcessBatch()
            throws Exception
    {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        eventTapFlow.processBatch(events);
        List<Request> requests = captureRequests();
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
        Set<URI> remainingFirstTaps = new HashSet<URI>(multipleTaps);
        Set<URI> remainingSecondTaps = new HashSet<URI>(multipleTaps);

        for (int i = 0; !remainingSecondTaps.isEmpty(); ++i) {
            assertTrue(i < 10000);
            multipleEventTapFlow.processBatch(events);
            List<Request> requests = captureRequests();
            assertEquals(requests.size(), i + 1);
            Request request = requests.get(i);
            URI uri = request.getUri();

            assertTrue(multipleTaps.contains(uri));
            if (remainingFirstTaps.remove(uri)) {
                assertEquals(request.getHeaders().get(X_PROOFPOINT_QOS), ImmutableList.of("firstBatch"));
            }
            else if (remainingSecondTaps.remove(uri)) {
                assertEquals(request.getHeaders().get(X_PROOFPOINT_QOS), ImmutableList.<String>of());
            }
        }
    }

    @Test
    public void testProcessDroppedMessagesBatch()
            throws Exception
    {
        singleEventTapFlow.processBatch(events);
        singleEventTapFlow.notifyEntriesDropped(10);
        singleEventTapFlow.processBatch(events);
        List<Request> requests = captureRequests();
        assertEquals(requests.size(), 2);
        Request request = requests.get(1);

        assertEquals(request.getHeaders().get(X_PROOFPOINT_QOS), ImmutableList.of("droppedMessages=10"));
    }

    @Test
    public void testProcessDroppedMessagesFirstBatch()
            throws Exception
    {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        singleEventTapFlow.notifyEntriesDropped(10);
        singleEventTapFlow.processBatch(events);
        List<Request> requests = captureRequests();
        assertEquals(requests.size(), 1);

        Request request = requests.get(0);
        BodyGenerator bodyGenerator = request.getBodyGenerator();
        assertTrue(singleTap.contains(request.getUri()));

        bodyGenerator.write(byteArrayOutputStream);
        assertEquals(byteArrayOutputStream.toString(), EVENT_LIST_JSON_CODEC.toJson(events));

        assertEquals(request.getHeaders().get(CONTENT_TYPE), ImmutableList.of("application/json"));
        assertEqualsNoOrder(request.getHeaders().get(X_PROOFPOINT_QOS).toArray(), new String[]{"firstBatch", "droppedMessages=10"});
    }

    @Test
    public void testNoDroppedMessagesOnSuccess()
            throws Exception
    {
        singleEventTapFlow.processBatch(events);
        respondWithOK();

        // The no records lost will be reported in the next message.
        singleEventTapFlow.processBatch(events);
        List<Request> requests = captureRequests();
        assertEquals(requests.size(), 2);

        Request request = requests.get(1);
        assertEqualsNoOrder(request.getHeaders().get(X_PROOFPOINT_QOS).toArray(), new String[]{});
    }

    @Test
    public void testDroppedMessagesOnFailure()
            throws Exception
    {
        // This these requires at least 2 events, to make sure the HttpEventTapFlow
        // sets dropped messages to the number of events dropped.
        assertTrue(events.size() > 1);
        singleEventTapFlow.processBatch(events);
        respondWithException();

        // The records lost will be reported in the next message.
        singleEventTapFlow.processBatch(events);
        List<Request> requests = captureRequests();
        assertEquals(requests.size(), 2);

        Request request = requests.get(1);
        assertEqualsNoOrder(request.getHeaders().get(X_PROOFPOINT_QOS).toArray(), new String[]{format("droppedMessages=%d", events.size())});
    }

    @Test
    public void testNoDroppedMessagesOnNon200Error()
            throws Exception
    {
        singleEventTapFlow.notifyEntriesDropped(10);
        singleEventTapFlow.processBatch(events);
        respondWithError();
        List<Request> requests = captureRequests();
        assertEquals(requests.size(), 1);
        Request request = requests.get(0);
        assertEqualsNoOrder(request.getHeaders().get(X_PROOFPOINT_QOS).toArray(), new String[]{"droppedMessages=10", "firstBatch"});

        // The records lost will be reported in the next message, because they
        // are considers to *NOT* have been successfully reported in the rejected message.
        singleEventTapFlow.processBatch(events);
        requests = captureRequests();
        assertEquals(requests.size(), 2);

        request = requests.get(1);
        assertEqualsNoOrder(request.getHeaders().get(X_PROOFPOINT_QOS).toArray(), new String[]{format("droppedMessages=%d", 10 + events.size())});
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
    public void testObserverOnSuccess()
            throws Exception
    {
        eventTapFlow.processBatch(events);
        respondWithOK();

        ArgumentCaptor<URI> uriArgumentCaptor = ArgumentCaptor.forClass(URI.class);
        verify(observer, times(1)).onRecordsSent(uriArgumentCaptor.capture(), eq(events.size()));
        assertTrue(taps.contains(uriArgumentCaptor.getValue()));
        verifyNoMoreInteractions(observer);
    }

    @Test
    public void testObserverOnFailure()
            throws Exception
    {
        eventTapFlow.processBatch(events);
        respondWithException();

        ArgumentCaptor<URI> uriArgumentCaptor = ArgumentCaptor.forClass(URI.class);
        verify(observer, times(1)).onRecordsLost(any(URI.class), eq(events.size()));
        verifyNoMoreInteractions(observer);
    }

    @Test
    public void testObserverOnNon200Error()
            throws Exception
    {
        eventTapFlow.processBatch(events);
        respondWithError();

        ArgumentCaptor<URI> uriArgumentCaptor = ArgumentCaptor.forClass(URI.class);
        verify(observer, times(1)).onRecordsLost(uriArgumentCaptor.capture(), eq(events.size()));
        assertTrue(taps.contains(uriArgumentCaptor.getValue()));
        verifyNoMoreInteractions(observer);
    }

    private void respondWithOK()
            throws Exception
    {
        Response response = mock(Response.class);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getStatusMessage()).thenReturn("OK");
        respondWith(response);
    }

    private void respondWithError()
            throws Exception
    {
        Response response = mock(Response.class);
        when(response.getStatusCode()).thenReturn(500);
        when(response.getStatusMessage()).thenReturn("Server Error");
        respondWith(response);
    }

    private void respondWith(Response response)
            throws Exception
    {
        // Process the events in order to get the EventTapFlow to provide us
        // with a request and response handler that we can use to feed
        // it responses.
        ArgumentCaptor<Request> requestArgumentCaptor = ArgumentCaptor.forClass(Request.class);
        ArgumentCaptor<ResponseHandler> responseHandlerArgumentCaptor = ArgumentCaptor.forClass(ResponseHandler.class);
        verify(httpClient, atLeastOnce()).execute(requestArgumentCaptor.capture(), responseHandlerArgumentCaptor.capture());
        ResponseHandler<Void, Exception> responseHandler = responseHandlerArgumentCaptor.getValue();
        Request request = requestArgumentCaptor.getValue();
        responseHandler.handle(request, response);
    }

    private void respondWithException()
            throws Exception
    {
        ArgumentCaptor<Request> requestArgumentCaptor = ArgumentCaptor.forClass(Request.class);
        ArgumentCaptor<ResponseHandler> responseHandlerArgumentCaptor = ArgumentCaptor.forClass(ResponseHandler.class);
        verify(httpClient, atLeastOnce()).execute(requestArgumentCaptor.capture(), responseHandlerArgumentCaptor.capture());

        Request request = requestArgumentCaptor.getValue();
        ResponseHandler<Void, Exception> responseHandler = responseHandlerArgumentCaptor.getValue();
        responseHandler.handleException(request, new Exception());
    }

    private List<Request> captureRequests()
            throws Exception
    {
        ArgumentCaptor<Request> requestArgumentCaptor = ArgumentCaptor.forClass(Request.class);
        verify(httpClient, atLeast(0)).execute(requestArgumentCaptor.capture(), any(ResponseHandler.class));
        return ImmutableList.copyOf(requestArgumentCaptor.getAllValues());
    }
}
