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

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.List;
import java.util.Set;

import static java.net.URI.create;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertTrue;

public class TestEventTapFlow
{
    private static final JsonCodec<List<Event>> EVENT_LIST_JSON_CODEC = JsonCodec.listJsonCodec(Event.class);
    private static final Set<URI> taps = ImmutableSet.of(create("http://n1.event.tap/post"), create("http://n2.event.tap/post"));
    private final List<Event> events = ImmutableList.of(new Event("EventType", "UUID", "foo.com", DateTime.now(), ImmutableMap.<String, Object>of()));
    private HttpClient httpClient;
    private Observer observer;
    private EventTapFlow eventTapFlow;

    @BeforeMethod
    private void setup()
    {
        httpClient = mock(HttpClient.class);
        observer = mock(Observer.class);
        eventTapFlow = new EventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, "EventType", "FlowID", taps, observer);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "httpClient is null")
    public void testConstructorNullHttpClient()
    {
        new EventTapFlow(null, EVENT_LIST_JSON_CODEC, "EventType", "FlowID", taps, observer);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventsCodec is null")
    public void testConstructorNullEventsCodec()
    {
        new EventTapFlow(httpClient, null, "EventType", "FlowID", taps, observer);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventType is null")
    public void testConstructorNullEventType()
    {
        new EventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, null, "FlowID", taps, observer);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "flowId is null")
    public void testConstructorNullFlowId()
    {
        new EventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, "EventType", null, taps, observer);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "taps is null")
    public void testConstructorNullTaps()
    {
        new EventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, "EventType", "FlowID", null, observer);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "taps is empty")
    public void testCustructorEmptyTaps()
    {
        new EventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, "EventType", "FlowID", ImmutableSet.<URI>of(), observer);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "observer is null")
    public void testConstructorNullObserver()
    {
        new EventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, "EventType", "FlowID", taps, null);
    }

    @Test
    public void testProcessBatch()
            throws Exception
    {
        ArgumentCaptor<Request> requestArgumentCaptor = ArgumentCaptor.forClass(Request.class);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();

        eventTapFlow.processBatch(events);
        verify(httpClient, times(1)).execute(requestArgumentCaptor.capture(), any(ResponseHandler.class));

        Request request = requestArgumentCaptor.getValue();
        BodyGenerator bodyGenerator = request.getBodyGenerator();
        assertTrue(taps.contains(request.getUri()));

        bodyGenerator.write(byteArrayOutputStream);
        assertEquals(byteArrayOutputStream.toString(), EVENT_LIST_JSON_CODEC.toJson(events));

        assertEquals(request.getHeaders().get("Content-Type"), ImmutableList.<String>of("application/json"));
    }

    @Test
    public void testGetTaps()
    {
        EventTapFlow flow = new EventTapFlow(httpClient, EVENT_LIST_JSON_CODEC, "EventType", "FlowId", taps, observer);
        assertEqualsNoOrder(flow.getTaps().toArray(), taps.toArray());
    }

    @Test
    public void testObserverOnSuccess()
            throws Exception
    {
        ArgumentCaptor<Request> requestArgumentCaptor = ArgumentCaptor.forClass(Request.class);
        ArgumentCaptor<ResponseHandler> responseHandlerArgumentCaptor = ArgumentCaptor.forClass(ResponseHandler.class);
        ArgumentCaptor<URI> uriArgumentCaptor = ArgumentCaptor.forClass(URI.class);

        // Process the events in order to get the EventTapFlow to provide us
        // with a request and response handler that we can use to feed
        // it responses.
        eventTapFlow.processBatch(events);
        verify(httpClient, times(1)).execute(requestArgumentCaptor.capture(), responseHandlerArgumentCaptor.capture());
        ResponseHandler<Void, Exception> responseHandler = responseHandlerArgumentCaptor.getValue();
        Request request = requestArgumentCaptor.getValue();

        Response response = mock(Response.class);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getStatusMessage()).thenReturn("OK");
        responseHandler.handle(request, response);
        verify(observer, times(1)).onRecordsSent(uriArgumentCaptor.capture(), eq(events.size()));
        assertTrue(taps.contains(uriArgumentCaptor.getValue()));
        verifyNoMoreInteractions(observer);
    }

    @Test
    public void testObserverOnFailure()
            throws Exception
    {
        ArgumentCaptor<Request> requestArgumentCaptor = ArgumentCaptor.forClass(Request.class);
        ArgumentCaptor<ResponseHandler> responseHandlerArgumentCaptor = ArgumentCaptor.forClass(ResponseHandler.class);
        ArgumentCaptor<URI> uriArgumentCaptor = ArgumentCaptor.forClass(URI.class);

        // Process the events in order to get the EventTapFlow to provide us
        // with the a request and response handler that we can use to feed
        // it responses.
        eventTapFlow.processBatch(events);
        verify(httpClient, times(1)).execute(requestArgumentCaptor.capture(), responseHandlerArgumentCaptor.capture());
        Request request = requestArgumentCaptor.getValue();
        ResponseHandler<Void, Exception> responseHandler = responseHandlerArgumentCaptor.getValue();

        responseHandler.handleException(request, new Exception());
        verify(observer, times(1)).onRecordsLost(any(URI.class), eq(events.size()));
        verifyNoMoreInteractions(observer);
    }

    @Test
    public void testObserverOnNon200Error()
            throws Exception
    {
        ArgumentCaptor<Request> requestArgumentCaptor = ArgumentCaptor.forClass(Request.class);
        ArgumentCaptor<ResponseHandler> responseHandlerArgumentCaptor = ArgumentCaptor.forClass(ResponseHandler.class);
        ArgumentCaptor<URI> uriArgumentCaptor = ArgumentCaptor.forClass(URI.class);

        // Process the events in order to get the EventTapFlow to provide us
        // with the a request and response handler that we can use to feed
        // it responses.
        eventTapFlow.processBatch(events);
        verify(httpClient, times(1)).execute(requestArgumentCaptor.capture(), responseHandlerArgumentCaptor.capture());
        Request request = requestArgumentCaptor.getValue();
        ResponseHandler<Void, Exception> responseHandler = responseHandlerArgumentCaptor.getValue();

        Response response = mock(Response.class);
        when(response.getStatusCode()).thenReturn(500);
        when(response.getStatusMessage()).thenReturn("Server Error");
        responseHandler.handle(request, response);
        verify(observer, times(1)).onRecordsLost(uriArgumentCaptor.capture(), eq(events.size()));
        assertTrue(taps.contains(uriArgumentCaptor.getValue()));
        verifyNoMoreInteractions(observer);
    }
}
