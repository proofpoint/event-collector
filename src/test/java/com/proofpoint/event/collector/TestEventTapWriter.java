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
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceState;
import com.proofpoint.discovery.client.testing.StaticServiceSelector;
import com.proofpoint.http.client.BodyGenerator;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.Request;
import com.proofpoint.http.client.ResponseHandler;
import com.proofpoint.json.JsonCodec;
import org.joda.time.DateTime;
import org.logicalshift.concurrent.SerialScheduledExecutorService;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.UUID.randomUUID;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestEventTapWriter
{
    private static final JsonCodec<List<Event>> EVENT_LIST_JSON_CODEC = JsonCodec.listJsonCodec(Event.class);
    private ServiceSelector serviceSelector;
    private HttpClient httpClient;
    private ScheduledExecutorService executorService;

    @BeforeMethod
    public void setup()
    {
        serviceSelector = new StaticServiceSelector(ImmutableSet.<ServiceDescriptor>of());
        httpClient = mock(HttpClient.class);
        executorService = new SerialScheduledExecutorService();
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "selector is null")
    public void testConstructorNullSelector()
    {
        new EventTapWriter(null, httpClient, EVENT_LIST_JSON_CODEC, executorService,
                new BatchProcessorConfig(), new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "httpClient is null")
    public void testConstructorNullHttpClient()
    {
        new EventTapWriter(serviceSelector, null, EVENT_LIST_JSON_CODEC, executorService,
                new BatchProcessorConfig(), new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventsCodec is null")
    public void testConstructorNullEventCodec()
    {
        new EventTapWriter(serviceSelector, httpClient, null, executorService,
                new BatchProcessorConfig(), new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "executorService is null")
    public void testConstructorNullExecutorService()
    {
        new EventTapWriter(serviceSelector, httpClient, EVENT_LIST_JSON_CODEC, null,
                new BatchProcessorConfig(), new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "batchProcessorConfig is null")
    public void testConstructorNullBatchProcessorConfig()
    {
        new EventTapWriter(serviceSelector, httpClient, EVENT_LIST_JSON_CODEC, executorService,
                null, new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "config is null")
    public void testConstructorNullConfig()
    {
        new EventTapWriter(serviceSelector, httpClient, EVENT_LIST_JSON_CODEC, executorService,
                new BatchProcessorConfig(), null);
    }

    @Test
    public void testWrite()
            throws Exception
    {
        int maxEventsPerType = 10;
        int numEventTypes = 10;

        for (int i = 1; i <= maxEventsPerType; ++i) {
            reset(httpClient);
            testWrite(numEventTypes, maxEventsPerType, i);
        }
    }

    private void testWrite(int numEventTypes, int eventCountPerType, int batchSize)
            throws Exception
    {
        ArgumentCaptor<Request> requestArgumentCaptor = ArgumentCaptor.forClass(Request.class);
        Map<String, List<Event>> events = new HashMap<String, List<Event>>();

        ImmutableList.Builder<ServiceDescriptor> serviceDescriptorBuilder = ImmutableList.<ServiceDescriptor>builder();
        for (int typeIndex = 0; typeIndex < numEventTypes; ++typeIndex) {
            String type = String.format("Type%d", typeIndex);
            List<Event> typeEvents = new ArrayList<Event>();
            for (int i_event = 0; i_event < eventCountPerType; ++i_event) {
                typeEvents.add(createEvent(type));
            }
            events.put(type, typeEvents);

            String nodeId = randomUUID().toString();
            ServiceDescriptor descriptor = new ServiceDescriptor(
                    randomUUID(),
                    nodeId,
                    "EventTap",
                    "global",
                    "/" + nodeId,
                    ServiceState.RUNNING,
                    ImmutableMap.<String, String>of("eventType", type, "tapId", "1", "http", String.format("http://%s.event.tap", type))
            );
            serviceDescriptorBuilder.add(descriptor);
        }

        EventTapWriter writer = new EventTapWriter(
                new StaticServiceSelector(serviceDescriptorBuilder.build()),
                httpClient,
                EVENT_LIST_JSON_CODEC,
                executorService,
                new BatchProcessorConfig().setMaxBatchSize(batchSize),
                new EventTapConfig());
        writer.refreshFlows();

        for (List<Event> eventsOfType : events.values()) {
            for (Event event : eventsOfType) {
                writer.write(event);
            }
        }

        // UGH!!! BatchProcessor creates it's own thread to process stuff.
        // For now, just sleep to give the thread enough time to handle
        // the events.
        Thread.sleep(1000);

        verify(httpClient, atLeastOnce()).execute(requestArgumentCaptor.capture(), any(ResponseHandler.class));

        for (Request request : requestArgumentCaptor.getAllValues()) {
            List<Event> receivedEvents = getBody(request);
            String type = request.getUri().getHost().split("\\.")[0];
            List<Event> remainingEvents = events.get(type);
            String context = String.format("size=%d type=%s countPerType=%d batchSize=%d remaining=%d",
                    receivedEvents.size(), type, eventCountPerType, batchSize, remainingEvents.size());

            int size = receivedEvents.size();
            assertTrue(size <= batchSize, context);

            for (int i = 0; i < size; ++i) {
                checkEquals(receivedEvents.get(i), remainingEvents.get(i));
            }

            for (int i = 0; i < size; ++i) {
                remainingEvents.remove(0);
            }

        }
    }

    private void checkEquals(Event actual, Event expected)
    {
        assertEquals(actual.getUuid(), actual.getUuid());
        assertEquals(actual.getType(), expected.getType());
        assertEquals(actual.getHost(), expected.getHost());
    }

    private Event createEvent(String type)
    {
        return new Event(type, randomUUID().toString(), "host", DateTime.now(), ImmutableMap.<String, Object>of());
    }

    private List<Event> getBody(Request request)
            throws Exception
    {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        BodyGenerator bodyGenerator = request.getBodyGenerator();
        bodyGenerator.write(byteArrayOutputStream);
        return EVENT_LIST_JSON_CODEC.fromJson(byteArrayOutputStream.toString());
    }
}
