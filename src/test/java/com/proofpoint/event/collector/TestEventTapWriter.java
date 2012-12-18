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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceState;
import com.proofpoint.discovery.client.testing.StaticServiceSelector;
import com.proofpoint.event.collector.BatchProcessor.BatchHandler;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.json.JsonCodec;
import org.joda.time.DateTime;
import org.logicalshift.concurrent.SerialScheduledExecutorService;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestEventTapWriter
{
    private static final JsonCodec<List<Event>> EVENT_LIST_JSON_CODEC = JsonCodec.listJsonCodec(Event.class);
    private ServiceSelector serviceSelector;
    private HttpClient httpClient;
    private SerialScheduledExecutorService executorService;
    private Multimap<String, BatchProcessor<Event>> batchProcessors;

    @BeforeMethod
    public void setup()
    {
        serviceSelector = new StaticServiceSelector(ImmutableSet.<ServiceDescriptor>of());
        httpClient = mock(HttpClient.class);
        executorService = new SerialScheduledExecutorService();
        batchProcessors = HashMultimap.create();
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "selector is null")
    public void testConstructorNullSelector()
    {
        new EventTapWriter(null, httpClient, EVENT_LIST_JSON_CODEC, executorService,
                createBatchProcessorFactory(), new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "httpClient is null")
    public void testConstructorNullHttpClient()
    {
        new EventTapWriter(serviceSelector, null, EVENT_LIST_JSON_CODEC, executorService,
                createBatchProcessorFactory(), new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventsCodec is null")
    public void testConstructorNullEventCodec()
    {
        new EventTapWriter(serviceSelector, httpClient, null, executorService,
                createBatchProcessorFactory(), new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "executorService is null")
    public void testConstructorNullExecutorService()
    {
        new EventTapWriter(serviceSelector, httpClient, EVENT_LIST_JSON_CODEC, null,
                createBatchProcessorFactory(), new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "batchProcessorFactory is null")
    public void testConstructorNullBatchProcessorFactory()
    {
        new EventTapWriter(serviceSelector, httpClient, EVENT_LIST_JSON_CODEC, executorService,
                null, new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "config is null")
    public void testConstructorNullConfig()
    {
        new EventTapWriter(serviceSelector, httpClient, EVENT_LIST_JSON_CODEC, executorService,
                createBatchProcessorFactory(), null);
    }

    @Test
    public void testRefreshFlowsCreatesNewEntries()
    {
        EventTapConfig eventTapConfig = new EventTapConfig();
        BatchProcessorFactory batchProcessorFactory = createBatchProcessorFactory();
        String type1 = "Type1";
        String type2 = "Type2";
        ServiceDescriptor tap1 = createServiceDescriptor(type1);
        ServiceDescriptor tap2 = createServiceDescriptor(type2);
        ServiceSelector serviceSelector = mock(ServiceSelector.class);
        EventTapWriter eventTapWriter = new EventTapWriter(
                serviceSelector, httpClient, EVENT_LIST_JSON_CODEC, executorService,
                batchProcessorFactory, eventTapConfig);

        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.of(tap1));
        eventTapWriter.refreshFlows();
        checkActiveProcessors(type1);

        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.of(tap1, tap2));
        eventTapWriter.refreshFlows();
        checkActiveProcessors(type1, type2);
    }

    @Test
    public void testRefreshFlowsRemovesOldEntries()
    {
        EventTapConfig eventTapConfig = new EventTapConfig();
        BatchProcessorFactory batchProcessorFactory = createBatchProcessorFactory();
        String type1 = "Type1";
        String type2 = "Type2";
        ServiceDescriptor tap1 = createServiceDescriptor(type1);
        ServiceDescriptor tap2 = createServiceDescriptor(type2);
        ServiceSelector serviceSelector = mock(ServiceSelector.class);
        EventTapWriter eventTapWriter = new EventTapWriter(
                serviceSelector, httpClient, EVENT_LIST_JSON_CODEC, executorService,
                batchProcessorFactory, eventTapConfig);

        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.of(tap1, tap2));
        eventTapWriter.refreshFlows();
        checkActiveProcessors(type1, type2);

        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.of(tap1));
        eventTapWriter.refreshFlows();
        checkActiveProcessors(type1);
    }

    @Test
    public void testRefreshFlowsUpdatesExistingProcessor()
    {
        // If the entries for a given tap changes, don't create the processor
        EventTapConfig eventTapConfig = new EventTapConfig();
        BatchProcessorFactory batchProcessorFactory = createBatchProcessorFactory();
        String type1 = "Type1";
        ServiceDescriptor tap1 = createServiceDescriptor(type1);
        ServiceDescriptor tap2 = createServiceDescriptor(type1);
        ServiceSelector serviceSelector = mock(ServiceSelector.class);
        EventTapWriter eventTapWriter = new EventTapWriter(
                serviceSelector, httpClient, EVENT_LIST_JSON_CODEC, executorService,
                batchProcessorFactory, eventTapConfig);

        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.of(tap1));
        eventTapWriter.refreshFlows();
        checkActiveProcessors(type1);

        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.of(tap2));
        eventTapWriter.refreshFlows();
        checkActiveProcessors(type1);
    }

    @Test
    public void testRefreshFlowsIsCalledPeriodically()
    {
        EventTapConfig eventTapConfig = new EventTapConfig();
        BatchProcessorFactory batchProcessorFactory = createBatchProcessorFactory();
        String type1 = "Type1";
        String type2 = "Type2";
        String type3 = "Type3";
        ServiceDescriptor tap1 = createServiceDescriptor(type1);
        ServiceDescriptor tap2 = createServiceDescriptor(type2);
        ServiceDescriptor tap3 = createServiceDescriptor(type3);
        ServiceSelector serviceSelector = mock(ServiceSelector.class);
        EventTapWriter eventTapWriter = new EventTapWriter(
                serviceSelector, httpClient, EVENT_LIST_JSON_CODEC, executorService,
                batchProcessorFactory, eventTapConfig);

        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.of(tap1));

        // When refreshFlows() is it must create new processors using the factory.
        assertTrue(batchProcessors.isEmpty());
        eventTapWriter.start();

        assertTrue(batchProcessors.containsKey(type1));
        assertEquals(batchProcessors.get(type1).size(), 1);

        // If the refreshFlows() is called after the period, tap2 should
        // created to handle the new tap after one period.
        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.of(tap2));
        executorService.elapseTime(
                (long) eventTapConfig.getEventTapRefreshDuration().toMillis() - 1,
                TimeUnit.MILLISECONDS);
        assertFalse(batchProcessors.containsKey(type2));
        executorService.elapseTime(1, TimeUnit.MILLISECONDS);
        assertTrue(batchProcessors.containsKey(type2));
        assertEquals(batchProcessors.get(type2).size(), 1);

        // Same is true after the second period, but with tap3.
        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.of(tap3));
        executorService.elapseTime(
                (long) eventTapConfig.getEventTapRefreshDuration().toMillis() - 1,
                TimeUnit.MILLISECONDS);
        assertFalse(batchProcessors.containsKey(type3));
        executorService.elapseTime(1, TimeUnit.MILLISECONDS);
        assertTrue(batchProcessors.containsKey(type3));
        assertEquals(batchProcessors.get(type3).size(), 1);
    }

    @Test
    public void testWritePartitionsByType()
    {
        EventTapConfig eventTapConfig = new EventTapConfig();
        BatchProcessorFactory batchProcessorFactory = createBatchProcessorFactory();
        String type1 = "Type1";
        String type2 = "Type2";
        String type3 = "Type3";
        ServiceDescriptor tap1 = createServiceDescriptor(type1);
        ServiceDescriptor tap2 = createServiceDescriptor(type2);
        ServiceSelector serviceSelector = mock(ServiceSelector.class);
        EventTapWriter eventTapWriter = new EventTapWriter(
                serviceSelector, httpClient, EVENT_LIST_JSON_CODEC, executorService,
                batchProcessorFactory, eventTapConfig);

        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.of(tap1, tap2));
        eventTapWriter.start();

        Event eventType1 = createEvent(type1);
        Event eventType2 = createEvent(type2);
        Event eventType3 = createEvent(type3);

        eventTapWriter.write(eventType1);
        eventTapWriter.write(eventType2);
        eventTapWriter.write(eventType3);

        checkEventsReceived(type1, eventType1);
        checkEventsReceived(type2, eventType2);
    }

    @Test
    public void testWriteSendsToNewProcessor()
    {
        EventTapConfig eventTapConfig = new EventTapConfig();
        BatchProcessorFactory batchProcessorFactory = createBatchProcessorFactory();
        String type1 = "Type1";
        String type2 = "Type2";
        ServiceDescriptor tap1 = createServiceDescriptor(type1);
        ServiceDescriptor tap2 = createServiceDescriptor(type2);
        ServiceSelector serviceSelector = mock(ServiceSelector.class);
        EventTapWriter eventTapWriter = new EventTapWriter(
                serviceSelector, httpClient, EVENT_LIST_JSON_CODEC, executorService,
                batchProcessorFactory, eventTapConfig);

        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.of(tap1));
        eventTapWriter.start();

        Event eventType1a = createEvent(type1);
        Event eventType1b = createEvent(type1);
        Event eventType2a = createEvent(type2);
        Event eventType2b = createEvent(type2);

        eventTapWriter.write(eventType1a);
        eventTapWriter.write(eventType2a);
        checkEventsReceived(type1, eventType1a);

        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.of(tap1, tap2));
        eventTapWriter.refreshFlows();

        eventTapWriter.write(eventType1b);
        eventTapWriter.write(eventType2b);
        checkEventsReceived(type1, eventType1a, eventType1b);
        checkEventsReceived(type2, eventType2b);
    }

    @Test
    public void testWriteDoesntSentToOldProcessor()
    {
        EventTapConfig eventTapConfig = new EventTapConfig();
        BatchProcessorFactory batchProcessorFactory = createBatchProcessorFactory();
        String type1 = "Type1";
        String type2 = "Type2";
        ServiceDescriptor tap1 = createServiceDescriptor(type1);
        ServiceDescriptor tap2 = createServiceDescriptor(type2);
        ServiceSelector serviceSelector = mock(ServiceSelector.class);
        EventTapWriter eventTapWriter = new EventTapWriter(
                serviceSelector, httpClient, EVENT_LIST_JSON_CODEC, executorService,
                batchProcessorFactory, eventTapConfig);

        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.of(tap1, tap2));
        eventTapWriter.start();

        Event eventType1a = createEvent(type1);
        Event eventType1b = createEvent(type1);
        Event eventType2a = createEvent(type2);
        Event eventType2b = createEvent(type2);

        eventTapWriter.write(eventType1a);
        eventTapWriter.write(eventType2a);
        checkEventsReceived(type1, eventType1a);
        checkEventsReceived(type2, eventType2a);

        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.of(tap1));
        eventTapWriter.refreshFlows();

        eventTapWriter.write(eventType1b);
        eventTapWriter.write(eventType2b);
        checkEventsReceived(type1, eventType1a, eventType1b);
        checkEventsReceived(type2, eventType2a);
    }

    private Event createEvent(String type)
    {
        return new Event(type, randomUUID().toString(), "host", DateTime.now(), ImmutableMap.<String, Object>of());
    }

    private void checkActiveProcessors(String... types)
    {
        List<String> typesAsList = ImmutableList.copyOf(types);

        for (String type : typesAsList) {
            assertTrue(batchProcessors.containsKey(type), format("no processor created for %s", type));

            List<BatchProcessor<Event>> processors = ImmutableList.copyOf(batchProcessors.get(type));
            assertEquals(processors.size(), 1, format("wrong number of processors for %s", type));

            // The batch processor should have been started, but not stopped
            // if it is still active.
            BatchProcessor<Event> processor = processors.get(0);
            verify(processor).start();
            verify(processor, never()).stop();
        }

        // For all non-active processors, make sure they have been stopped.
        for (Entry<String, Collection<BatchProcessor<Event>>> entry : batchProcessors.asMap().entrySet()) {
            if (typesAsList.contains(entry.getKey())) {
                continue;           // Handles in loop above
            }

            List<BatchProcessor<Event>> processors = ImmutableList.copyOf(entry.getValue());
            assertEquals(processors.size(), 1, format("wrong number of processors for %s", entry.getKey()));

            // The batch processor should have been started and stopped.
            BatchProcessor<Event> processor = processors.get(0);
            verify(processor).start();
            verify(processor).stop();
        }
    }

    private void checkEventsReceived(String type, Event... events)
    {
        assertTrue(batchProcessors.containsKey(type), format("no processor for type %s", type));

        List<BatchProcessor<Event>> processors = ImmutableList.copyOf(batchProcessors.get(type));
        assertEquals(processors.size(), 1);
        BatchProcessor<Event> processor = processors.get(0);

        ArgumentCaptor<Event> eventArgumentCaptor = ArgumentCaptor.forClass(Event.class);
        verify(processor, times(events.length)).put(eventArgumentCaptor.capture());
        assertEqualsNoOrder(eventArgumentCaptor.getAllValues().toArray(), events);
    }

    private ServiceDescriptor createServiceDescriptor(String eventType)
    {
        String nodeId = randomUUID().toString();
        return new ServiceDescriptor(
                randomUUID(),
                nodeId,
                "EventTap",
                "global",
                "/" + nodeId,
                ServiceState.RUNNING,
                ImmutableMap.<String, String>of("eventType", eventType, "tapId", "1", "http", format("http://%s.event.tap", eventType)));
    }

    private BatchProcessorFactory createBatchProcessorFactory()
    {
        return new MockBatchProcessorFactory();
    }

    private class MockBatchProcessorFactory implements BatchProcessorFactory
    {
        @Override
        public BatchProcessor createBatchProcessor(BatchHandler batchHandler, String eventType)
        {
            @SuppressWarnings("unchecked")
            BatchProcessor<Event> batchProcessor = mock(BatchProcessor.class);
            batchProcessors.put(eventType, batchProcessor);
            return batchProcessor;
        }
    }
}
