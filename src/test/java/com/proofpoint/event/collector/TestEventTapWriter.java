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
import com.proofpoint.event.collector.EventTapFlow.Observer;
import org.joda.time.DateTime;
import org.logicalshift.concurrent.SerialScheduledExecutorService;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Strings.nullToEmpty;
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
    private ServiceSelector serviceSelector;
    private SerialScheduledExecutorService executorService;
    private BatchProcessorFactory batchProcessorFactory = new MockBatchProcessorFactory();
    private Multimap<String, BatchProcessor<Event>> batchProcessors;
    private EventTapFlowFactory eventTapFlowFactory = new MockEventTapFlowFactory();
    private Map<List<String>, EventTapFlow> eventTapFlows;
    private EventTapConfig eventTapConfig;
    private EventTapWriter eventTapWriter;

    @BeforeMethod
    public void setup()
    {
        serviceSelector = new StaticServiceSelector(ImmutableSet.<ServiceDescriptor>of());
        executorService = new SerialScheduledExecutorService();
        batchProcessors = HashMultimap.create();
        eventTapFlows = new HashMap();
        eventTapConfig = new EventTapConfig();
        serviceSelector = mock(ServiceSelector.class);
        eventTapWriter = new EventTapWriter(
                serviceSelector, executorService,
                batchProcessorFactory, eventTapFlowFactory,
                eventTapConfig);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "selector is null")
    public void testConstructorNullSelector()
    {
        new EventTapWriter(null, executorService, batchProcessorFactory, eventTapFlowFactory, new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "executorService is null")
    public void testConstructorNullExecutorService()
    {
        new EventTapWriter(serviceSelector, null, batchProcessorFactory, eventTapFlowFactory, new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "batchProcessorFactory is null")
    public void testConstructorNullBatchProcessorFactory()
    {
        new EventTapWriter(serviceSelector, executorService, null, eventTapFlowFactory, new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventTapFlowFactory is null")
    public void testConstructorNullEventTapFlowFactory()
    {
        new EventTapWriter(serviceSelector, executorService, batchProcessorFactory, null, new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "config is null")
    public void testConstructorNullConfig()
    {
        new EventTapWriter(serviceSelector, executorService, batchProcessorFactory, eventTapFlowFactory, null);
    }

    @Test
    public void testRefreshFlowsCreatesNewEntries()
    {
        ServiceDescriptor tapA = createServiceDescriptor("TypeA");
        ServiceDescriptor tapB = createServiceDescriptor("TypeB");

        updateThenRefreshFlowsThenCheck(tapA);
        updateThenRefreshFlowsThenCheck(tapA, tapB);
    }

    @Test
    public void testRefreshFlowsRemovesOldEntries()
    {
        ServiceDescriptor tapA = createServiceDescriptor("TypeA");
        ServiceDescriptor tapB = createServiceDescriptor("TypeB");

        updateThenRefreshFlowsThenCheck(tapA, tapB);
        updateThenRefreshFlowsThenCheck(tapA);
    }

    @Test
    public void testRefreshFlowsUpdatesExistingProcessor()
    {
        // If the entries for a given tap changes, don't create the processor
        String typeA = "TypeA";
        ServiceDescriptor tapA1 = createServiceDescriptor(typeA);
        ServiceDescriptor tapA2 = createServiceDescriptor(typeA);

        updateThenRefreshFlowsThenCheck(tapA1);
        updateThenRefreshFlowsThenCheck(tapA2);
    }

    @Test
    public void testRefreshFlowsIsCalledPeriodically()
    {
        String type1 = "Type1";
        String type2 = "Type2";
        String type3 = "Type3";
        ServiceDescriptor tap1 = createServiceDescriptor(type1);
        ServiceDescriptor tap2 = createServiceDescriptor(type2);
        ServiceDescriptor tap3 = createServiceDescriptor(type3);

        updateTaps(tap1);

        // When refreshFlows() is called, it will create new processors using the factory,
        // which adds them to the batchProcessor map.
        assertTrue(batchProcessors.isEmpty());
        eventTapWriter.start();

        assertTrue(batchProcessors.containsKey(type1));
        assertEquals(batchProcessors.get(type1).size(), 1);

        // If the refreshFlows() is called after the period, tap2 should be
        // created to handle the new tap after one period.
        updateTaps(tap2);
        executorService.elapseTime(
                (long) eventTapConfig.getEventTapRefreshDuration().toMillis() - 1,
                TimeUnit.MILLISECONDS);
        assertFalse(batchProcessors.containsKey(type2));
        executorService.elapseTime(1, TimeUnit.MILLISECONDS);
        assertTrue(batchProcessors.containsKey(type2));
        assertEquals(batchProcessors.get(type2).size(), 1);

        // Same is true after the second period, but with tap3.
        updateTaps(tap3);
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
        String type1 = "Type1";
        String type2 = "Type2";
        String type3 = "Type3";
        ServiceDescriptor tap1 = createServiceDescriptor(type1);
        ServiceDescriptor tap2 = createServiceDescriptor(type2);

        updateTaps(tap1, tap2);
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
        String type1 = "Type1";
        String type2 = "Type2";
        ServiceDescriptor tap1 = createServiceDescriptor(type1);
        ServiceDescriptor tap2 = createServiceDescriptor(type2);

        updateTaps(tap1);
        eventTapWriter.start();

        Event eventType1a = createEvent(type1);
        Event eventType1b = createEvent(type1);
        Event eventType2a = createEvent(type2);
        Event eventType2b = createEvent(type2);

        eventTapWriter.write(eventType1a);
        eventTapWriter.write(eventType2a);
        checkEventsReceived(type1, eventType1a);

        updateTaps(tap1, tap2);
        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.of(tap1, tap2));
        eventTapWriter.refreshFlows();

        eventTapWriter.write(eventType1b);
        eventTapWriter.write(eventType2b);
        checkEventsReceived(type1, eventType1a, eventType1b);
        checkEventsReceived(type2, eventType2b);
    }

    @Test
    public void testWriteDoesntSendToOldProcessor()
    {
        String type1 = "Type1";
        String type2 = "Type2";
        ServiceDescriptor tap1 = createServiceDescriptor(type1);
        ServiceDescriptor tap2 = createServiceDescriptor(type2);

        updateTaps(tap1, tap2);
        eventTapWriter.start();

        Event eventType1a = createEvent(type1);
        Event eventType1b = createEvent(type1);
        Event eventType2a = createEvent(type2);
        Event eventType2b = createEvent(type2);

        eventTapWriter.write(eventType1a);
        eventTapWriter.write(eventType2a);
        checkEventsReceived(type1, eventType1a);
        checkEventsReceived(type2, eventType2a);

        updateTaps(tap1);
        eventTapWriter.refreshFlows();

        eventTapWriter.write(eventType1b);
        eventTapWriter.write(eventType2b);
        checkEventsReceived(type1, eventType1a, eventType1b);
        checkEventsReceived(type2, eventType2a);
    }

    private void updateThenRefreshFlowsThenCheck(ServiceDescriptor... taps)
    {
        updateTaps(taps);
        eventTapWriter.refreshFlows();
        checkActiveProcessors(taps);
    }

    private void updateTaps(ServiceDescriptor... taps)
    {
        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.copyOf(taps));
    }

    private Event createEvent(String type)
    {
        return new Event(type, randomUUID().toString(), "host", DateTime.now(), ImmutableMap.<String, Object>of());
    }

    private void checkActiveProcessors(ServiceDescriptor... taps)
    {
        List<ServiceDescriptor> tapsAsList = ImmutableList.copyOf(taps);

        for (ServiceDescriptor tap : tapsAsList) {
            String processorName = extractProcessorName(tap);
            assertTrue(batchProcessors.containsKey(processorName), format("no processor created for %s", processorName));

            List<BatchProcessor<Event>> processors = ImmutableList.copyOf(batchProcessors.get(processorName));
            assertEquals(processors.size(), 1, format("wrong number of processors for %s", processorName));

            // The batch processor should have been started, but not stopped
            // if it is still active.
            BatchProcessor<Event> processor = processors.get(0);
            verify(processor).start();
            verify(processor, never()).stop();
        }

        // For all non-active processors, make sure they have been stopped.
        for (Entry<String, Collection<BatchProcessor<Event>>> entry : batchProcessors.asMap().entrySet()) {
            String processorName = entry.getKey();
            ServiceDescriptor tap = null;
            for (ServiceDescriptor t: tapsAsList) {
                if (processorName.equals(extractProcessorName(t))) {
                    tap = t;
                    break;
                }
            }
            if (tap != null) {
                continue;           // Handled in loop above
            }

            List<BatchProcessor<Event>> processors = ImmutableList.copyOf(entry.getValue());
            assertEquals(processors.size(), 1, format("wrong number of processors for %s", processorName));

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

    private String extractProcessorName(ServiceDescriptor tap)
    {
        return nullToEmpty(tap.getProperties().get("eventType"));
    }

    private ServiceDescriptor createServiceDescriptor(String eventType, Map<String, String> properties)
    {
        String nodeId = randomUUID().toString();
        ImmutableMap.Builder<String,String> builder = ImmutableMap.builder();

        builder.putAll(properties);
        builder.put("eventType", eventType);
        if (!properties.containsKey("tapId")) {
            builder.put("tapId", "1");
        }
        if (!properties.containsKey("http")) {
             builder.put("http", format("http://%s.event.tap", eventType));
        }
        return new ServiceDescriptor(
                randomUUID(),
                nodeId,
                "EventTap",
                "global",
                "/" + nodeId,
                ServiceState.RUNNING,
                builder.build());
    }

    private ServiceDescriptor createServiceDescriptor(String eventType)
    {
        return createServiceDescriptor(eventType, ImmutableMap.<String, String>of());
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

    private class MockEventTapFlowFactory implements EventTapFlowFactory
    {
        @Override
        public EventTapFlow createEventTapFlow(String eventType, String flowId, Set<URI> taps, Observer observer)
        {
            List<String> key = ImmutableList.of(eventType, flowId);
            EventTapFlow eventTapFlow = eventTapFlows.get(key);
            if (eventTapFlow == null) {
                eventTapFlow = mock(EventTapFlow.class);
                eventTapFlows.put(key, eventTapFlow);
            }
            return eventTapFlow;
        }
    }

}
