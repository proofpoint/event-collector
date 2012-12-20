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
import com.google.common.collect.Sets;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceState;
import com.proofpoint.discovery.client.testing.StaticServiceSelector;
import com.proofpoint.event.collector.BatchProcessor.BatchHandler;
import com.proofpoint.event.collector.EventCounters.CounterState;
import com.proofpoint.event.collector.EventTapFlow.Observer;
import org.joda.time.DateTime;
import org.logicalshift.concurrent.SerialScheduledExecutorService;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.concat;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestEventTapWriter
{
    private ServiceSelector serviceSelector;
    private SerialScheduledExecutorService executorService;
    private BatchProcessorFactory batchProcessorFactory = new MockBatchProcessorFactory();
    private Multimap<String, MockBatchProcessor<Event>> batchProcessors;
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
        String typeA = "TypeA";
        String typeB = "TypeB";
        ServiceDescriptor tapA = createServiceDescriptor(typeA);
        ServiceDescriptor tapB = createServiceDescriptor(typeB);
        Event eventA1 = createEvent(typeA);
        Event eventA2 = createEvent(typeA);
        Event eventB1 = createEvent(typeB);
        Event eventB2 = createEvent(typeB);

        updateThenRefreshFlowsThenCheck(tapA);
        eventTapWriter.write(eventA1);
        eventTapWriter.write(eventB1);
        checkTapEvents(tapA, eventA1);
        checkTapEvents(tapB);

        updateThenRefreshFlowsThenCheck(tapA, tapB);
        eventTapWriter.write(eventA2);
        eventTapWriter.write(eventB2);
        checkTapEvents(tapA, eventA1, eventA2);
        checkTapEvents(tapB, eventB2);
    }

    @Test
    public void testRefreshFlowsRemovesOldEntries()
    {
        String typeA = "TypeA";
        String typeB = "TypeB";
        ServiceDescriptor tapA = createServiceDescriptor(typeA);
        ServiceDescriptor tapB = createServiceDescriptor(typeB);
        Event eventA1 = createEvent(typeA);
        Event eventA2 = createEvent(typeA);
        Event eventB1 = createEvent(typeB);
        Event eventB2 = createEvent(typeB);

        updateThenRefreshFlowsThenCheck(tapA, tapB);
        eventTapWriter.write(eventA1);
        eventTapWriter.write(eventB1);
        checkTapEvents(tapA, eventA1);
        checkTapEvents(tapB, eventB1);

        updateThenRefreshFlowsThenCheck(tapA);
        eventTapWriter.write(eventA2);
        eventTapWriter.write(eventB2);
        checkTapEvents(tapA, eventA1, eventA2);
        checkTapEvents(tapB, eventB1);
    }

    @Test
    public void testRefreshFlowsUpdatesExistingProcessor()
    {
        // If the taps for a given event type changes, don't create the processor
        String typeA = "TypeA";
        ServiceDescriptor tapA1 = createServiceDescriptor(typeA, ImmutableMap.of("tapId", "1"));
        ServiceDescriptor tapA2 = createServiceDescriptor(typeA, ImmutableMap.of("tapId", "2"));
        Event eventA1 = createEvent(typeA);
        Event eventA2 = createEvent(typeA);

        updateThenRefreshFlowsThenCheck(tapA1);
        eventTapWriter.write(eventA1);
        checkTapEvents(tapA1, eventA1);
        checkTapEvents(tapA2);

        updateThenRefreshFlowsThenCheck(tapA2);
        eventTapWriter.write(eventA2);
        checkTapEvents(tapA1, eventA1);
        checkTapEvents(tapA2, eventA2);
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

        Event event1 = createEvent(type1);
        Event event2 = createEvent(type2);
        Event event3 = createEvent(type3);

        eventTapWriter.write(event1);
        eventTapWriter.write(event2);
        eventTapWriter.write(event3);

        checkTapEvents(tap1, event1);
        checkTapEvents(tap2, event2);
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

        Event event1a = createEvent(type1);
        Event event1b = createEvent(type1);
        Event event2a = createEvent(type2);
        Event event2b = createEvent(type2);

        eventTapWriter.write(event1a);
        eventTapWriter.write(event2a);
        checkTapEvents(tap1, event1a);

        updateTaps(tap1, tap2);
        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.of(tap1, tap2));
        eventTapWriter.refreshFlows();

        eventTapWriter.write(event1b);
        eventTapWriter.write(event2b);
        checkTapEvents(tap1, event1a, event1b);
        checkTapEvents(tap2, event2b);
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

        Event event1a = createEvent(type1);
        Event event1b = createEvent(type1);
        Event event2a = createEvent(type2);
        Event event2b = createEvent(type2);

        eventTapWriter.write(event1a);
        eventTapWriter.write(event2a);
        checkTapEvents(tap1, event1a);
        checkTapEvents(tap2, event2a);

        updateTaps(tap1);
        eventTapWriter.refreshFlows();

        eventTapWriter.write(event1b);
        eventTapWriter.write(event2b);
        checkTapEvents(tap1, event1a, event1b);
        checkTapEvents(tap2, event2a);
    }

    @Test
    public void testQueueCounters()
    {
        String type = "Type";
        ServiceDescriptor tap = createServiceDescriptor(type);
        Event event1 = createEvent(type);
        Event event2 = createEvent(type);
        updateTaps(tap);
        eventTapWriter.start();

        MockBatchProcessor<Event> processors = batchProcessors.get(type).iterator().next();
        eventTapWriter.write(event1);
        checkCounters(eventTapWriter.getQueueCounters(), type, 1, 0);
        checkCountersForOnly(eventTapWriter.getQueueCounters(), type);
        processors.succeed = false;
        eventTapWriter.write(event2);
        checkCounters(eventTapWriter.getQueueCounters(), type, 2, 1);
        checkCountersForOnly(eventTapWriter.getQueueCounters(), type);
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

            List<MockBatchProcessor<Event>> processors = ImmutableList.copyOf(batchProcessors.get(processorName));
            assertEquals(processors.size(), 1, format("wrong number of processors for %s", processorName));

            // The batch processor should have been started, but not stopped
            // if it is still active.
            MockBatchProcessor<Event> processor = processors.get(0);
            assertEquals(processor.startCount, 1);
            assertEquals(processor.stopCount, 0);
        }

        // For all non-active processors, make sure they have been stopped.
        for (Entry<String, Collection<MockBatchProcessor<Event>>> entry : batchProcessors.asMap().entrySet()) {
            String processorName = entry.getKey();
            ServiceDescriptor tap = null;
            for (ServiceDescriptor t : tapsAsList) {
                if (processorName.equals(extractProcessorName(t))) {
                    tap = t;
                    break;
                }
            }
            if (tap != null) {
                continue;           // Handled in loop above
            }

            List<MockBatchProcessor<Event>> processors = ImmutableList.copyOf(entry.getValue());
            assertEquals(processors.size(), 1, format("wrong number of processors for %s", processorName));

            // The batch processor should have been started and stopped.
            MockBatchProcessor<Event> processor = processors.get(0);
            assertEquals(processor.startCount, 1);
            assertEquals(processor.stopCount, 1);
        }
    }

    private void checkTapEvents(ServiceDescriptor tap, Event... events)
    {
        String eventType = nullToEmpty(tap.getProperties().get("eventType"));
        String flowId = nullToEmpty(tap.getProperties().get("tapId"));
        EventTapFlow eventTapFlow = eventTapFlows.get(ImmutableList.of(eventType, flowId));
        @SuppressWarnings("deprecated")
        ArgumentCaptor<List<Event>> eventArgumentCaptor = new ArgumentCaptor<List<Event>>();

        if (events.length == 0) {
            if (eventTapFlow != null) {
                verify(eventTapFlow, never()).processBatch(any(List.class));
            }
        }
        else {
            assertNotNull(eventTapFlow);
            verify(eventTapFlow, atLeast(1)).processBatch(eventArgumentCaptor.capture());
            List<Event> actualEvents = ImmutableList.copyOf(concat(eventArgumentCaptor.getAllValues()));
            assertEqualsNoOrder(actualEvents.toArray(), ImmutableList.copyOf(events).toArray());
        }
    }

    private void checkCounters(Map<String, CounterState> counters, String type, int received, int lost)
    {
        CounterState counterState = counters.get(type);
        assertEquals(counterState.getReceived(), received);
        assertEquals(counterState.getLost(), lost);
    }

    private void checkCountersForOnly(Map<String, CounterState> counters, String... types)
    {
        assertEquals(Sets.difference(counters.keySet(), ImmutableSet.copyOf(types)), ImmutableSet.<String>of());
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
        public <T> BatchProcessor<T> createBatchProcessor(BatchHandler<T> batchHandler, String eventType, BatchProcessor.Observer observer)
        {
            @SuppressWarnings("unchecked")
            MockBatchProcessor batchProcessor = new MockBatchProcessor(eventType, batchHandler, observer);
            batchProcessors.put(eventType, batchProcessor);
            return batchProcessor;
        }
    }

    private class MockBatchProcessor<T> implements BatchProcessor<T>
    {
        public int startCount = 0;
        public int stopCount = 0;
        public boolean succeed = true;
        public List<T> entries = new LinkedList<T>();
        public final String name;

        private BatchHandler<T> handler;
        private Observer observer;

        public MockBatchProcessor(String name, BatchHandler<T> batchHandler, Observer observer)
        {
            this.name = name;
            this.handler = batchHandler;
            this.observer = observer;
        }

        @Override
        public void start()
        {
            startCount += 1;
        }

        @Override
        public void stop()
        {
            stopCount += 1;
        }

        @Override
        public void put(T entry)
        {
            entries.add(entry);
            if (succeed) {
                handler.processBatch(ImmutableList.of(entry));
            }
            else {
                observer.onRecordsLost(1);
            }
            observer.onRecordsReceived(1);
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
