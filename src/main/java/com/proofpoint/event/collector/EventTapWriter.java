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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceType;
import com.proofpoint.event.collector.BatchProcessor.Observer;
import com.proofpoint.event.collector.EventCounters.CounterState;
import com.proofpoint.log.Logger;
import com.proofpoint.units.Duration;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.nullToEmpty;

public class EventTapWriter implements EventWriter, EventTapStats
{
    private static final Logger log = Logger.get(EventTapWriter.class);
    private final ServiceSelector selector;
    private final ScheduledExecutorService executorService;
    private final BatchProcessorFactory batchProcessorFactory;
    private final EventTapFlowFactory eventTapFlowFactory;

    private final AtomicReference<Map<String, EventTypeInfo>> eventTypeInfo = new AtomicReference<Map<String, EventTypeInfo>>(
            ImmutableMap.<String, EventTypeInfo>of());

    private ScheduledFuture<?> refreshJob;
    private final Duration flowRefreshDuration;

    private final EventCounters<String> queueCounters = new EventCounters<String>();
    private final EventCounters<List<String>> flowCounters = new EventCounters<List<String>>();

    @Inject
    public EventTapWriter(@ServiceType("eventTap") ServiceSelector selector,
            @EventTap ScheduledExecutorService executorService,
            BatchProcessorFactory batchProcessorFactory,
            EventTapFlowFactory eventTapFlowFactory,
            EventTapConfig config)
    {
        this.selector = checkNotNull(selector, "selector is null");
        this.executorService = checkNotNull(executorService, "executorService is null");
        this.flowRefreshDuration = checkNotNull(config, "config is null").getEventTapRefreshDuration();
        this.batchProcessorFactory = checkNotNull(batchProcessorFactory, "batchProcessorFactory is null");
        this.eventTapFlowFactory = checkNotNull(eventTapFlowFactory, "eventTapFlowFactory is null");
    }

    @PostConstruct
    public synchronized void start()
    {
        // has the refresh job already been started
        if (refreshJob != null) {
            return;
        }

        refreshFlows();
        refreshJob = executorService.scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run()
            {
                refreshFlows();
            }
        }, (long) flowRefreshDuration.toMillis(), (long) flowRefreshDuration.toMillis(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public synchronized void stop()
    {
        for (Map.Entry<String, EventTypeInfo> entry : eventTypeInfo.get().entrySet()) {
            stopEventType(entry.getKey(), entry.getValue());
        }
        eventTypeInfo.set(ImmutableMap.<String, EventTypeInfo>of());

        if (refreshJob != null) {
            refreshJob.cancel(false);
            refreshJob = null;
        }
    }

    @Managed
    public void refreshFlows()
    {
        // This function works by creating a new Map of EventTypeInfo objects
        // based on the current taps in discovery. The EventTypeInfo objects
        // in the new map are new, but they refer to the existing BatchProcessor,
        // BatchCloner, and EventTapFlow objects for taps that still exist.
        Map<String, EventTypeInfo> eventTypeInfo = this.eventTypeInfo.get();
        // First level is EventType, second is flowId
        Map<String, Multimap<String, URI>> flows = new HashMap<String, Multimap<String, URI>>();
        ImmutableMap.Builder<String, EventTypeInfo> eventTypeInfoBuilder = ImmutableMap.<String, EventTypeInfo>builder();
        List<ServiceDescriptor> descriptors = selector.selectAllServices();
        Set<String> seenEventTypes = new HashSet<String>();

        for (ServiceDescriptor descriptor : descriptors) {
            Map<String, String> properties = descriptor.getProperties();
            String eventType = properties.get("eventType");
            if (nullToEmpty(eventType).isEmpty()) {
                continue;
            }
            String flowId = properties.get("tapId");
            if (nullToEmpty(flowId).isEmpty()) {
                continue;
            }
            URI uri;
            try {
                uri = URI.create(properties.get("http"));
            }
            catch (Exception e) {
                continue;
            }
            Multimap<String, URI> typeInfo = flows.get(eventType);
            if (typeInfo == null) {
                typeInfo = ArrayListMultimap.<String, URI>create();
                flows.put(eventType, typeInfo);
            }
            typeInfo.put(flowId, uri);
            seenEventTypes.add(eventType);
        }

        for (Map.Entry<String, Multimap<String, URI>> eventTypeFlows : flows.entrySet()) {
            String eventType = eventTypeFlows.getKey();
            Multimap<String, URI> flowsForType = eventTypeFlows.getValue();
            EventTypeInfo.Builder newEventTypeInfoBuilder = new EventTypeInfo.Builder();

            EventTypeInfo oldEventTypeInfo = eventTypeInfo.get(eventType);
            if (oldEventTypeInfo == null) {
                BatchCloner<Event> batchCloner = new BatchCloner<Event>();
                oldEventTypeInfo = new EventTypeInfo.Builder()
                        .withBestEffortCloner(batchCloner)
                        .withBestEffortProcessor(createBatchProcessor(eventType, batchCloner))
                        .build();

                log.debug("Starting processor for type %s", eventType);
                oldEventTypeInfo.bestEffortProcessor.start();
            }

            newEventTypeInfoBuilder
                    .withBestEffortProcessor(oldEventTypeInfo.bestEffortProcessor)
                    .withBestEffortCloner(oldEventTypeInfo.bestEffortCloner);

            for (Entry<String, Collection<URI>> flowEntry : flowsForType.asMap().entrySet()) {
                String flowId = flowEntry.getKey();
                Set<URI> uris = ImmutableSet.copyOf(flowEntry.getValue());
                EventTapFlow oldEventTapFlow = oldEventTypeInfo.bestEffortTaps.get(flowId);

                EventTapFlow newEventTapFlow;
                if (oldEventTapFlow == null) {
                    log.debug("new event flow: id=%s uris=%s", flowId, uris);
                    newEventTapFlow = createEventTapFlow(eventType, flowId, uris);
                }
                else if (!uris.equals(oldEventTapFlow.getTaps())) {
                    log.debug("update event flow: id=%s uris=%s (was %s)",
                            flowId, uris, oldEventTapFlow.getTaps());
                    newEventTapFlow = oldEventTapFlow;
                    newEventTapFlow.setTaps(uris);
                }
                else {
                    newEventTapFlow = oldEventTapFlow;
                }

                newEventTypeInfoBuilder.withBestEffortTap(flowId, newEventTapFlow);
            }

            EventTypeInfo newEventTypeInfo = newEventTypeInfoBuilder.build();
            newEventTypeInfo.bestEffortCloner.setDestinations(
                    ImmutableSet.copyOf(newEventTypeInfo.bestEffortTaps.values()));

            eventTypeInfoBuilder.put(eventType, newEventTypeInfo);
        }

        this.eventTypeInfo.set(eventTypeInfoBuilder.build());

        // Now that the new processors are going to be used, stop the old ones
        // that were not pulled into the new map.
        for (Map.Entry<String, EventTypeInfo> entry : eventTypeInfo.entrySet()) {
            String eventType = entry.getKey();
            if (!seenEventTypes.contains(eventType)) {
                stopEventType(eventType, entry.getValue());
            }
        }
    }

    @Override
    public void write(Event event)
    {
        EventTypeInfo eventTypeInfo = this.eventTypeInfo.get().get(event.getType());
        if (eventTypeInfo != null) {
            eventTypeInfo.bestEffortProcessor.put(event);
        }
    }

    public Map<String, CounterState> getQueueCounters()
    {
        return queueCounters.getCounts();
    }

    @Override
    public void resetQueueCounters()
    {
        queueCounters.resetCounts();
    }

    @Override
    public Map<String, CounterState> getFlowCounters()
    {
        return flowCounters.getCounts();
    }

    @Override
    public void resetFlowCounters()
    {
        flowCounters.resetCounts();
    }

    private BatchProcessor<Event> createBatchProcessor(final String eventType, BatchProcessor.BatchHandler<Event> batchHandler)
    {
        return batchProcessorFactory.createBatchProcessor(eventType, batchHandler, new Observer()
        {
            @Override
            public void onRecordsLost(int count)
            {
                queueCounters.recordLost(eventType, count);
            }

            @Override
            public void onRecordsReceived(int count)
            {
                queueCounters.recordReceived(eventType, count);
            }
        });
    }

    private EventTapFlow createEventTapFlow(final String eventType, final String flowId, Set<URI> taps)
    {
        log.debug("Create event tap flow: eventType=%s id=%s uris=%s", eventType, flowId, taps);
        return eventTapFlowFactory.createEventTapFlow(eventType, flowId, taps, createEventTapFlowObserver(eventType, flowId));
    }

    private EventTapFlow.Observer createEventTapFlowObserver(String eventType, String flowId)
    {
        final List<String> key = ImmutableList.of(eventType, flowId);

        return new EventTapFlow.Observer()
        {
            @Override
            public void onRecordsSent(URI uri, int count)
            {
                flowCounters.recordReceived(key, count);
            }

            @Override
            public void onRecordsLost(URI uri, int count)
            {
                flowCounters.recordLost(key, count);
            }
        };
    }

    private void stopEventType(String eventType, EventTypeInfo eventTypeInfo)
    {
        log.debug("Stopping processor for type %s: no longer required", eventType);
        eventTypeInfo.bestEffortProcessor.stop();
    }

    @VisibleForTesting
    static class EventTypeInfo
    {
        public final BatchProcessor<Event> bestEffortProcessor;
        public final BatchCloner<Event> bestEffortCloner;
        public final Map<String, EventTapFlow> bestEffortTaps;

        private EventTypeInfo(
                BatchProcessor<Event> bestEffortProcessor,
                BatchCloner<Event> bestEffortCloner,
                Map<String, EventTapFlow> bestEffortTaps)
        {
            this.bestEffortProcessor = bestEffortProcessor;
            this.bestEffortCloner = bestEffortCloner;
            this.bestEffortTaps = bestEffortTaps;
        }

        public static class Builder
        {
            private BatchProcessor<Event> bestEffortProcessor = null;
            private BatchCloner<Event> bestEffortCloner = null;
            private ImmutableMap.Builder<String, EventTapFlow> bestEffortTapsBuilder = ImmutableMap.builder();

            public Builder withBestEffortProcessor(BatchProcessor<Event> bestEffortProcessor)
            {
                this.bestEffortProcessor = bestEffortProcessor;
                return this;
            }

            public Builder withBestEffortCloner(BatchCloner<Event> bestEffortCloner)
            {
                this.bestEffortCloner = bestEffortCloner;
                return this;
            }

            public Builder withBestEffortTap(String flowId, EventTapFlow eventTapFlow)
            {
                bestEffortTapsBuilder.put(flowId, eventTapFlow);
                return this;
            }

            public EventTypeInfo build()
            {
                return new EventTypeInfo(bestEffortProcessor, bestEffortCloner, bestEffortTapsBuilder.build());
            }
        }
    }
}
