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
import com.proofpoint.event.collector.EventTapWriter.EventTypeInfo.QosInfo;
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

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.nullToEmpty;

public class EventTapWriter implements EventWriter, EventTapStats
{
    private static final Logger log = Logger.get(EventTapWriter.class);
    private static final EventTypeInfo NULL_EVENT_TYPE_INFO = new EventTypeInfo.Builder().build();
    private final ServiceSelector selector;
    private final ScheduledExecutorService executorService;
    private final BatchProcessorFactory batchProcessorFactory;
    private final EventTapFlowFactory eventTapFlowFactory;

    private final AtomicReference<Map<String, EventTypeInfo>> eventTypeInfoMap = new AtomicReference<Map<String, EventTypeInfo>>(
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
        for (Map.Entry<String, EventTypeInfo> entry : eventTypeInfoMap.get().entrySet()) {
            String eventType = entry.getKey();
            EventTypeInfo eventTypeInfo = entry.getValue();
            stopBestEffortProcessor(eventType, eventTypeInfo.bestEffortProcessor);
            for (Map.Entry<String, QosInfo> qosEntry : eventTypeInfo.qosTaps.entrySet()) {
                String flowId = qosEntry.getKey();
                QosInfo qosInfo = qosEntry.getValue();
                stopQosProcessor(eventType, flowId, qosInfo.processor);
            }
        }
        eventTypeInfoMap.set(ImmutableMap.<String, EventTypeInfo>of());

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
        Map<String, EventTypeInfo> oldEventTypeInfoMap = eventTypeInfoMap.get();
        // First level is EventType, second is flowId
        Map<String, Multimap<String, URI>> flows = new HashMap<String, Multimap<String, URI>>();
        ImmutableMap.Builder<String, EventTypeInfo> eventTypeInfoBuilder = ImmutableMap.<String, EventTypeInfo>builder();
        List<ServiceDescriptor> descriptors = selector.selectAllServices();
        Set<List<String>> qosFlows = new HashSet<List<String>>();

        log.debug("Processing %s service descriptors", descriptors.size());
        for (ServiceDescriptor descriptor : descriptors) {
            log.debug("Processing descriptor %s", descriptor.getId());
            Map<String, String> properties = descriptor.getProperties();
            String eventType = properties.get("eventType");
            if (nullToEmpty(eventType).isEmpty()) {
                log.debug("Skipping descriptor %s: no event type", descriptor.getId());
                continue;
            }
            String flowId = properties.get("tapId");
            if (nullToEmpty(flowId).isEmpty()) {
                log.debug("Skipping descriptor %s: no flow ID", descriptor.getId());
                continue;
            }
            URI uri;
            try {
                uri = URI.create(properties.get("http"));
            }
            catch (Exception e) {
                log.debug("Skipping descriptor %s: invalid or missing destination", descriptor.getId());
                continue;
            }
            String qosDelivery = properties.get("qos.delivery");
            if (nullToEmpty(qosDelivery).equalsIgnoreCase("retry")) {
                log.debug("Found QoS descriptor %s", descriptor.getId());
                qosFlows.add(ImmutableList.of(eventType, flowId));
            }

            Multimap<String, URI> typeInfo = flows.get(eventType);
            if (typeInfo == null) {
                typeInfo = ArrayListMultimap.<String, URI>create();
                flows.put(eventType, typeInfo);
            }
            typeInfo.put(flowId, uri);
            log.debug("Handling descriptor %s: id=%s uri=%s", descriptor.getId(), flowId, uri);
        }

        for (Map.Entry<String, Multimap<String, URI>> eventTypeFlows : flows.entrySet()) {
            String eventType = eventTypeFlows.getKey();
            Multimap<String, URI> flowsForType = eventTypeFlows.getValue();
            EventTypeInfo.Builder newEventTypeInfoBuilder = new EventTypeInfo.Builder();
            EventTypeInfo oldEventTypeInfo = firstNonNull(oldEventTypeInfoMap.get(eventType), NULL_EVENT_TYPE_INFO);
            BatchProcessor<Event> bestEffortProcessor = oldEventTypeInfo.bestEffortProcessor;
            BatchCloner<Event> bestEffortCloner = oldEventTypeInfo.bestEffortCloner;

            for (Entry<String, Collection<URI>> flowEntry : flowsForType.asMap().entrySet()) {
                String flowId = flowEntry.getKey();
                Set<URI> uris = ImmutableSet.copyOf(flowEntry.getValue());

                if (qosFlows.contains(ImmutableList.of(eventType, flowId))) {
                    QosInfo qosInfo = oldEventTypeInfo.qosTaps.get(flowId);
                    if (qosInfo == null) {
                        log.debug("new QoS event flow: id=%s uris=%s", flowId, uris);
                        EventTapFlow eventTapFlow = createEventTapFlow(eventType, flowId, uris);
                        BatchProcessor<Event> batchProcessor = createBatchProcessor(eventType, createQosName(eventType, flowId), eventTapFlow);
                        newEventTypeInfoBuilder.withQosTap(flowId, batchProcessor, eventTapFlow);
                        log.debug("Starting processor for qos type %s (flowId %s)", eventType, flowId);
                        batchProcessor.start();
                    }
                    else if (!uris.equals(qosInfo.tapFlow.getTaps())) {
                        log.debug("update QoS event flow: id=%s uris=%s (was %s)",
                                flowId, uris, qosInfo.tapFlow.getTaps());
                        qosInfo.tapFlow.setTaps(uris);
                        newEventTypeInfoBuilder.withQosTap(flowId, qosInfo.processor, qosInfo.tapFlow);
                    }
                }
                else {
                    if (bestEffortCloner == null) {
                        bestEffortCloner = new BatchCloner<Event>();
                    }
                    if (bestEffortProcessor == null) {
                        bestEffortProcessor = createBatchProcessor(eventType, createBestEffortName(eventType), bestEffortCloner);
                        log.debug("Starting best effort processor for type %s", eventType);
                        bestEffortProcessor.start();
                    }
                    newEventTypeInfoBuilder.withBestEffortCloner(bestEffortCloner);
                    newEventTypeInfoBuilder.withBestEffortProcessor(bestEffortProcessor);

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
            }

            eventTypeInfoBuilder.put(eventType, newEventTypeInfoBuilder.build());
        }

        Map<String, EventTypeInfo> newEventTypeInfoMap = eventTypeInfoBuilder.build();
        eventTypeInfoMap.set(newEventTypeInfoMap);

        // Now that the new processors are going to be used, stop the old ones
        // that were not pulled into the new map.
        // NOTE: If a flowId moved from QoS to non-QoS (or vice versa) a new EventTapFlow
        //       is created.
        log.debug("Cleaning up processors that are no longer required");
        for (Map.Entry<String, EventTypeInfo> entry : oldEventTypeInfoMap.entrySet()) {
            String eventType = entry.getKey();
            EventTypeInfo oldEventTypeInfo = entry.getValue();
            EventTypeInfo newEventTypeInfo = newEventTypeInfoMap.get(eventType);
            log.debug("Checking event type %s", eventType);

            if (oldEventTypeInfo.bestEffortProcessor != null &&
                    (newEventTypeInfo == null || newEventTypeInfo.bestEffortProcessor == null)) {
                log.debug("Event type %s no longer has a best-effort processor", eventType);
                stopBestEffortProcessor(eventType, oldEventTypeInfo.bestEffortProcessor);
            }

            for (Map.Entry<String, QosInfo> qosEntry : oldEventTypeInfo.qosTaps.entrySet()) {
                String flowId = qosEntry.getKey();
                QosInfo oldQosInfo = qosEntry.getValue();
                QosInfo newQosInfo = null;

                log.debug("Checking event type %s, qos %s", eventType, flowId);

                if (newEventTypeInfo != null) {
                    newQosInfo = newEventTypeInfo.qosTaps.get(flowId);
                }

                if (newQosInfo == null || newQosInfo.processor != oldQosInfo.processor) {
                    log.debug("Event type %s no longer has a qos flow %s", eventType, flowId);
                    stopQosProcessor(eventType, flowId, oldQosInfo.processor);
                }
            }
        }
    }

    @Override
    public void write(Event event)
    {
        EventTypeInfo eventTypeInfo = this.eventTypeInfoMap.get().get(event.getType());
        if (eventTypeInfo != null) {
            if (eventTypeInfo.bestEffortProcessor != null) {
                eventTypeInfo.bestEffortProcessor.put(event);
            }
            for (QosInfo qosInfo : eventTypeInfo.qosTaps.values()) {
                qosInfo.processor.put(event);
            }
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

    private BatchProcessor<Event> createBatchProcessor(final String eventType, String name, BatchProcessor.BatchHandler<Event> batchHandler)
    {
        return batchProcessorFactory.createBatchProcessor(name, batchHandler, new Observer()
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

    private EventTapFlow createEventTapFlow(String eventType, String flowId, Set<URI> taps)
    {
        final List<String> key = ImmutableList.of(eventType, flowId);

        return eventTapFlowFactory.createEventTapFlow(eventType, flowId, taps,
                new EventTapFlow.Observer()
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
                });
    }

    private void stopProcessor(String processorName, BatchProcessor<Event> processor)
    {
        log.debug("Stopping processor %s: no longer required", processorName);
        processor.stop();
    }

    private void stopBestEffortProcessor(String eventType, BatchProcessor<Event> processor)
    {
        stopProcessor(createBestEffortName(eventType), processor);
    }

    private void stopQosProcessor(String eventType, String flowId, BatchProcessor<Event> processor)
    {
        stopProcessor(createQosName(eventType, flowId), processor);
    }

    private String createBestEffortName(String eventType)
    {
        return eventType;
    }

    private String createQosName(String eventType, String flowId)
    {
        return String.format("%s{%s}", eventType, flowId);
    }

    @VisibleForTesting
    static class EventTypeInfo
    {
        public final BatchProcessor<Event> bestEffortProcessor;
        public final BatchCloner<Event> bestEffortCloner;
        public final Map<String, EventTapFlow> bestEffortTaps;
        public final Map<String, QosInfo> qosTaps;

        private EventTypeInfo(
                BatchProcessor<Event> bestEffortProcessor,
                BatchCloner<Event> bestEffortCloner,
                Map<String, EventTapFlow> bestEffortTaps,
                Map<String, QosInfo> qosTaps)
        {
            this.bestEffortProcessor = bestEffortProcessor;
            this.bestEffortCloner = bestEffortCloner;
            this.bestEffortTaps = bestEffortTaps;
            this.qosTaps = qosTaps;
        }

        public static class QosInfo
        {
            public final BatchProcessor<Event> processor;
            public final EventTapFlow tapFlow;

            public QosInfo(BatchProcessor<Event> processor, EventTapFlow tapFlow)
            {
                this.processor = processor;
                this.tapFlow = tapFlow;
            }
        }

        public static class Builder
        {
            private BatchProcessor<Event> bestEffortProcessor = null;
            private BatchCloner<Event> bestEffortCloner = null;
            private ImmutableMap.Builder<String, EventTapFlow> bestEffortTapsBuilder = ImmutableMap.builder();
            private ImmutableMap.Builder<String, QosInfo> qosTapsBuilder = ImmutableMap.builder();

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

            public Builder withQosTap(String flowId, BatchProcessor<Event> processor, EventTapFlow eventTapFlow)
            {
                return withQosTap(flowId, new QosInfo(processor, eventTapFlow));
            }

            public Builder withQosTap(String flowId, QosInfo qosInfo)
            {
                qosTapsBuilder.put(flowId, qosInfo);
                return this;
            }

            public EventTypeInfo build()
            {
                Map<String, EventTapFlow> bestEffortTaps = bestEffortTapsBuilder.build();
                if (bestEffortCloner != null) {
                    bestEffortCloner.setDestinations(ImmutableSet.copyOf(bestEffortTaps.values()));
                }
                return new EventTypeInfo(bestEffortProcessor, bestEffortCloner,
                        bestEffortTaps, qosTapsBuilder.build());
            }
        }
    }
}
