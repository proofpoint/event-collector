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
import com.proofpoint.discovery.client.ServiceType;
import com.proofpoint.event.collector.BatchProcessor.BatchHandler;
import com.proofpoint.event.collector.EventCounters.CounterState;
import com.proofpoint.event.collector.EventTapWriter.EventTypePolicy.QosPolicy;
import com.proofpoint.log.Logger;
import com.proofpoint.units.Duration;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.net.URI;
import java.util.HashMap;
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
import static com.google.common.base.Strings.isNullOrEmpty;

public class EventTapWriter implements EventWriter, EventTapStats
{
    private static final Logger log = Logger.get(EventTapWriter.class);
    private static final EventTypePolicy NULL_EVENT_TYPE_POLICY = new EventTypePolicy.Builder().build();
    private final ServiceSelector selector;
    private final ScheduledExecutorService executorService;
    private final BatchProcessorFactory batchProcessorFactory;
    private final EventTapFlowFactory eventTapFlowFactory;

    private final AtomicReference<Map<String, EventTypePolicy>> eventTypePolicies = new AtomicReference<Map<String, EventTypePolicy>>(
            ImmutableMap.<String, EventTypePolicy>of());
    private Map<String, Map<String, FlowInfo>> flows = ImmutableMap.of();

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
        for (Map.Entry<String, EventTypePolicy> entry : eventTypePolicies.get().entrySet()) {
            String eventType = entry.getKey();
            EventTypePolicy eventTypePolicy = entry.getValue();
            stopNonQosBatchProcessor(eventType, eventTypePolicy.nonQosBatchProcessor);
            for (Map.Entry<String, QosPolicy> qosEntry : eventTypePolicy.qosPolicies.entrySet()) {
                String flowId = qosEntry.getKey();
                QosPolicy qosPolicy = qosEntry.getValue();
                stopQosBatchProcessor(eventType, flowId, qosPolicy.processor);
            }
        }
        eventTypePolicies.set(ImmutableMap.<String, EventTypePolicy>of());

        if (refreshJob != null) {
            refreshJob.cancel(false);
            refreshJob = null;
        }
    }

    @Managed
    public void refreshFlows()
    {
        Map<String, EventTypePolicy> existingPolicies = eventTypePolicies.get();
        Map<String, Map<String, FlowInfo>> existingFlows = flows;
        ImmutableMap.Builder<String, EventTypePolicy> policiesBuilder = ImmutableMap.<String, EventTypePolicy>builder();

        Map<String, Map<String, FlowInfo>> newFlows = constructFlowInfoFromDiscovery();
        if (existingFlows.equals(newFlows)) {
            return;
        }

        for (Map.Entry<String, Map<String, FlowInfo>> flowsForEventTypeEntry : newFlows.entrySet()) {
            String eventType = flowsForEventTypeEntry.getKey();
            Map<String, FlowInfo> flowsForEventType = flowsForEventTypeEntry.getValue();
            EventTypePolicy existingPolicy = firstNonNull(existingPolicies.get(eventType), NULL_EVENT_TYPE_POLICY);

            policiesBuilder.put(eventType, constructPolicyForFlows(existingPolicy, eventType, flowsForEventType));
        }

        Map<String, EventTypePolicy> newPolicies = policiesBuilder.build();
        eventTypePolicies.set(newPolicies);
        flows = newFlows;

        stopExistingPoliciesNoLongerInUse(existingPolicies, newPolicies);
    }

    @Override
    public void write(Event event)
    {
        EventTypePolicy eventTypePolicy = this.eventTypePolicies.get().get(event.getType());
        if (eventTypePolicy != null) {
            if (eventTypePolicy.nonQosBatchProcessor != null) {
                eventTypePolicy.nonQosBatchProcessor.put(event);
            }
            for (QosPolicy qosPolicy : eventTypePolicy.qosPolicies.values()) {
                qosPolicy.processor.put(event);
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

    private Map<String, Map<String, FlowInfo>> constructFlowInfoFromDiscovery()
    {
        List<ServiceDescriptor> descriptors = selector.selectAllServices();
        // First level is EventType, second is flowId
        Map<String, Map<String, FlowInfo.Builder>> flows = new HashMap<String, Map<String, FlowInfo.Builder>>();

        for (ServiceDescriptor descriptor : descriptors) {
            Map<String, String> properties = descriptor.getProperties();
            String eventType = properties.get("eventType");
            String flowId = properties.get("tapId");
            URI uri = safeUriFromString(properties.get("http"));
            String qosDelivery = properties.get("qos.delivery");

            if (isNullOrEmpty(eventType) || isNullOrEmpty(flowId) || uri == null) {
                continue;
            }

            Map<String, FlowInfo.Builder> flowsForEventType = flows.get(eventType);
            if (flowsForEventType == null) {
                flowsForEventType = new HashMap<String, FlowInfo.Builder>();
                flows.put(eventType, flowsForEventType);
            }

            FlowInfo.Builder flowBuilder = flowsForEventType.get(flowId);
            if (flowBuilder == null) {
                flowBuilder = FlowInfo.builder();
                flowsForEventType.put(flowId, flowBuilder);
            }

            if ("retry".equalsIgnoreCase(qosDelivery)) {
                flowBuilder.setQosEnabled(true);
            }

            flowBuilder.addDestination(uri);
        }

        return constructFlowsFromBuilderMap(flows);
    }

    private Map<String, Map<String, FlowInfo>> constructFlowsFromBuilderMap(Map<String, Map<String, FlowInfo.Builder>> flows)
    {
        ImmutableMap.Builder<String, Map<String, FlowInfo>> flowsBuilder = ImmutableMap.builder();
        for (Entry<String, Map<String, FlowInfo.Builder>> flowsEntry : flows.entrySet()) {
            String eventType = flowsEntry.getKey();
            Map<String, FlowInfo.Builder> flowsForEventType = flowsEntry.getValue();

            ImmutableMap.Builder<String, FlowInfo> flowsBuilderForEventType = ImmutableMap.builder();
            for (Entry<String, FlowInfo.Builder> flowsForEventTypeEntry : flowsForEventType.entrySet()) {
                String flowId = flowsForEventTypeEntry.getKey();
                FlowInfo.Builder flowBuilder = flowsForEventTypeEntry.getValue();
                flowsBuilderForEventType.put(flowId, flowBuilder.build());
            }

            flowsBuilder.put(eventType, flowsBuilderForEventType.build());
        }

        return flowsBuilder.build();
    }

    private EventTypePolicy constructPolicyForFlows(EventTypePolicy existingPolicy, String eventType, Map<String, FlowInfo> flows)
    {
        EventTypePolicy.Builder policyBuilder = EventTypePolicy.builder();
        BatchProcessor<Event> nonQosProcessor = existingPolicy.nonQosBatchProcessor;
        BatchCloner<Event> nonQosCloner = existingPolicy.nonQosBatchCloner;

        for (Entry<String, FlowInfo> flowEntry : flows.entrySet()) {
            String flowId = flowEntry.getKey();
            FlowInfo flow = flowEntry.getValue();
            Set<URI> destinations = ImmutableSet.copyOf(flow.destinations);

            if (flow.qosEnabled) {
                QosPolicy qosPolicy = existingPolicy.qosPolicies.get(flowId);
                if (qosPolicy == null) {
                    log.debug("New QoS event flow: id=%s destinations=%s", flowId, destinations);
                    EventTapFlow eventTapFlow = createQosEventTapFlow(eventType, flowId, destinations);
                    BatchProcessor<Event> batchProcessor = createQosBatchProcessor(eventType, flowId, eventTapFlow);
                    policyBuilder.addQosPolicy(flowId, batchProcessor, eventTapFlow);
                    log.debug("Starting processor for qos type %s (flowId %s)", eventType, flowId);
                    batchProcessor.start();
                }
                else if (!destinations.equals(qosPolicy.eventTapFlow.getTaps())) {
                    log.debug("Update QoS event flow: id=%s destinations=%s (was %s)",
                            flowId, destinations, qosPolicy.eventTapFlow.getTaps());
                    qosPolicy.eventTapFlow.setTaps(destinations);
                    policyBuilder.addQosPolicy(flowId, qosPolicy.processor, qosPolicy.eventTapFlow);
                }
                else {
                    policyBuilder.addQosPolicy(flowId, qosPolicy);
                }
            }
            else {
                if (nonQosCloner == null) {
                    nonQosCloner = new BatchCloner<Event>();
                }
                if (nonQosProcessor == null) {
                    nonQosProcessor = createNonQosBatchProcessor(eventType, nonQosCloner);
                    log.debug("Starting non-QoS batch processor for type %s", eventType);
                    nonQosProcessor.start();
                }
                policyBuilder.setNonQosBatchCloner(nonQosCloner);
                policyBuilder.setNonQosBatchProcessor(nonQosProcessor);

                EventTapFlow oldEventTapFlow = existingPolicy.nonQosEventTapFlows.get(flowId);

                EventTapFlow newEventTapFlow;
                if (oldEventTapFlow == null) {
                    log.debug("New event flow: id=%s destinations=%s", flowId, destinations);
                    newEventTapFlow = createNonQosEventTapFlow(eventType, flowId, destinations);
                }
                else if (!destinations.equals(oldEventTapFlow.getTaps())) {
                    log.debug("Update event flow: id=%s destinations=%s (was %s)",
                            flowId, destinations, oldEventTapFlow.getTaps());
                    newEventTapFlow = oldEventTapFlow;
                    newEventTapFlow.setTaps(destinations);
                }
                else {
                    newEventTapFlow = oldEventTapFlow;
                }

                policyBuilder.addNonQosEventTapFlow(flowId, newEventTapFlow);
            }
        }

        return policyBuilder.build();
    }

    private void stopExistingPoliciesNoLongerInUse(Map<String, EventTypePolicy> existingPolicies, Map<String, EventTypePolicy> newPolicies)
    {
        // NOTE: If a flowId moved from QoS to non-QoS (or vice versa) a new EventTapFlow
        //       is created.
        log.debug("Cleaning up processors that are no longer required");
        for (Entry<String, EventTypePolicy> policyEntry : existingPolicies.entrySet()) {
            String eventType = policyEntry.getKey();
            stopExistingPolicyIfNoLongerInUse(eventType,
                    policyEntry.getValue(),
                    firstNonNull(newPolicies.get(eventType), NULL_EVENT_TYPE_POLICY));
        }
    }

    private void stopExistingPolicyIfNoLongerInUse(String eventType, EventTypePolicy existingPolicy, EventTypePolicy newPolicy)
    {
        if (existingPolicy.nonQosBatchProcessor != null && existingPolicy.nonQosBatchProcessor != newPolicy.nonQosBatchProcessor) {
            stopNonQosBatchProcessor(eventType, existingPolicy.nonQosBatchProcessor);
        }

        for (Entry<String, QosPolicy> qosEntry : existingPolicy.qosPolicies.entrySet()) {
            String flowId = qosEntry.getKey();
            QosPolicy existingQosPolicy = qosEntry.getValue();
            QosPolicy newQosPolicy = newPolicy.qosPolicies.get(flowId);

            if (newQosPolicy == null || newQosPolicy.processor != existingQosPolicy.processor) {
                stopQosBatchProcessor(eventType, flowId, existingQosPolicy.processor);
            }
        }
    }

    private BatchProcessor<Event> createNonQosBatchProcessor(String eventType, BatchHandler<Event> batchHandler)
    {
        return batchProcessorFactory.createBatchProcessor(createNonQosBatchProcessorName(eventType), batchHandler, createBatchProcessorObserver(eventType));
    }

    private BatchProcessor<Event> createQosBatchProcessor(String eventType, String flowId, BatchHandler<Event> batchHandler)
    {
        return batchProcessorFactory.createBatchProcessor(createQosBatchProcessorName(eventType, flowId), batchHandler, createBatchProcessorObserver(eventType));
    }

    private BatchProcessor.Observer createBatchProcessorObserver(final String eventType)
    {
        return new BatchProcessor.Observer()
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
        };
    }

    private EventTapFlow createNonQosEventTapFlow(String eventType, String flowId, Set<URI> taps)
    {
        log.debug("Create event tap flow: eventType=%s id=%s uris=%s", eventType, flowId, taps);
        return eventTapFlowFactory.createEventTapFlow(eventType, flowId, taps, createEventTapFlowObserver(eventType, flowId));
    }

    private EventTapFlow createQosEventTapFlow(String eventType, String flowId, Set<URI> taps)
    {
        log.debug("Create QoS event tap flow: eventType=%s flowId=%s taps=%s", eventType, flowId, taps);
        return eventTapFlowFactory.createQosEventTapFlow(eventType, flowId, taps, createEventTapFlowObserver(eventType, flowId));
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

    private void stopProcessor(String processorName, BatchProcessor<Event> processor)
    {
        log.debug("Stopping processor %s: no longer required", processorName);
        processor.stop();
    }

    private void stopNonQosBatchProcessor(String eventType, BatchProcessor<Event> processor)
    {
        stopProcessor(createNonQosBatchProcessorName(eventType), processor);
    }

    private void stopQosBatchProcessor(String eventType, String flowId, BatchProcessor<Event> processor)
    {
        stopProcessor(createQosBatchProcessorName(eventType, flowId), processor);
    }

    private static URI safeUriFromString(String uri)
    {
        try {
            return URI.create(uri);
        }
        catch (Exception ignored) {
            return null;
        }
    }

    private static String createNonQosBatchProcessorName(String eventType)
    {
        return eventType;
    }

    private static String createQosBatchProcessorName(String eventType, String flowId)
    {
        return String.format("%s{%s}", eventType, flowId);
    }

    static class FlowInfo
    {
        private final boolean qosEnabled;
        private final Set<URI> destinations;

        private FlowInfo(boolean qosEnabled, Set<URI> destinations)
        {
            this.qosEnabled = qosEnabled;
            this.destinations = ImmutableSet.copyOf(destinations);
        }

        static Builder builder()
        {
            return new Builder();
        }

        static class Builder
        {
            private boolean qosEnabled = false;
            private ImmutableSet.Builder<URI> destinations = ImmutableSet.builder();

            public Builder setQosEnabled(boolean enabled)
            {
                qosEnabled = enabled;
                return this;
            }

            public Builder addDestination(URI destination)
            {
                destinations.add(destination);
                return this;
            }

            public FlowInfo build()
            {
                return new FlowInfo(qosEnabled, destinations.build());
            }
        }
    }

    static class EventTypePolicy
    {
        public final BatchProcessor<Event> nonQosBatchProcessor;
        public final BatchCloner<Event> nonQosBatchCloner;
        public final Map<String, EventTapFlow> nonQosEventTapFlows;
        public final Map<String, QosPolicy> qosPolicies;

        private EventTypePolicy(
                BatchProcessor<Event> nonQosBatchProcessor,
                BatchCloner<Event> nonQosBatchCloner,
                Map<String, EventTapFlow> nonQosEventTapFlows,
                Map<String, QosPolicy> qosPolicies)
        {
            this.nonQosBatchProcessor = nonQosBatchProcessor;
            this.nonQosBatchCloner = nonQosBatchCloner;
            this.nonQosEventTapFlows = nonQosEventTapFlows;
            this.qosPolicies = qosPolicies;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static class QosPolicy
        {
            public final BatchProcessor<Event> processor;
            public final EventTapFlow eventTapFlow;

            public QosPolicy(BatchProcessor<Event> processor, EventTapFlow eventTapFlow)
            {
                this.processor = processor;
                this.eventTapFlow = eventTapFlow;
            }
        }

        public static class Builder
        {
            private BatchProcessor<Event> nonQosBatchProcessor = null;
            private BatchCloner<Event> nonQosBatchCloner = null;
            private ImmutableMap.Builder<String, EventTapFlow> nonQosEventTapFlows = ImmutableMap.builder();
            private ImmutableMap.Builder<String, QosPolicy> qosPoliciesBuilder = ImmutableMap.builder();

            public Builder setNonQosBatchProcessor(BatchProcessor<Event> batchProcessor)
            {
                this.nonQosBatchProcessor = batchProcessor;
                return this;
            }

            public Builder setNonQosBatchCloner(BatchCloner<Event> batchCloner)
            {
                this.nonQosBatchCloner = batchCloner;
                return this;
            }

            public Builder addNonQosEventTapFlow(String flowId, EventTapFlow eventTapFlow)
            {
                nonQosEventTapFlows.put(flowId, eventTapFlow);
                return this;
            }

            public Builder addQosPolicy(String flowId, BatchProcessor<Event> processor, EventTapFlow eventTapFlow)
            {
                return addQosPolicy(flowId, new QosPolicy(processor, eventTapFlow));
            }

            public Builder addQosPolicy(String flowId, QosPolicy qosPolicy)
            {
                qosPoliciesBuilder.put(flowId, qosPolicy);
                return this;
            }

            public EventTypePolicy build()
            {
                Map<String, EventTapFlow> nonQosEventTapFlows = this.nonQosEventTapFlows.build();
                if (nonQosBatchCloner != null) {
                    nonQosBatchCloner.setDestinations(ImmutableSet.copyOf(nonQosEventTapFlows.values()));
                }
                return new EventTypePolicy(nonQosBatchProcessor, nonQosBatchCloner,
                        nonQosEventTapFlows, qosPoliciesBuilder.build());
            }
        }
    }
}
