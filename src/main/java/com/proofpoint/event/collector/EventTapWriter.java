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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceType;
import com.proofpoint.event.collector.BatchProcessor.BatchHandler;
import com.proofpoint.event.collector.EventTapWriter.EventTypePolicy.FlowPolicy;
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

public class EventTapWriter implements EventWriter
{
    @VisibleForTesting
    static final String FLOW_ID_PROPERTY_NAME = "flowId";

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

        refreshJob = executorService.scheduleAtFixedRate(new Runnable()
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
            for (Map.Entry<String, FlowPolicy> flowPolicyEntry : eventTypePolicy.flowPolicies.entrySet()) {
                String flowId = flowPolicyEntry.getKey();
                FlowPolicy flowPolicy = flowPolicyEntry.getValue();
                stopBatchProcessor(eventType, flowId, flowPolicy.processor);
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
        try {
            Map<String, EventTypePolicy> existingPolicies = eventTypePolicies.get();
            Map<String, Map<String, FlowInfo>> existingFlows = flows;
            ImmutableMap.Builder<String, EventTypePolicy> policiesBuilder = ImmutableMap.builder();

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
        catch (Exception e) {
            log.error(e, "Couldn't refresh flows");
        }

    }

    @Override
    public void write(Event event)
    {
        for (FlowPolicy flowPolicy : getPolicyForEvent(event).flowPolicies.values()) {
            flowPolicy.processor.put(event);
        }
    }

    private Map<String, Map<String, FlowInfo>> constructFlowInfoFromDiscovery()
    {
        List<ServiceDescriptor> descriptors = selector.selectAllServices();
        // First level is EventType, second is flowId
        Map<String, Map<String, FlowInfo.Builder>> flows = new HashMap<>();

        for (ServiceDescriptor descriptor : descriptors) {
            Map<String, String> properties = descriptor.getProperties();
            String eventType = properties.get("eventType");
            String flowId = properties.get(FLOW_ID_PROPERTY_NAME);
            URI uri = safeUriFromString(properties.get("http"));
            String qosDelivery = properties.get("qos.delivery");

            if (isNullOrEmpty(eventType) || isNullOrEmpty(flowId) || uri == null) {
                continue;
            }

            Map<String, FlowInfo.Builder> flowsForEventType = flows.get(eventType);
            if (flowsForEventType == null) {
                flowsForEventType = new HashMap<>();
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

        log.debug("Constructing policy for %s", eventType);

        for (Entry<String, FlowInfo> flowEntry : flows.entrySet()) {
            String flowId = flowEntry.getKey();
            FlowInfo updatedFlowInfo = flowEntry.getValue();
            log.debug("** considering flow ID %s", flowId);
            Set<URI> destinations = ImmutableSet.copyOf(updatedFlowInfo.destinations);
            FlowPolicy existingFlowPolicy = existingPolicy.flowPolicies.get(flowId);

            if (existingFlowPolicy == null || existingFlowPolicy.qosEnabled != updatedFlowInfo.qosEnabled) {
                log.debug("**-> making new policy because %s", existingFlowPolicy == null ? "existing is null" : "qos changed");
                EventTapFlow eventTapFlow;
                if (updatedFlowInfo.qosEnabled) {
                    eventTapFlow = createQosEventTapFlow(eventType, flowId, destinations);
                }
                else {
                    eventTapFlow = createNonQosEventTapFlow(eventType, flowId, destinations);
                }
                log.debug("  -> made flow with destinations %s", destinations);
                BatchProcessor<Event> batchProcessor = createBatchProcessor(eventType, flowId, eventTapFlow);
                policyBuilder.addFlowPolicy(flowId, batchProcessor, eventTapFlow, updatedFlowInfo.qosEnabled);
                batchProcessor.start();
            }
            else if (!destinations.equals(existingFlowPolicy.eventTapFlow.getTaps())) {
                log.debug("**-> changing taps from %s to %s", existingFlowPolicy.eventTapFlow.getTaps(), destinations);
                existingFlowPolicy.eventTapFlow.setTaps(destinations);
                policyBuilder.addFlowPolicy(flowId, existingFlowPolicy);
            }
            else {
                log.debug("**-> keeping as is with destinations %s", existingFlowPolicy.eventTapFlow.getTaps());
                policyBuilder.addFlowPolicy(flowId, existingFlowPolicy);
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
        for (Entry<String, FlowPolicy> flowPolicyEntry : existingPolicy.flowPolicies.entrySet()) {
            String flowId = flowPolicyEntry.getKey();
            FlowPolicy existingFlowPolicy = flowPolicyEntry.getValue();
            FlowPolicy newFlowPolicy = newPolicy.flowPolicies.get(flowId);

            if (newFlowPolicy == null || newFlowPolicy.processor != existingFlowPolicy.processor) {
                stopBatchProcessor(eventType, flowId, existingFlowPolicy.processor);
            }
        }
    }

    private EventTypePolicy getPolicyForEvent(Event event)
    {
        return firstNonNull(eventTypePolicies.get().get(event.getType()), NULL_EVENT_TYPE_POLICY);
    }

    private BatchProcessor<Event> createBatchProcessor(String eventType, String flowId, BatchHandler<Event> batchHandler)
    {
        return batchProcessorFactory.createBatchProcessor(createBatchProcessorName(eventType, flowId), batchHandler);
    }

    private EventTapFlow createNonQosEventTapFlow(String eventType, String flowId, Set<URI> taps)
    {
        return eventTapFlowFactory.createEventTapFlow(eventType, flowId, taps);
    }

    private EventTapFlow createQosEventTapFlow(String eventType, String flowId, Set<URI> taps)
    {
        return eventTapFlowFactory.createQosEventTapFlow(eventType, flowId, taps);
    }

    private void stopBatchProcessor(String eventType, String flowId, BatchProcessor<Event> processor)
    {
        log.debug("Stopping processor %s: no longer required", createBatchProcessorName(eventType, flowId));
        processor.stop();
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

    private static String createBatchProcessorName(String eventType, String flowId)
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
        public final Map<String, FlowPolicy> flowPolicies;

        private EventTypePolicy(
                Map<String, FlowPolicy> flowPolicies)
        {
            this.flowPolicies = flowPolicies;
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public static class FlowPolicy
        {
            public final BatchProcessor<Event> processor;
            public final EventTapFlow eventTapFlow;
            public final boolean qosEnabled;

            public FlowPolicy(BatchProcessor<Event> processor, EventTapFlow eventTapFlow, boolean qosEnabled)
            {
                this.processor = processor;
                this.eventTapFlow = eventTapFlow;
                this.qosEnabled = qosEnabled;
            }
        }

        public static class Builder
        {
            private ImmutableMap.Builder<String, FlowPolicy> flowPoliciesBuilder = ImmutableMap.builder();

            public Builder addFlowPolicy(String flowId, BatchProcessor<Event> processor, EventTapFlow eventTapFlow, boolean qosEnabled)
            {
                return addFlowPolicy(flowId, new FlowPolicy(processor, eventTapFlow, qosEnabled));
            }

            public Builder addFlowPolicy(String flowId, FlowPolicy flowPolicy)
            {
                flowPoliciesBuilder.put(flowId, flowPolicy);
                return this;
            }

            public EventTypePolicy build()
            {
                return new EventTypePolicy(flowPoliciesBuilder.build());
            }
        }
    }
}
