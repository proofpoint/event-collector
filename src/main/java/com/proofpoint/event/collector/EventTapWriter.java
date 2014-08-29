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
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceType;
import com.proofpoint.event.collector.EventTapWriter.EventTypePolicy.FlowPolicy;
import com.proofpoint.event.collector.StaticEventTapConfig.FlowKey;
import com.proofpoint.log.Logger;
import com.proofpoint.units.Duration;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.IOException;
import java.net.URI;
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
import static com.proofpoint.event.collector.QosDelivery.BEST_EFFORT;
import static com.proofpoint.event.collector.QosDelivery.RETRY;
import static java.lang.String.format;

public class EventTapWriter implements EventWriter
{
    @VisibleForTesting
    static final String FLOW_ID_PROPERTY_NAME = "flowId";

    @VisibleForTesting
    static final String HTTP_PROPERTY_NAME = "http";

    @VisibleForTesting
    static final String EVENT_TYPE_PROPERTY_NAME = "eventType";

    private static final String HTTPS_PROPERTY_NAME = "https";
    private static final String QOS_DELIVERY_PROPERTY_NAME = "qos.delivery";

    private static final Logger log = Logger.get(EventTapWriter.class);
    private static final EventTypePolicy NULL_EVENT_TYPE_POLICY = new EventTypePolicy.Builder().build();
    private final ServiceSelector selector;
    private final ScheduledExecutorService executorService;
    private final BatchProcessorFactory batchProcessorFactory;
    private final EventTapFlowFactory eventTapFlowFactory;
    private final boolean allowHttpConsumers;

    private final AtomicReference<Map<String, EventTypePolicy>> eventTypePolicies = new AtomicReference<Map<String, EventTypePolicy>>(
            ImmutableMap.<String, EventTypePolicy>of());
    private final List<TapSpec> tapSpecs;
    private Table<String, String, FlowInfo> flows = ImmutableTable.of();

    private ScheduledFuture<?> refreshJob;
    private final Duration flowRefreshDuration;

    @Inject
    public EventTapWriter(@ServiceType("eventTap") ServiceSelector selector,
            @EventTap ScheduledExecutorService executorService,
            BatchProcessorFactory batchProcessorFactory,
            EventTapFlowFactory eventTapFlowFactory,
            EventTapConfig config,
            StaticEventTapConfig staticEventTapConfig)
    {
        this.tapSpecs = createTapSpecFromConfig(checkNotNull(staticEventTapConfig, "staticEventTapConfig is null"));
        this.selector = checkNotNull(selector, "selector is null");
        this.executorService = checkNotNull(executorService, "executorService is null");
        this.flowRefreshDuration = checkNotNull(config, "config is null").getEventTapRefreshDuration();
        this.allowHttpConsumers = config.isAllowHttpConsumers();
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
            Table<String, String, FlowInfo> existingFlows = flows;
            ImmutableMap.Builder<String, EventTypePolicy> policiesBuilder = ImmutableMap.builder();

            Table<String, String, FlowInfo> newFlows = constructFlowInfoFromTapSpec(Iterables.concat(tapSpecs, createTapSpecFromDiscovery(selector.selectAllServices())));
            if (existingFlows.equals(newFlows)) {
                return;
            }

            for (String eventType : newFlows.rowKeySet()) {
                EventTypePolicy existingPolicy = firstNonNull(existingPolicies.get(eventType), NULL_EVENT_TYPE_POLICY);
                Map<String, FlowInfo> flowsForEventType = newFlows.row(eventType);
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

    @Override
    public void distribute(Event event)
            throws IOException
    {
        write(event);
    }

    private Table<String, String, FlowInfo> constructFlowInfoFromTapSpec(Iterable<TapSpec> tapSpecs)
    {
        Table<String, String, FlowInfo.Builder> flows = HashBasedTable.create();

        for (TapSpec tapSpec : tapSpecs) {
            String eventType = tapSpec.getEventType();
            String flowId = tapSpec.getFlowId();

            URI uri = tapSpec.getUri();

            if (isNullOrEmpty(eventType) || isNullOrEmpty(flowId) || uri == null) {
                continue;
            }

            FlowInfo.Builder flowBuilder = flows.get(eventType, flowId);
            if (flowBuilder == null) {
                flowBuilder = FlowInfo.builder();
                flows.put(eventType, flowId, flowBuilder);
            }

            QosDelivery qosDelivery = tapSpec.getQosDelivery();
            if (RETRY.equals(qosDelivery)) {
                flowBuilder.setQosEnabled(true);
            }

            flowBuilder.addDestination(uri);
        }

        return constructFlowsFromTable(flows);
    }

    private Table<String, String, FlowInfo> constructFlowsFromTable(Table<String, String, FlowInfo.Builder> flows)
    {
        ImmutableTable.Builder<String, String, FlowInfo> flowsBuilder = ImmutableTable.builder();

        for (String eventType : flows.rowKeySet()) {
            for (Entry<String, FlowInfo.Builder> flowsForEventTypeEntry : flows.row(eventType).entrySet()) {
                String flowId = flowsForEventTypeEntry.getKey();
                FlowInfo.Builder flowBuilder = flowsForEventTypeEntry.getValue();
                flowsBuilder.put(eventType, flowId, flowBuilder.build());
            }
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
                    eventTapFlow = eventTapFlowFactory.createQosEventTapFlow(eventType, flowId, destinations);
                }
                else {
                    eventTapFlow = eventTapFlowFactory.createEventTapFlow(eventType, flowId, destinations);
                }
                log.debug("  -> made flow with destinations %s", destinations);
                BatchProcessor<Event> batchProcessor = batchProcessorFactory.createBatchProcessor(createBatchProcessorName(eventType, flowId), eventTapFlow);
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

    private void stopBatchProcessor(String eventType, String flowId, BatchProcessor<Event> processor)
    {
        log.debug("Stopping processor %s: no longer required", createBatchProcessorName(eventType, flowId));
        processor.stop();
    }

    private List<TapSpec> createTapSpecFromDiscovery(Iterable<ServiceDescriptor> descriptors)
    {
        ImmutableList.Builder<TapSpec> tapSpecBuilder = ImmutableList.builder();
        for (ServiceDescriptor descriptor : descriptors) {

            Map<String, String> properties = descriptor.getProperties();
            String eventType = properties.get(EVENT_TYPE_PROPERTY_NAME);
            String flowId = properties.get(FLOW_ID_PROPERTY_NAME);

            URI uri = safeUriFromString(properties.get(HTTPS_PROPERTY_NAME));
            if (uri == null && allowHttpConsumers) {
                uri = safeUriFromString(properties.get(HTTP_PROPERTY_NAME));
            }

            String qosDeliveryString = firstNonNull(properties.get(QOS_DELIVERY_PROPERTY_NAME), BEST_EFFORT.toString());
            TapSpec tapSpec = new TapSpec(eventType, flowId, uri, QosDelivery.fromString(qosDeliveryString));

            tapSpecBuilder.add(tapSpec);
        }

        return tapSpecBuilder.build();
    }

    private static List<TapSpec> createTapSpecFromConfig(StaticEventTapConfig staticEventTapConfig)
    {
        ImmutableList.Builder<TapSpec> tapSpecBuilder = ImmutableList.builder();
        for (Entry<FlowKey, PerFlowStaticEventTapConfig> entry : staticEventTapConfig.getStaticTaps().entrySet()) {
            FlowKey flowKey = entry.getKey();
            String eventType = flowKey.getEventType();
            String flowId = flowKey.getFlowId();
            PerFlowStaticEventTapConfig config = entry.getValue();
            Set<String> uris = config.getUris();
            QosDelivery qosDelivery = config.getQosDelivery();
            for (String uriString : uris) {
                URI uri = safeUriFromString(uriString);
                TapSpec tapSpec = new TapSpec(eventType, flowId, uri, qosDelivery);

                tapSpecBuilder.add(tapSpec);
            }
        }

        return tapSpecBuilder.build();
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
        return format("%s{%s}", eventType, flowId);
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

    static class TapSpec
    {
        private String eventType;
        private String flowId;
        private URI uri;
        private QosDelivery qosDelivery;

        public TapSpec(String eventType, String flowId, URI uri, QosDelivery qosDelivery)
        {
            this.eventType = eventType;
            this.flowId = flowId;
            this.uri = uri;
            this.qosDelivery = qosDelivery;
        }

        public String getEventType()
        {
            return eventType;
        }

        public String getFlowId()
        {
            return flowId;
        }

        public URI getUri()
        {
            return uri;
        }

        public QosDelivery getQosDelivery()
        {
            return qosDelivery;
        }
    }
}
