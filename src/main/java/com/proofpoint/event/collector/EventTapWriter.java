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
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceType;
import com.proofpoint.event.collector.EventTapWriter.EventTypePolicy.FlowPolicy;
import com.proofpoint.event.collector.EventTapWriter.FlowInfo.Builder;
import com.proofpoint.event.collector.StaticEventTapConfig.FlowKey;
import com.proofpoint.event.collector.queue.Queue;
import com.proofpoint.event.collector.queue.QueueFactory;
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

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;
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
    private static final EventTypePolicy NULL_EVENT_TYPE_POLICY = new EventTypePolicy(null);
    private final ServiceSelector selector;
    private final ScheduledExecutorService executorService;
    private final BatchProcessorFactory batchProcessorFactory;
    private final EventTapFlowFactory eventTapFlowFactory;
    private final boolean allowHttpConsumers;

    private final AtomicReference<Map<String, EventTypePolicy>> eventTypePolicies = new AtomicReference<Map<String, EventTypePolicy>>(
            ImmutableMap.<String, EventTypePolicy>of());
    private final List<TapSpec> staticTapSpecs;
    private Table<String, String, FlowInfo> flows = ImmutableTable.of();

    private ScheduledFuture<?> refreshJob;
    private final Duration flowRefreshDuration;
    private final QueueFactory queueFactory;

    @Inject
    public EventTapWriter(@ServiceType("eventTap") ServiceSelector selector,
            @EventTap ScheduledExecutorService executorService,
            BatchProcessorFactory batchProcessorFactory,
            EventTapFlowFactory eventTapFlowFactory,
            EventTapConfig config,
            StaticEventTapConfig staticEventTapConfig,
            QueueFactory queueFactory)
    {
        this.staticTapSpecs = createTapSpecFromConfig(checkNotNull(staticEventTapConfig, "staticEventTapConfig is null"));
        this.selector = checkNotNull(selector, "selector is null");
        this.executorService = checkNotNull(executorService, "executorService is null");
        this.flowRefreshDuration = checkNotNull(config, "config is null").getEventTapRefreshDuration();
        this.allowHttpConsumers = config.isAllowHttpConsumers();
        this.batchProcessorFactory = checkNotNull(batchProcessorFactory, "batchProcessorFactory is null");
        this.eventTapFlowFactory = checkNotNull(eventTapFlowFactory, "eventTapFlowFactory is null");
        this.queueFactory = checkNotNull(queueFactory, "queueFactory is null");
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
            for (Map.Entry<String, FlowPolicy> flowPolicyEntry : eventTypePolicy.getFlowPolicies().entrySet()) {
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
            Table<String, String, FlowInfo> existingFlows = flows;

            Table<String, String, FlowInfo> newFlows = constructFlowInfoFromTapSpec(Iterables.concat(staticTapSpecs, createTapSpecFromDiscovery(selector.selectAllServices())));
            if (existingFlows.equals(newFlows)) { // TODO: equals() doesn't work properly because FlowInfo#equals is not implemented
                return;
            }

            Map<String, EventTypePolicy> existingPolicies = eventTypePolicies.get();
            ImmutableMap.Builder<String, EventTypePolicy> policiesBuilder = ImmutableMap.builder();
            for (Map.Entry<String, Map<String, FlowInfo>> entry : newFlows.rowMap().entrySet()) {
                String eventType = entry.getKey();
                EventTypePolicy existingPolicy = firstNonNull(existingPolicies.get(eventType), NULL_EVENT_TYPE_POLICY);
                policiesBuilder.put(eventType, constructPolicyForFlows(existingPolicy, eventType, entry.getValue()));
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
        for (FlowPolicy flowPolicy : getPolicyForEvent(event).getFlowPolicies().values()) {
            flowPolicy.processor.put(event);
        }
    }

    @Override
    public void distribute(Event event)
            throws IOException
    {
        write(event);
    }

    /**
     * @param tapSpecs tap specs to construct the flow info from
     * @return a table with eventTypes as rows, flowIds as columns and FlowInfo as cell values; FlowInfo instances are constructed based on the given tap specs
     */
    private Table<String, String, FlowInfo> constructFlowInfoFromTapSpec(Iterable<TapSpec> tapSpecs)
    {
        Table<String, String, FlowInfo.Builder> flows = HashBasedTable.create();

        for (TapSpec tapSpec : tapSpecs) {
            String eventType = tapSpec.getEventType();
            String flowId = tapSpec.getFlowId();

            URI uri = tapSpec.getUri();

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

        for (Cell<String, String, Builder> cell : flows.cellSet()) {
            flowsBuilder.put(cell.getRowKey(), cell.getColumnKey(), cell.getValue().build());
        }

        return flowsBuilder.build();
    }

    /**
     * @param existingPolicy existing policy for the given event type
     * @param eventType event type
     * @param flows new flows registered with the given event type; map between flowIds and FlowInfos
     * @return new event type policy
     */
    private EventTypePolicy constructPolicyForFlows(EventTypePolicy existingPolicy, String eventType, Map<String, FlowInfo> flows)
            throws IOException
    {
        log.debug("Constructing policy for %s", eventType);

        ImmutableMap.Builder<String, FlowPolicy> newPolicies = ImmutableMap.builder();
        for (Entry<String, FlowInfo> flowEntry : flows.entrySet()) {

            String flowId = flowEntry.getKey();
            FlowInfo updatedFlowInfo = flowEntry.getValue();
            log.debug("** considering flow ID %s", flowId);

            Set<URI> destinations = ImmutableSet.copyOf(updatedFlowInfo.destinations);
            FlowPolicy existingFlowPolicy = existingPolicy.getFlowPolicies().get(flowId);

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

                Queue<Event> queue = queueFactory.create(createBatchProcessorName(eventType, flowId));
                BatchProcessor<Event> batchProcessor = batchProcessorFactory.createBatchProcessor(createBatchProcessorName(eventType, flowId), eventTapFlow, queue);

                FlowPolicy flowPolicy = new FlowPolicy(batchProcessor, eventTapFlow, updatedFlowInfo.qosEnabled);
                newPolicies.put(flowId, flowPolicy);
                batchProcessor.start();
            }
            else if (!destinations.equals(existingFlowPolicy.eventTapFlow.getTaps())) {
                log.debug("**-> changing taps from %s to %s", existingFlowPolicy.eventTapFlow.getTaps(), destinations);
                existingFlowPolicy.eventTapFlow.setTaps(destinations);
                newPolicies.put(flowId, existingFlowPolicy);
            }
            else {
                log.debug("**-> keeping as is with destinations %s", existingFlowPolicy.eventTapFlow.getTaps());
                newPolicies.put(flowId, existingFlowPolicy);
            }
        }

        return new EventTypePolicy(newPolicies.build());
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
        for (Entry<String, FlowPolicy> flowPolicyEntry : existingPolicy.getFlowPolicies().entrySet()) {
            String flowId = flowPolicyEntry.getKey();
            FlowPolicy existingFlowPolicy = flowPolicyEntry.getValue();
            FlowPolicy newFlowPolicy = newPolicy.getFlowPolicies().get(flowId);

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

            if (isNullOrEmpty(eventType) || isNullOrEmpty(flowId) || uri == null) {
                continue;
            }

            tapSpecBuilder.add(
                    new TapSpec(eventType, flowId, uri, QosDelivery.fromString(properties.get(QOS_DELIVERY_PROPERTY_NAME)))
            );
        }

        return tapSpecBuilder.build();
    }

    private static List<TapSpec> createTapSpecFromConfig(StaticEventTapConfig staticEventTapConfig)
    {
        ImmutableList.Builder<TapSpec> tapSpecBuilder = ImmutableList.builder();
        for (Entry<FlowKey, PerFlowStaticEventTapConfig> entry : staticEventTapConfig.getStaticTaps().entrySet()) {
            FlowKey flowKey = entry.getKey();
            PerFlowStaticEventTapConfig config = entry.getValue();
            for (String uri : config.getUris()) {
                tapSpecBuilder.add(new TapSpec(flowKey.getEventType(), flowKey.getFlowId(), safeUriFromString(uri), config.getQosDelivery()));
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

    /**
     * Contains information associated with a flow (except the flowId).
     */
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

    /**
     * Contains a map between flowIds and FlowPolicies associated with them.
     */
    static class EventTypePolicy
    {
        // cache is used to support scenarios where a flow temporarily disappears from discovery
        // the primary reason for this is to support the switch over from OLD to NEW event indexers for re-clustering work by JAB team
        private final Cache<String, FlowPolicy> flowPolicies;

        private EventTypePolicy(Map<String, FlowPolicy> flowPolicies)
        {
            this.flowPolicies = CacheBuilder.newBuilder().expireAfterWrite(2, TimeUnit.HOURS).build();
            if (flowPolicies != null) {
                this.flowPolicies.putAll(flowPolicies);
            }
        }

        public static Builder builder()
        {
            return new Builder();
        }

        public Map<String, FlowPolicy> getFlowPolicies()
        {
            return flowPolicies.asMap();
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
    }

    /**
     * Describes a tap for a specific event type along with the tap's flow id, uri and QoS settings.
     */
    private static class TapSpec
    {
        private final String eventType;
        private final String flowId;
        private final URI uri;
        private final QosDelivery qosDelivery;

        public TapSpec(String eventType, String flowId, URI uri, QosDelivery qosDelivery)
        {
            this.eventType = checkNotNull(eventType, "eventType is null");
            this.flowId = checkNotNull(flowId, "flowId is null");
            this.uri = checkNotNull(uri, "uri is null");
            this.qosDelivery = checkNotNull(qosDelivery, "qosDelivery is null");
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
