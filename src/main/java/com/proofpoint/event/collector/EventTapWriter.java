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
import com.google.common.collect.Table.Cell;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceType;
import com.proofpoint.event.collector.EventTapWriter.EventTypePolicy.FlowPolicy;
import com.proofpoint.event.collector.EventTapWriter.FlowInfo.Builder;
import com.proofpoint.event.collector.StaticEventTapConfig.FlowKey;
import com.proofpoint.event.collector.util.Clock;
import com.proofpoint.log.Logger;
import com.proofpoint.units.Duration;
import org.joda.time.DateTime;
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
    private static final EventTypePolicy NULL_EVENT_TYPE_POLICY = new EventTypePolicy(null);
    private final ServiceSelector selector;
    private final ScheduledExecutorService executorService;
    private final BatchProcessorFactory batchProcessorFactory;
    private final EventTapFlowFactory eventTapFlowFactory;
    private final boolean allowHttpConsumers;

    private final AtomicReference<Map<String, EventTypePolicy>> eventTypePolicies = new AtomicReference<Map<String, EventTypePolicy>>(
            ImmutableMap.<String, EventTypePolicy>of());
    private final List<TapSpec> staticTapSpecs;
    private final EventTapConfig eventTapConfig;
    private Table<String, String, FlowInfo> flows = ImmutableTable.of();

    private ScheduledFuture<?> refreshJob;
    private final Duration flowRefreshDuration;
    private final Clock clock;

    @Inject
    public EventTapWriter(@ServiceType("eventTap") ServiceSelector selector,
            @EventTap ScheduledExecutorService executorService,
            BatchProcessorFactory batchProcessorFactory,
            EventTapFlowFactory eventTapFlowFactory,
            EventTapConfig config,
            StaticEventTapConfig staticEventTapConfig,
            Clock clock)
    {
        this.staticTapSpecs = createTapSpecFromConfig(checkNotNull(staticEventTapConfig, "staticEventTapConfig is null"), checkNotNull(clock, "clock must not be null"));
        this.selector = checkNotNull(selector, "selector is null");
        this.executorService = checkNotNull(executorService, "executorService is null");

        this.eventTapConfig = checkNotNull(config, "config is null");
        this.flowRefreshDuration = config.getEventTapRefreshDuration();

        this.allowHttpConsumers = config.isAllowHttpConsumers();
        this.batchProcessorFactory = checkNotNull(batchProcessorFactory, "batchProcessorFactory is null");
        this.eventTapFlowFactory = checkNotNull(eventTapFlowFactory, "eventTapFlowFactory is null");
        this.clock = checkNotNull(clock, "clock must not be null");
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

    @VisibleForTesting
    Table<String, String, FlowInfo> getFlows()
    {
        return flows;
    }

    @Managed
    public void refreshFlows()
    {
        try {
            Map<String, EventTypePolicy> existingPolicies = eventTypePolicies.get();
            Table<String, String, FlowInfo> existingFlows = flows;
            ImmutableMap.Builder<String, EventTypePolicy> policiesBuilder = ImmutableMap.builder();

            Table<String, String, FlowInfo> newFlows = constructFlowInfoFromTapSpec(Iterables.concat(staticTapSpecs, createTapSpecFromDiscovery(flows, selector.selectAllServices())));
            if (existingFlows.equals(newFlows)) { // TODO: equals() doesn't work properly because FlowInfo#equals is not implemented
                return;
            }

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
            flowBuilder.setLastKnownToExist(tapSpec.getLastKnownToExist());
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

        // go through the existing flows and add FlowPolicy entries for missing flowIds
        ImmutableMap.Builder<String, FlowPolicy> newPolicies = ImmutableMap.builder();
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
                log.debug("  -> made flow for %s with destinations %s", eventType, destinations);

                String queueName = createBatchProcessorName(eventType, flowId);
                BatchProcessor<Event> batchProcessor = batchProcessorFactory.createBatchProcessor(createBatchProcessorName(eventType, flowId), eventTapFlow);

                FlowPolicy flowPolicy = new FlowPolicy(batchProcessor, eventTapFlow, updatedFlowInfo.qosEnabled);
                newPolicies.put(flowId, flowPolicy);

                log.info("Starting processor %s", queueName);
                batchProcessor.start();
            }
            else if (!destinations.equals(existingFlowPolicy.eventTapFlow.getTaps())) {
                log.debug("**-> changing taps for %s from %s to %s", eventType, existingFlowPolicy.eventTapFlow.getTaps(), destinations);
                existingFlowPolicy.eventTapFlow.setTaps(destinations);
                newPolicies.put(flowId, existingFlowPolicy);
            }
            else {
                log.debug("**-> keeping %s as is with destinations %s", eventType, existingFlowPolicy.eventTapFlow.getTaps());
                newPolicies.put(flowId, existingFlowPolicy);
            }
        }

        return new EventTypePolicy(newPolicies.build());
    }

    private void stopExistingPoliciesNoLongerInUse(Map<String, EventTypePolicy> existingPolicies, Map<String, EventTypePolicy> newPolicies)
            throws IOException
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
            throws IOException
    {
        for (Entry<String, FlowPolicy> flowPolicyEntry : existingPolicy.flowPolicies.entrySet()) {
            String flowId = flowPolicyEntry.getKey();
            FlowPolicy existingFlowPolicy = flowPolicyEntry.getValue();
            FlowPolicy newFlowPolicy = newPolicy.flowPolicies.get(flowId);

            if (newFlowPolicy == null || newFlowPolicy.processor != existingFlowPolicy.processor) {
                terminateQueue(eventType, flowId, existingFlowPolicy.processor);
            }
        }
    }

    private void terminateQueue(String eventType, String flowId, BatchProcessor<Event> processor)
            throws IOException
    {
        log.info("Stopping processor and terminating queue %s: no longer required", createBatchProcessorName(eventType, flowId));
        processor.stop();
        processor.terminateQueue();
    }

    private EventTypePolicy getPolicyForEvent(Event event)
    {
        return firstNonNull(eventTypePolicies.get().get(event.getType()), NULL_EVENT_TYPE_POLICY);
    }

    private void stopBatchProcessor(String eventType, String flowId, BatchProcessor<Event> processor)
    {
        log.info("Stopping processor %s: no longer required", createBatchProcessorName(eventType, flowId));
        processor.stop();
    }

    private List<TapSpec> createTapSpecFromDiscovery(Table<String, String, FlowInfo> flows, Iterable<ServiceDescriptor> descriptors)
    {
        ImmutableList.Builder<TapSpec> tapSpecBuilder = ImmutableList.builder();
        Table<String, String, FlowInfo> oldFlows = HashBasedTable.create(flows);
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

            TapSpec tapSpec = new TapSpec(eventType, flowId, uri, QosDelivery.fromString(properties.get(QOS_DELIVERY_PROPERTY_NAME)), clock.now());
            tapSpecBuilder.add(tapSpec);
            log.debug("Added EXISTING tapSpec: eventType=%s, flowId=%s, uri=%s, qosDelivery=%s", tapSpec.eventType, tapSpec.flowId, tapSpec.uri, tapSpec.qosDelivery);

            oldFlows.remove(eventType, flowId);
        }

        // if flow caching is enabled, then append old flows that haven't been missing from discovery long enough
        if (eventTapConfig.getEventTapCacheExpiration().toMillis() > 0) {
            for (Cell<String, String, FlowInfo> oldFlowCell : oldFlows.cellSet()) {
                FlowInfo flowInfo = oldFlowCell.getValue();
                if (isValidFlow(flowInfo)) {
                    for (URI destination : flowInfo.destinations) {
                        TapSpec tapSpec = new TapSpec(oldFlowCell.getRowKey(), oldFlowCell.getColumnKey(), destination, flowInfo.qosEnabled ? RETRY : BEST_EFFORT, flowInfo.lastKnownToExist);
                        tapSpecBuilder.add(tapSpec);
                        log.debug("Added OLD tapSpec which has not yet expired: eventType=%s, flowId=%s, uri=%s, qosDelivery=%s, lastKnownToExist=%s", tapSpec.eventType, tapSpec.flowId, tapSpec.uri, tapSpec.qosDelivery, tapSpec.lastKnownToExist);
                    }
                }
                else {
                    log.debug("Skipped tapSpec because it expired expired: eventType=%s, flowId=%s, qosDelivery=%s, lastKnownToExist=%s", oldFlowCell.getRowKey(), oldFlowCell.getColumnKey(), flowInfo.qosEnabled ? RETRY : BEST_EFFORT, flowInfo.lastKnownToExist);
                }
            }
        }

        return tapSpecBuilder.build();
    }

    private boolean isValidFlow(FlowInfo flowInfo)
    {
        long flowCacheMillis = eventTapConfig.getEventTapCacheExpiration().toMillis();
        return flowInfo.lastKnownToExist.plus(flowCacheMillis).isAfter(clock.now().toInstant());
    }

    private static List<TapSpec> createTapSpecFromConfig(StaticEventTapConfig staticEventTapConfig, Clock clock)
    {
        ImmutableList.Builder<TapSpec> tapSpecBuilder = ImmutableList.builder();
        for (Entry<FlowKey, PerFlowStaticEventTapConfig> entry : staticEventTapConfig.getStaticTaps().entrySet()) {
            FlowKey flowKey = entry.getKey();
            PerFlowStaticEventTapConfig config = entry.getValue();
            for (String uri : config.getUris()) {
                tapSpecBuilder.add(new TapSpec(flowKey.getEventType(), flowKey.getFlowId(), safeUriFromString(uri), config.getQosDelivery(), clock.now()));
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
        private final DateTime lastKnownToExist;

        private FlowInfo(boolean qosEnabled, Set<URI> destinations, DateTime lastKnownToExist)
        {
            this.qosEnabled = qosEnabled;
            this.destinations = ImmutableSet.copyOf(destinations);
            this.lastKnownToExist = checkNotNull(lastKnownToExist, "lastKnownToExist must not be null");
        }

        static Builder builder()
        {
            return new Builder();
        }

        static class Builder
        {
            private boolean qosEnabled = false;
            private ImmutableSet.Builder<URI> destinations = ImmutableSet.builder();
            private DateTime lastKnownToExist;

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

            public Builder setLastKnownToExist(DateTime lastKnownToExist)
            {
                this.lastKnownToExist = lastKnownToExist;
                return this;
            }

            public FlowInfo build()
            {
                return new FlowInfo(qosEnabled, destinations.build(), lastKnownToExist);
            }
        }
    }

    /**
     * Contains a map between flowIds and FlowPolicies associated with them.
     */
    static class EventTypePolicy
    {
        private final Map<String, FlowPolicy> flowPolicies;

        private EventTypePolicy(Map<String, FlowPolicy> flowPolicies)
        {
            this.flowPolicies = flowPolicies == null ? ImmutableMap.<String, FlowPolicy>of() : ImmutableMap.copyOf(flowPolicies);
        }

        public static class FlowPolicy
        {
            public final BatchProcessor<Event> processor;
            public final EventTapFlow eventTapFlow;
            public final boolean qosEnabled;

            public FlowPolicy(BatchProcessor<Event> processor, EventTapFlow eventTapFlow, boolean qosEnabled)
            {
                log.debug("Creating new FlowPolicy for ", eventTapFlow == null ? null : eventTapFlow.getTaps());
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
        private final DateTime lastKnownToExist;

        public TapSpec(String eventType, String flowId, URI uri, QosDelivery qosDelivery, DateTime lastKnownToExist)
        {
            this.eventType = checkNotNull(eventType, "eventType is null");
            this.flowId = checkNotNull(flowId, "flowId is null");
            this.uri = checkNotNull(uri, "uri is null");
            this.qosDelivery = checkNotNull(qosDelivery, "qosDelivery is null");
            this.lastKnownToExist = checkNotNull(lastKnownToExist, "lastKnownToExist must not be null");
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

        public DateTime getLastKnownToExist()
        {
            return lastKnownToExist;
        }
    }
}
