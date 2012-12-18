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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceType;
import com.proofpoint.event.collector.BatchProcessor.BatchHandler;
import com.proofpoint.event.collector.EventCounters.CounterState;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.log.Logger;
import com.proofpoint.units.Duration;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;

public class EventTapWriter implements EventWriter, BatchHandler<Event>, EventTapStats
{
    private static final Logger log = Logger.get(EventTapWriter.class);
    private final ServiceSelector selector;
    private final HttpClient httpClient;
    private final JsonCodec<List<Event>> eventsCodec;
    private final ScheduledExecutorService executorService;
    private final BatchProcessorFactory batchProcessorFactory;

    private final AtomicReference<Multimap<String, EventTapFlow>> eventFlows = new AtomicReference<Multimap<String, EventTapFlow>>(ImmutableMultimap.<String, EventTapFlow>of());
    private ScheduledFuture<?> refreshJob;
    private final Duration flowRefreshDuration;

    private final ConcurrentMap<String, BatchProcessor<Event>> processors = new ConcurrentHashMap<String, BatchProcessor<Event>>();

    private final EventCounters<List<String>> flowCounters = new EventCounters<List<String>>();

    @Inject
    public EventTapWriter(@ServiceType("eventTap") ServiceSelector selector,
            @EventTap HttpClient httpClient,
            JsonCodec<List<Event>> eventsCodec,
            @EventTap ScheduledExecutorService executorService,
            BatchProcessorFactory batchProcessorFactory,
            EventTapConfig config)
    {
        this.selector = checkNotNull(selector, "selector is null");
        this.httpClient = checkNotNull(httpClient, "httpClient is null");
        this.eventsCodec = checkNotNull(eventsCodec, "eventsCodec is null");
        this.executorService = checkNotNull(executorService, "executorService is null");
        this.flowRefreshDuration = checkNotNull(config, "config is null").getEventTapRefreshDuration();
        this.batchProcessorFactory = checkNotNull(batchProcessorFactory, "batchProcessorFactory is null");
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
        for (BatchProcessor<Event> processor : processors.values()) {
            processor.stop();
        }
        processors.clear();

        if (refreshJob != null) {
            refreshJob.cancel(false);
            refreshJob = null;
        }
    }

    @Managed
    public void refreshFlows()
    {
        Multimap<List<String>, URI> flows = ArrayListMultimap.create();
        List<ServiceDescriptor> descriptors = selector.selectAllServices();
        Set<String> eventTypes = new HashSet<String>();         // Event types that have listeners

        for (ServiceDescriptor descriptor : descriptors) {
            String eventType = descriptor.getProperties().get("eventType");
            if (eventType == null) {
                continue;
            }
            String flowId = descriptor.getProperties().get("tapId");
            if (flowId == null) {
                continue;
            }
            URI uri;
            try {
                uri = URI.create(descriptor.getProperties().get("http"));
            }
            catch (Exception e) {
                continue;
            }
            flows.put(ImmutableList.of(eventType, flowId), uri);
            // Save the event types for later to avoid accessing the (concurrent)
            // processors map more to only once per event type.
            eventTypes.add(eventType);
        }

        // By removing the processors that are no longer required first, the
        // existing flows in eventFlow will stop being used before they are
        // destroyed (because any event received for a non-existing processor
        // will be dropped).
        Set<String> oldEventTypes = new HashSet<String>();
        for (String eventType : processors.keySet()) {
            if (!eventTypes.contains(eventType)) {
                oldEventTypes.add(eventType);
            }
        }
        for (String eventType : oldEventTypes) {
            log.debug("Cleaning up resources for event type '%s': no more event taps for type", eventType);
            BatchProcessor<Event> processor = processors.remove(eventType);
            processor.stop();
        }

        ImmutableListMultimap.Builder<String, EventTapFlow> builder = ImmutableListMultimap.builder();
        for (Entry<List<String>, Collection<URI>> entry : flows.asMap().entrySet()) {
            final String eventType = entry.getKey().get(0);
            final String flowId = entry.getKey().get(1);
            Set<URI> taps = ImmutableSet.copyOf(entry.getValue());

            builder.put(eventType, new EventTapFlow(httpClient, eventsCodec, eventType, flowId, taps,
                    new EventTapFlow.Observer()
                    {
                        @Override
                        public void onRecordsSent(URI uri, int count)
                        {
                            flowCounters.recordReceived(createCounterKey(uri), count);
                        }

                        @Override
                        public void onRecordsLost(URI uri, int count)
                        {
                            flowCounters.recordLost(createCounterKey(uri), count);
                        }

                        private List<String> createCounterKey(URI uri)
                        {
                            return ImmutableList.of(eventType, flowId, uri.toString());
                        }
                    }));
        }
        eventFlows.set(builder.build());

        // By creating new processors after the event flows have been updated,
        // the new flow will be available for the processor when it needs it.
        for (String eventType : eventTypes) {
            if (!processors.containsKey(eventType)) {
                log.debug("Creating resources for event type '%s': new event taps for type", eventType);
                BatchProcessor<Event> batchProcessor = batchProcessorFactory.createBatchProcessor(this, eventType);
                processors.put(eventType, batchProcessor);
                batchProcessor.start();
            }
        }
    }

    @Override
    public void write(Event event)
    {
        BatchProcessor<Event> processor = processors.get(event.getType());
        if (processor != null) {
            processor.put(event);
        }
    }

    @Override
    public void processBatch(List<Event> events)
    {
        Multimap<String, EventTapFlow> eventFlows = this.eventFlows.get();
        if (eventFlows.isEmpty()) {
            return;
        }

        Collection<EventTapFlow> currentFlows = eventFlows.get(events.iterator().next().getType());
        for (EventTapFlow flow : currentFlows) {
            flow.processBatch(ImmutableList.copyOf(events));
        }
    }

    @Override
    public Map<String, CounterState> getQueueCounters()
    {
        ImmutableMap.Builder<String, CounterState> counters = ImmutableMap.builder();
        for (Entry<String, BatchProcessor<Event>> entry : processors.entrySet()) {
            counters.put(entry.getKey(), entry.getValue().getCounterState());
        }
        return counters.build();
    }

    @Override
    public void resetQueueCounters()
    {
        for (BatchProcessor<Event> processor : processors.values()) {
            processor.resetCounter();
        }
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
}
