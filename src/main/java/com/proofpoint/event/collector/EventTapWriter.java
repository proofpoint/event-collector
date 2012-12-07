/*
 * Copyright 2011 Proofpoint, Inc.
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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceType;
import com.proofpoint.event.collector.BatchProcessor.BatchHandler;
import com.proofpoint.event.collector.EventCounters.CounterState;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.JsonBodyGenerator;
import com.proofpoint.http.client.Request;
import com.proofpoint.http.client.RequestBuilder;
import com.proofpoint.http.client.Response;
import com.proofpoint.http.client.ResponseHandler;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.log.Logger;
import com.proofpoint.units.Duration;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class EventTapWriter implements EventWriter, BatchHandler<Event>, EventTapStats
{
    private static final Logger log = Logger.get(EventTapWriter.class);
    private static final Random RANDOM = new Random();

    private final ServiceSelector selector;
    private final HttpClient httpClient;
    private final JsonCodec<List<Event>> eventsCodec;
    private final ScheduledExecutorService executorService;

    private final AtomicReference<Multimap<String, EventFlow>> eventFlows = new AtomicReference<Multimap<String, EventFlow>>(ImmutableMultimap.<String, EventFlow>of());
    private ScheduledFuture<?> refreshJob;
    private final Duration flowRefreshDuration;

    private final int maxBatchSize;
    private final int queueSize;
    private final LoadingCache<String, BatchProcessor<Event>> processors = CacheBuilder.newBuilder().build(new CacheLoader<String, BatchProcessor<Event>>()
    {
        @Override
        public BatchProcessor<Event> load(String key)
                throws Exception
        {
            BatchProcessor<Event> processor = new BatchProcessor<Event>(key, EventTapWriter.this, maxBatchSize, queueSize);
            processor.start();
            return processor;
        }
    });

    private final EventCounters<List<String>> flowCounters = new EventCounters<List<String>>();

    @Inject
    public EventTapWriter(@ServiceType("eventTap") ServiceSelector selector,
            @EventTap HttpClient httpClient,
            JsonCodec<List<Event>> eventCodec,
            @EventTap ScheduledExecutorService executorService,
            EventTapConfig config)
    {
        this.maxBatchSize = checkNotNull(config, "config is null").getMaxBatchSize();
        this.queueSize = config.getQueueSize();
        this.selector = checkNotNull(selector, "selector is null");
        this.httpClient = checkNotNull(httpClient, "httpClient is null");
        this.eventsCodec = checkNotNull(eventCodec, "eventCodec is null");
        this.executorService = checkNotNull(executorService, "executorService is null");
        this.flowRefreshDuration = config.getEventTapRefreshDuration();
        refreshFlows();
    }

    @PostConstruct
    public synchronized void start()
    {
        // has the refresh job already been started
        if (refreshJob != null) {
            return;
        }

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
        for(BatchProcessor<Event> processor : processors.asMap().values()) {
            processor.stop();
        }

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
        }

        ImmutableListMultimap.Builder<String, EventFlow> builder = ImmutableListMultimap.builder();
        for (Entry<List<String>, Collection<URI>> entry : flows.asMap().entrySet()) {
            String eventType = entry.getKey().get(0);
            String flowId = entry.getKey().get(1);
            List<URI> taps = ImmutableList.copyOf(entry.getValue());
            builder.put(eventType, new EventFlow(eventType, flowId, taps));
        }
        eventFlows.set(builder.build());
    }

    @Override
    public void write(Event event)
    {
        processors.getUnchecked(event.getType()).put(event);
    }

    @Override
    public void processBatch(Collection<Event> events)
    {
        Multimap<String, EventFlow> eventFlows = this.eventFlows.get();
        if (eventFlows.isEmpty()) {
            return;
        }

        Collection<EventFlow> currentFlows = eventFlows.get(events.iterator().next().getType());
        for (EventFlow flow : currentFlows) {
            try {
                flow.sendEvents(ImmutableList.copyOf(events), flowCounters);
            }
            catch (Exception ignored) {
                // already logged
            }
        }
    }

    @Override
    public Map<String, CounterState> getQueueCounters()
    {
        ImmutableMap.Builder<String, CounterState> counters = ImmutableMap.builder();
        for(Entry<String, BatchProcessor<Event>> entry : processors.asMap().entrySet()) {
            counters.put(entry.getKey(), entry.getValue().getCounterState());
        }
        return counters.build();
    }

    @Override
    public void resetQueueCounters()
    {
        for(BatchProcessor<Event> processor : processors.asMap().values()) {
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

    private class EventFlow
    {
        private final String eventType;
        private final String flowId;
        private final List<URI> taps;
        private final EventCounters<URI> counters = new EventCounters<URI>();

        private EventFlow(String eventType, String flowId, List<URI> taps)
        {
            this.eventType = checkNotNull(eventType, "eventType is null");
            this.flowId = checkNotNull(flowId, "flowId is null");
            this.taps = checkNotNull(taps, "taps is null");
            checkArgument(!taps.isEmpty(), "taps is empty");
        }

        public Map<String, CounterState> getCounters()
        {
            return counters.getCounts();
        }

        public void sendEvents(final List<Event> value, final EventCounters<List<String>> counters)
                throws Exception
        {
            final URI uri = taps.get(RANDOM.nextInt(taps.size()));

            Request request = RequestBuilder.preparePost()
                    .setUri(uri)
                    .setHeader("Content-Type", "application/json")
                    .setBodyGenerator(JsonBodyGenerator.jsonBodyGenerator(eventsCodec, value))
                    .build();

            final List<String> counterKey = ImmutableList.of(eventType, flowId, uri.toString());

            httpClient.execute(request, new ResponseHandler<Void, Exception>()
            {
                @Override
                public Exception handleException(Request request, Exception exception)
                {
                    log.warn(exception, "Error posting %s events to flow %s at %s ", eventType, flowId, uri);
                    counters.recordLost(counterKey, value.size());
                    return exception;
                }

                @Override
                public Void handle(Request request, Response response)
                        throws Exception
                {
                    if (response.getStatusCode() / 100 != 2) {
                        log.warn("Error posting %s events to flow %s at %s: got response %s %s ", eventType, flowId, uri, response.getStatusCode(), response.getStatusMessage());
                        counters.recordLost(counterKey, value.size());
                    }
                    else {
                        log.debug("Posted %s events", value.size());
                        counters.recordReceived(counterKey, value.size());
                    }
                    return null;
                }
            });
        }
    }
}
