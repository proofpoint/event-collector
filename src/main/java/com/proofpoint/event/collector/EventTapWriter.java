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

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceType;
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

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class EventTapWriter implements EventWriter
{
    private static final Logger log = Logger.get(EventTapWriter.class);
    private static final Random RANDOM = new SecureRandom();

    private final ServiceSelector selector;
    private final HttpClient httpClient;
    private final JsonCodec<List<Event>> eventsCodec;
    private final ScheduledExecutorService executorService;

    private final AtomicReference<Multimap<String, EventFlow>> eventFlows = new AtomicReference<Multimap<String, EventFlow>>(ImmutableMultimap.<String, EventFlow>of());
    private ScheduledFuture<?> refreshJob;
    private final Duration flowRefreshDuration;

    @Inject
    public EventTapWriter(@ServiceType("eventTap") ServiceSelector selector,
            @EvenTap HttpClient httpClient,
            JsonCodec<List<Event>> eventCodec,
            @EvenTap ScheduledExecutorService executorService,
            EventTapConfig config)
    {
        Preconditions.checkNotNull(selector, "selector is null");
        Preconditions.checkNotNull(httpClient, "httpClient is null");
        Preconditions.checkNotNull(eventCodec, "eventCodec is null");
        Preconditions.checkNotNull(executorService, "executorService is null");
        Preconditions.checkNotNull(config, "config is null");

        this.selector = selector;
        this.httpClient = httpClient;
        this.eventsCodec = eventCodec;
        this.executorService = executorService;
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
        if (refreshJob != null) {
            refreshJob.cancel(false);
        }
        refreshJob = null;
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
    public void write(Iterable<Event> events)
            throws IOException
    {
        if (eventFlows.get().isEmpty()) {
            return;
        }

        ImmutableListMultimap<String, Event> eventsByType = Multimaps.index(events, new Function<Event, String>()
        {
            @Override
            public String apply(@Nullable Event event)
            {
                return event.getType();
            }
        });

        for (Entry<String, Collection<Event>> entry : eventsByType.asMap().entrySet()) {
            for (EventFlow flow : eventFlows.get().get(entry.getKey())) {
                try {
                    flow.sendEvents(ImmutableList.copyOf(entry.getValue()));
                }
                catch (Exception ignored) {
                    // already logged
                }
            }
        }
    }

    private class EventFlow
    {
        private final String eventType;
        private final String flowId;
        private final List<URI> taps;

        private EventFlow(String eventType, String flowId, List<URI> taps)
        {
            Preconditions.checkNotNull(eventType, "eventType is null");
            Preconditions.checkNotNull(flowId, "flowId is null");
            Preconditions.checkNotNull(taps, "taps is null");
            Preconditions.checkArgument(!taps.isEmpty(), "taps is empty");

            this.eventType = eventType;
            this.flowId = flowId;
            this.taps = taps;
        }

        public void sendEvents(final List<Event> value)
                throws Exception
        {
            final URI uri = taps.get(RANDOM.nextInt(taps.size()));

            Request request = RequestBuilder.preparePost()
                    .setUri(uri)
                    .setHeader("Content-Type", "application/json")
                    .setBodyGenerator(JsonBodyGenerator.jsonBodyGenerator(eventsCodec, value))
                    .build();
            httpClient.execute(request, new ResponseHandler<Void, Exception>()
            {
                @Override
                public Exception handleException(Request request, Exception exception)
                {
                    log.warn(exception, "Error posting %s events to flow %s at %s ", eventType, flowId, uri);
                    return exception;
                }

                @Override
                public Void handle(Request request, Response response)
                        throws Exception
                {
                    if (response.getStatusCode() / 100 != 2) {
                        log.warn("Error posting %s events to flow %s at %s: got response %s %s ", eventType, flowId, uri, response.getStatusCode(), response.getStatusMessage());
                    }
                    else {
                        log.debug("Posted %s events", value.size());
                    }
                    return null;
                }
            });
        }
    }
}
