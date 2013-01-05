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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.Request;
import com.proofpoint.http.client.RequestBuilder;
import com.proofpoint.http.client.Response;
import com.proofpoint.http.client.ResponseHandler;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.log.Logger;

import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status.Family;
import java.net.URI;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.proofpoint.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static javax.ws.rs.core.Response.Status.Family.SUCCESSFUL;
import static javax.ws.rs.core.Response.Status.fromStatusCode;

class HttpEventTapFlow implements EventTapFlow
{
    private static final Logger log = Logger.get(HttpEventTapFlow.class);
    private static final Random RANDOM = new Random();
    private static final String QOS_HEADER = "X-Proofpoint-QoS";
    private static final String QOS_HEADER_FIRST_BATCH = "firstBatch";
    private static final String QOS_HEADER_DROPPED_ENTRIES = "droppedMessages=%d";

    private final HttpClient httpClient;
    private final JsonCodec<List<Event>> eventsCodec;
    private final String eventType;
    private final String flowId;
    private final AtomicReference<List<URI>> taps = new AtomicReference<List<URI>>(ImmutableList.<URI>of());
    private final Observer observer;
    private final Set<URI> unestablishedTaps = Sets.newSetFromMap(new MapMaker().<URI, Boolean>makeMap());
    private final AtomicLong droppedEntries = new AtomicLong(0);

    public HttpEventTapFlow(HttpClient httpClient, JsonCodec<List<Event>> eventsCodec,
            String eventType, String flowId, Set<URI> taps, Observer observer)
    {
        this.httpClient = checkNotNull(httpClient, "httpClient is null");
        this.eventsCodec = checkNotNull(eventsCodec, "eventsCodec is null");
        this.eventType = checkNotNull(eventType, "eventType is null");
        this.flowId = checkNotNull(flowId, "flowId is null");
        this.observer = checkNotNull(observer, "observer is null");
        setTaps(taps);
    }

    @Override
    public void processBatch(List<Event> entries)
    {
        try {
            sendEvents(entries);
        }
        catch (Exception ignored) {
            // already logged
        }
    }

    @Override
    public void notifyEntriesDropped(int count)
    {
        droppedEntries.getAndAdd(count);
    }

    @Override
    public Set<URI> getTaps()
    {
        return ImmutableSet.copyOf(taps.get());
    }

    @Override
    public void setTaps(Set<URI> taps)
    {
        checkNotNull(taps, "taps is null");
        checkArgument(!taps.isEmpty(), "taps is empty");

        List<URI> existingTaps = this.taps.getAndSet(ImmutableList.copyOf(taps));

        for (URI tap : taps) {
            if (!existingTaps.contains(tap)) {
                unestablishedTaps.add(tap);
            }
        }
    }

    private void sendEvents(final List<Event> entries)
            throws Exception
    {
        List<URI> taps = this.taps.get();
        final URI uri = taps.get(RANDOM.nextInt(taps.size()));

        RequestBuilder requestBuilder = RequestBuilder.preparePost()
                .setUri(uri)
                .setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON)
                .setBodyGenerator(jsonBodyGenerator(eventsCodec, entries));

        if (unestablishedTaps.remove(uri)) {
            requestBuilder.addHeader(QOS_HEADER, QOS_HEADER_FIRST_BATCH);
        }

        // If there are multiple taps sharing the same flow that cares about
        // dropped messages, they must coordinate what events were received
        // anyway, so only one (the first) needs to get the dropped count.
        final long count = droppedEntries.getAndSet(0);
        if (count > 0) {
            requestBuilder.addHeader(QOS_HEADER, String.format(QOS_HEADER_DROPPED_ENTRIES, count));
        }

        httpClient.execute(requestBuilder.build(), new ResponseHandler<Void, Exception>()
        {
            @Override
            public Exception handleException(Request request, Exception exception)
            {
                log.warn(exception, "Error posting %s events to flow %s at %s ", eventType, flowId, uri);
                droppedEntries.getAndAdd(count + entries.size());
                observer.onRecordsLost(uri, entries.size());
                return exception;
            }

            @Override
            public Void handle(Request request, Response response)
                    throws Exception
            {
                if (fromStatusCode(response.getStatusCode()).getFamily() != SUCCESSFUL) {
                    log.warn("Error posting %s events to flow %s at %s: got response %s %s ", eventType, flowId, uri, response.getStatusCode(), response.getStatusMessage());
                    droppedEntries.getAndAdd(count + entries.size());
                    observer.onRecordsLost(uri, entries.size());
                }
                else {
                    log.debug("Posted %s events", entries.size());
                    observer.onRecordsSent(uri, entries.size());
                }
                return null;
            }
        });
    }

    @VisibleForTesting
    String getEventType()
    {
        return eventType;
    }

    @VisibleForTesting
    String getFlowId()
    {
        return flowId;
    }
}
