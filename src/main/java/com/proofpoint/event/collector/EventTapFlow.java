/*
 * Copyright 2012 Proofpoint, Inc.
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

import com.proofpoint.event.collector.BatchProcessor.BatchHandler;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.JsonBodyGenerator;
import com.proofpoint.http.client.Request;
import com.proofpoint.http.client.RequestBuilder;
import com.proofpoint.http.client.Response;
import com.proofpoint.http.client.ResponseHandler;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.log.Logger;

import java.net.URI;
import java.util.List;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

class EventTapFlow implements BatchHandler<Event>
{
    private static final Random RANDOM = new Random();
    private static final Logger log = Logger.get(EventTapFlow.class);

    private final HttpClient httpClient;
    private final JsonCodec<List<Event>> eventsCodec;
    private final String eventType;
    private final String flowId;
    private final List<URI> taps;
    private final Observer observer;

    public EventTapFlow(HttpClient httpClient, JsonCodec<List<Event>> eventsCodec,
            String eventType, String flowId, List<URI> taps,
            Observer observer)
    {
        this.httpClient = checkNotNull(httpClient, "httpClient is null");
        this.eventsCodec = checkNotNull(eventsCodec, "eventsCodec is null");
        this.eventType = checkNotNull(eventType, "eventType is null");
        this.flowId = checkNotNull(flowId, "flowId is null");
        this.taps = checkNotNull(taps, "taps is null");
        checkArgument(!taps.isEmpty(), "taps is empty");
        this.observer = checkNotNull(observer, "observer is null");
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

    private void sendEvents(final List<Event> entries)
            throws Exception
    {
        final URI uri = taps.get(RANDOM.nextInt(taps.size()));

        Request request = RequestBuilder.preparePost()
                .setUri(uri)
                .setHeader("Content-Type", "application/json")
                .setBodyGenerator(JsonBodyGenerator.jsonBodyGenerator(eventsCodec, entries))
                .build();

        httpClient.execute(request, new ResponseHandler<Void, Exception>()
        {
            @Override
            public Exception handleException(Request request, Exception exception)
            {
                log.warn(exception, "Error posting %s events to flow %s at %s ", eventType, flowId, uri);
                observer.onRecordsLost(uri, entries.size());
                return exception;
            }

            @Override
            public Void handle(Request request, Response response)
                    throws Exception
            {
                if (response.getStatusCode() / 100 != 2) {
                    log.warn("Error posting %s events to flow %s at %s: got response %s %s ", eventType, flowId, uri, response.getStatusCode(), response.getStatusMessage());
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

    public interface Observer
    {
        void onRecordsSent(URI uri, int count);

        void onRecordsLost(URI uri, int count);
    }
}
