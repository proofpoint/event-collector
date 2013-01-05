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

import com.proofpoint.event.collector.EventTapFlow.Observer;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.units.Duration;

import javax.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.proofpoint.event.collector.EventTapFlow.NULL_OBSERVER;

public class HttpEventTapFlowFactory implements EventTapFlowFactory
{
    private final HttpClient httpClient;
    private final JsonCodec<List<Event>> eventsCodec;
    private final int qosRetryCount;
    private final Duration qosRetryDelay;

    @Inject
    public HttpEventTapFlowFactory(@EventTap HttpClient httpClient, JsonCodec<List<Event>> eventsCodec,
            EventTapConfig config)
    {
        this.httpClient = checkNotNull(httpClient, "httpClient is null");
        this.eventsCodec = checkNotNull(eventsCodec, "eventCodec is null");
        this.qosRetryCount = checkNotNull(config, "config is null").getEventTapQosRetryCount();
        this.qosRetryDelay = config.getEventTapQosRetryDelay();
    }

    @Override
    public EventTapFlow createEventTapFlow(String eventType, String flowId, Set<URI> taps, Observer observer)
    {
        return new HttpEventTapFlow(httpClient, eventsCodec, eventType, flowId, taps, 0, null, observer);
    }

    @Override
    public EventTapFlow createEventTapFlow(String eventType, String flowId, Set<URI> taps)
    {
        return new HttpEventTapFlow(httpClient, eventsCodec, eventType, flowId, taps, 0, null, NULL_OBSERVER);
    }

    @Override
    public EventTapFlow createQosEventTapFlow(String eventType, String flowId, Set<URI> taps, Observer observer)
    {
        return new HttpEventTapFlow(httpClient, eventsCodec, eventType, flowId, taps, qosRetryCount, qosRetryDelay, observer);
    }

    @Override
    public EventTapFlow createQosEventTapFlow(String eventType, String flowId, Set<URI> taps)
    {
        return new HttpEventTapFlow(httpClient, eventsCodec, eventType, flowId, taps, qosRetryCount, qosRetryDelay, NULL_OBSERVER);
    }
}
