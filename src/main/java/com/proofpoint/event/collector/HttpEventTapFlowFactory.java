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

import javax.inject.Inject;
import java.net.URI;
import java.util.List;
import java.util.Set;

import static com.proofpoint.event.collector.EventTapFlow.NULL_OBSERVER;

public class HttpEventTapFlowFactory implements EventTapFlowFactory
{
    private final HttpClient httpClient;
    private final JsonCodec<List<Event>> eventsCodec;

    @Inject
    public HttpEventTapFlowFactory(@EventTap HttpClient httpClient, JsonCodec<List<Event>> eventsCodec)
    {
        this.httpClient = httpClient;
        this.eventsCodec = eventsCodec;
    }

    @Override
    public EventTapFlow createEventTapFlow(String eventType, String flowId, Set<URI> taps, Observer observer)
    {
        return new HttpEventTapFlow(httpClient, eventsCodec, eventType, flowId, taps, observer);
    }

    @Override
    public EventTapFlow createEventTapFlow(String eventType, String flowId, Set<URI> taps)
    {
        return new HttpEventTapFlow(httpClient, eventsCodec, eventType, flowId, taps, NULL_OBSERVER);
    }

    @Override
    public EventTapFlow createQosEventTapFlow(String eventType, String flowId, Set<URI> taps, Observer observer)
    {
        return new HttpEventTapFlow(httpClient, eventsCodec, eventType, flowId, taps, observer);
    }

    @Override
    public EventTapFlow createQosEventTapFlow(String eventType, String flowId, Set<URI> taps)
    {
        return new HttpEventTapFlow(httpClient, eventsCodec, eventType, flowId, taps, NULL_OBSERVER);
    }
}
