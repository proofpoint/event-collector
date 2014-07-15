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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.proofpoint.event.client.EventClient;
import com.proofpoint.event.client.JsonEventSerializer;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class LocalEventClient
        implements EventClient
{
    private final Set<EventWriter> eventWriters;
    private final JsonEventSerializer eventSerializer;
    private final ObjectMapper objectMapper;

    @Inject
    public LocalEventClient(Set<EventWriter> eventWriters, JsonEventSerializer eventSerializer, ObjectMapper objectMapper)
    {
        this.eventWriters = checkNotNull(eventWriters, "eventWriters");
        this.eventSerializer = checkNotNull(eventSerializer, "eventSerializer");
        this.objectMapper = checkNotNull(objectMapper, "objectMapper");
    }

    @Override
    public <T> ListenableFuture<Void> post(T... event)
    {
        checkNotNull(event, "event");
        return post(Arrays.asList(event));
    }

    @Override
    public <T> ListenableFuture<Void> post(final Iterable<T> events)
    {
        checkNotNull(events, "events");
        return post(new EventGenerator<T>()
        {
            @Override
            public void generate(EventPoster<T> eventPoster)
                    throws IOException
            {
                for (T event : events) {
                    eventPoster.post(event);
                }
            }
        });
    }

    @Override
    public <T> ListenableFuture<Void> post(EventGenerator<T> eventGenerator)
    {
        checkNotNull(eventGenerator, "eventGenerator");
        try {
            eventGenerator.generate(new EventPoster<T>()
            {
                @Override
                public void post(T event)
                        throws IOException
                {
                    checkNotNull(event, "event");
                    for (EventWriter eventWriter : eventWriters) {
                        eventWriter.write(serializeEvent(event));
                    }
                }
            });
        }
        catch (IOException e) {
            return Futures.immediateFailedFuture(new RuntimeException(e));
        }
        return Futures.immediateFuture(null);
    }

    private <T> Event serializeEvent(T event)
            throws IOException
    {
        TokenBuffer tokenBuffer = new TokenBuffer(objectMapper);
        eventSerializer.serialize(event, null, tokenBuffer);
        return objectMapper.readValue(tokenBuffer.asParser(), Event.class);
    }
}
