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
import com.proofpoint.log.Logger;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class LocalEventClient
        implements EventClient
{
    private static final Logger logger = Logger.get(LocalEventClient.class);

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

    @SafeVarargs
    @Override
    public final <T> ListenableFuture<Void> post(T... event)
    {
        checkNotNull(event, "event");
        return post(Arrays.asList(event));
    }

    @Override
    public <T> ListenableFuture<Void> post(final Iterable<T> events)
    {
        checkNotNull(events, "events");
        Throwable throwable = null;
        for (T event : events) {
            for (EventWriter eventWriter : eventWriters) {
                try {
                    eventWriter.write(serializeEvent(event));
                }
                catch (IOException e) {
                    logger.debug("Failed to serialize event %s: %s", event, e.getMessage());
                    if (throwable == null) {
                        throwable = e;
                    }
                    else {
                        throwable.addSuppressed(e);
                    }
                }
            }
        }

        if (throwable != null) {
            return Futures.immediateFailedFuture(throwable);
        }

        return Futures.immediateFuture(null);
    }

    @Override
    public <T> ListenableFuture<Void> post(EventGenerator<T> eventGenerator)
    {
        throw new UnsupportedOperationException();
    }

    private <T> Event serializeEvent(T event)
            throws IOException
    {
        TokenBuffer tokenBuffer = new TokenBuffer(objectMapper, false);
        eventSerializer.serialize(event, null, tokenBuffer);
        return objectMapper.readValue(tokenBuffer.asParser(), Event.class);
    }
}
