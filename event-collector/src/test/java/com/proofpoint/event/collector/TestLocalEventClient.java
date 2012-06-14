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

import com.google.common.collect.ImmutableSet;
import com.proofpoint.event.client.EventField;
import com.proofpoint.event.client.EventType;
import com.proofpoint.event.client.JsonEventSerializer;
import org.codehaus.jackson.map.ObjectMapper;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestLocalEventClient
{
    private InMemoryEventWriter eventWriter;
    private LocalEventClient eventClient;

    @BeforeMethod
    public void setup()
    {
        JsonEventSerializer eventSerializer = new JsonEventSerializer(TestEvent.class);
        ObjectMapper objectMapper = new ObjectMapper();

        eventWriter = new InMemoryEventWriter();
        ImmutableSet<EventWriter> eventWriters = ImmutableSet.of((EventWriter) eventWriter);

        eventClient = new LocalEventClient(eventWriters, eventSerializer, objectMapper);
    }

    @Test
    public void testLocalEventWriter()
    {
        eventClient.post(new TestEvent());

        assertEquals(eventWriter.getEvents().size(), 1);
        Event event = eventWriter.getEvents().get(0);

        assertEquals(event.getType(), "LocalTest");
        assertTrue(event.getData().containsKey("name"));
        assertEquals(event.getData().get("name"), "foo");
    }

    @EventType("LocalTest")
    private static class TestEvent
    {
        @EventField
        public String getName()
        {
            return "foo";
        }
    }
}
