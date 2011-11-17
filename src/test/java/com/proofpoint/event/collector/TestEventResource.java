/*
 * Copyright 2010 Proofpoint, Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.UUID;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestEventResource
{
    private EventResource resource;
    private InMemoryEventWriter writer;

    @BeforeMethod
    public void setup()
    {
        writer = new InMemoryEventWriter();
        resource = new EventResource(writer);
    }

    @Test
    public void testPost()
            throws IOException
    {
        ImmutableMap<String, String> data = ImmutableMap.of("foo", "bar", "hello", "world");
        Event event = new Event("test", UUID.randomUUID().toString(), "test.local", new DateTime(), data);

        Response response = resource.post(ImmutableList.of(event));

        assertEquals(response.getStatus(), Response.Status.ACCEPTED.getStatusCode());
        assertNull(response.getEntity());
        assertNull(response.getMetadata().get("Content-Type")); // content type is set by jersey based on @Produces

        assertEquals(writer.getEvents(), ImmutableList.of(event));
    }
}
