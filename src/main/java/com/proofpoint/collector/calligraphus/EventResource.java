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
package com.proofpoint.collector.calligraphus;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;

@Path("/v2/event")
public class EventResource
{
    private final EventWriter writer;

    @Inject
    public EventResource(EventWriter writer)
    {
        Preconditions.checkNotNull(writer, "writer must not be null");
        this.writer = writer;
    }

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    public Response post(List<Event> events)
            throws IOException
    {
        for (Event event : events) {
            writer.write(event);
        }
        return Response.status(Response.Status.ACCEPTED).build();
    }
}
