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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.List;

public class InMemoryEventWriter implements EventWriter
{
    List<Event> events = Lists.newArrayList();
    List<Event> distributedEvents = Lists.newArrayList();

    @Override
    public void write(Event event)
            throws IOException
    {
        this.events.add(event);
    }

    @Override
    public void distribute(Event event)
            throws IOException
    {
        this.distributedEvents.add(event);
    }

    public List<Event> getEvents()
    {
        return ImmutableList.copyOf(events);
    }

    public List<Event> getDistributedEvents()
    {
        return ImmutableList.copyOf(distributedEvents);
    }
}
