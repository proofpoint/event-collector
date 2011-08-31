package com.proofpoint.collector.calligraphus;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import javax.validation.constraints.NotNull;
import java.util.List;

public class InMemoryEventWriter implements EventWriter
{
    List<Event> events = Lists.newArrayList();

    @Override
    public void write(@NotNull Event event)
    {
        Preconditions.checkNotNull(event, "event must not be null");

        events.add(event);
    }

    public List<Event> getEvents()
    {
        return ImmutableList.copyOf(events);
    }
}
