package com.proofpoint.collector.calligraphus;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.Immutable;

@Immutable
public class EventPartition
{
    private final String eventType;
    private final String timeBucket;

    public EventPartition(String eventType, String timeBucket)
    {
        Preconditions.checkNotNull(eventType, "eventType is null");
        Preconditions.checkNotNull(eventType, "timeBucket is null");

        this.eventType = eventType;
        this.timeBucket = timeBucket;
    }

    public String getEventType()
    {
        return eventType;
    }

    public String getTimeBucket()
    {
        return timeBucket;
    }

    @Override
    public String toString()
    {
        return eventType + "/" + timeBucket;
    }

    @SuppressWarnings("RedundantIfStatement")
    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        EventPartition that = (EventPartition) o;

        if (!eventType.equals(that.eventType)) {
            return false;
        }
        if (!timeBucket.equals(that.timeBucket)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = eventType.hashCode();
        result = 31 * result + timeBucket.hashCode();
        return result;
    }
}
