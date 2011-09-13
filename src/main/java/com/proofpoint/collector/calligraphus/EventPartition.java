package com.proofpoint.collector.calligraphus;

import com.google.common.base.Preconditions;

import javax.annotation.concurrent.Immutable;

@Immutable
public class EventPartition
{
    private final String eventType;
    private final String majorTimeBucket;
    private final String minorTimeBucket;

    public EventPartition(String eventType, String majorTimeBucket, String minorTimeBucket)
    {
        Preconditions.checkNotNull(eventType, "eventType is null");
        Preconditions.checkNotNull(majorTimeBucket, "majorTimeBucket is null");
        Preconditions.checkNotNull(minorTimeBucket, "minorTimeBucket is null");

        this.eventType = eventType;
        this.majorTimeBucket = majorTimeBucket;
        this.minorTimeBucket = minorTimeBucket;
    }

    public String getEventType()
    {
        return eventType;
    }

    public String getMajorTimeBucket()
    {
        return majorTimeBucket;
    }

    public String getMinorTimeBucket()
    {
        return minorTimeBucket;
    }

    @Override
    public String toString()
    {
        return eventType + "/" + majorTimeBucket + "/" + minorTimeBucket;
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
        if (!majorTimeBucket.equals(that.majorTimeBucket)) {
            return false;
        }
        if (!minorTimeBucket.equals(that.minorTimeBucket)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = eventType.hashCode();
        result = 31 * result + majorTimeBucket.hashCode();
        result = 31 * result + minorTimeBucket.hashCode();
        return result;
    }
}
