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
package com.proofpoint.event.combiner;

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
