/*
 * Copyright 2011-2012 Proofpoint, Inc.
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

import com.proofpoint.configuration.Config;
import com.proofpoint.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class EventTapConfig
{
    private int eventTapThreads = 20;
    private Duration eventTapRefreshDuration = new Duration(10, TimeUnit.SECONDS);
    private int maxBatchSize = 1000;
    private int queueSize = 40000;


    @Deprecated
    @Min(1)
    public int getEventTapThreads()
    {
        return eventTapThreads;
    }

    @Deprecated
    @Config("collector.event-tap.threads")
    public EventTapConfig setEventTapThreads(int eventTapThreads)
    {
        this.eventTapThreads = eventTapThreads;
        return this;
    }

    @NotNull
    public Duration getEventTapRefreshDuration()
    {
        return eventTapRefreshDuration;
    }

    @Config("collector.event-tap.refresh")
    public EventTapConfig setEventTapRefreshDuration(Duration eventTapRefreshDuration)
    {
        this.eventTapRefreshDuration = eventTapRefreshDuration;
        return this;
    }

    @Min(1)
    public int getMaxBatchSize()
    {
        return maxBatchSize;
    }

    @Config("collector.event-tap.batch-size-max")
    public EventTapConfig setMaxBatchSize(int maxBatchSize)
    {
        this.maxBatchSize = maxBatchSize;
        return this;
    }

    @Min(1)
    public int getQueueSize()
    {
        return queueSize;
    }

    @Config("collector.event-tap.queue-size")
    public EventTapConfig setQueueSize(int queueSize)
    {
        this.queueSize = queueSize;
        return this;
    }
}
