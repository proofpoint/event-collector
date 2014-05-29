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

import com.proofpoint.configuration.Config;
import com.proofpoint.configuration.ConfigDescription;
import com.proofpoint.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class EventTapConfig
{
    private int eventTapThreads = 20;
    private Duration eventTapRefreshDuration = new Duration(10, TimeUnit.SECONDS);
    private int eventTapQosRetryCount = 10;
    private Duration eventTapQosRetryDelay = new Duration(30, TimeUnit.SECONDS);
    private boolean allowHttpConsumers = true;

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
    @ConfigDescription("The interval between updates to event tap configuration from published discovery information.")
    public EventTapConfig setEventTapRefreshDuration(Duration eventTapRefreshDuration)
    {
        this.eventTapRefreshDuration = eventTapRefreshDuration;
        return this;
    }

    @Min(0)
    public int getEventTapQosRetryCount()
    {
        return eventTapQosRetryCount;
    }

    @Config("collector.event-tap.qos-retry-count")
    @ConfigDescription("The number of times to retry failed messages for QoS taps.")
    public EventTapConfig setEventTapQosRetryCount(int eventTapQosRetryCount)
    {
        this.eventTapQosRetryCount = eventTapQosRetryCount;
        return this;
    }

    @NotNull
    public Duration getEventTapQosRetryDelay()
    {
        return eventTapQosRetryDelay;
    }

    @Config("collector.event-tap.qos-retry-delay")
    @ConfigDescription("The interval between attempts to resend failed messages for QoS taps.")
    public EventTapConfig setEventTapQosRetryDelay(Duration eventTapQosRetryDelay)
    {
        this.eventTapQosRetryDelay = eventTapQosRetryDelay;
        return this;
    }

    public boolean isAllowHttpConsumers()
    {
        return allowHttpConsumers;
    }

    @Config("collector.event-tap.allow-http-consumers")
    @ConfigDescription("Indicate whether to ignore consumers that only announce as http")
    public EventTapConfig setAllowHttpConsumers(boolean allowHttpConsumers)
    {
        this.allowHttpConsumers = allowHttpConsumers;
        return this;
    }
}