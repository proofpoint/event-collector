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

import com.proofpoint.configuration.Config;
import com.proofpoint.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class EventTapConfig
{
    private int eventTapThreads = 20;
    private Duration eventTapRefreshDuration = new Duration(10, TimeUnit.SECONDS);

    @Min(1)
    public int getEventTapThreads()
    {
        return eventTapThreads;
    }

    @Config("collector.event-tap-threads")
    public void setEventTapThreads(int eventTapThreads)
    {
        this.eventTapThreads = eventTapThreads;
    }

    @NotNull
    public Duration getEventTapRefreshDuration()
    {
        return eventTapRefreshDuration;
    }

    @Config("collector.event-tap-refresh")
    public void setEventTapRefreshDuration(Duration eventTapRefreshDuration)
    {
        this.eventTapRefreshDuration = eventTapRefreshDuration;
    }
}
