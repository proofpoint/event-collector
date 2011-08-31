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
import com.proofpoint.configuration.Config;
import com.proofpoint.configuration.ConfigDescription;
import com.proofpoint.units.Duration;

import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class ServerConfig
{
    private Duration maxBufferTime = new Duration(1, TimeUnit.MINUTES);

    @Config("collector.max-buffer-time")
    @ConfigDescription("maximum length of time to buffer events locally before persisting them")
    public ServerConfig setMaxBufferTime(Duration maxBufferTime)
    {
        Preconditions.checkNotNull(maxBufferTime, "maxBufferTime must not be null");
        Preconditions.checkArgument(maxBufferTime.toMillis() >= 1000, "maxBufferTime must be at least 1 second");
        this.maxBufferTime = maxBufferTime;
        return this;
    }

    @NotNull
    public Duration getMaxBufferTime()
    {
        return maxBufferTime;
    }
}
