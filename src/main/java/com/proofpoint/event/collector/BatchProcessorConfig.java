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
import com.proofpoint.configuration.ConfigDescription;

import javax.validation.constraints.Min;

public class BatchProcessorConfig
{
    private int maxBatchSize = 1000;
    private int queueSize = 40000;

    @Min(1)
    public int getMaxBatchSize()
    {
        return maxBatchSize;
    }

    @Config("collector.event-tap.batch-size-max")
    @ConfigDescription("The maximum number of events to include in a single batch posted to a given event tap.")
    public BatchProcessorConfig setMaxBatchSize(int maxBatchSize)
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
    @ConfigDescription("The maximum number of events queued for a given event type's queue.")
    public BatchProcessorConfig setQueueSize(int queueSize)
    {
        this.queueSize = queueSize;
        return this;
    }

}
