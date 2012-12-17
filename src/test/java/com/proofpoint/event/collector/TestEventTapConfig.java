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

import com.google.common.collect.ImmutableMap;
import com.proofpoint.configuration.testing.ConfigAssertions;
import com.proofpoint.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.proofpoint.experimental.testing.ValidationAssertions.assertFailsValidation;

public class TestEventTapConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(EventTapConfig.class)
                .setEventTapThreads(20)
                .setEventTapRefreshDuration(new Duration(10, TimeUnit.SECONDS))
                .setMaxBatchSize(1000)
                .setQueueSize(40000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("collector.event-tap.threads", "3")
                .put("collector.event-tap.refresh", "30m")
                .put("collector.event-tap.batch-size-max", "17")
                .put("collector.event-tap.queue-size", "977")
                .build();

        EventTapConfig expected = new EventTapConfig()
                .setEventTapThreads(3)
                .setEventTapRefreshDuration(new Duration(30, TimeUnit.MINUTES))
                .setMaxBatchSize(17)
                .setQueueSize(977);

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testEventTapThreadsValidation()
    {
        assertFailsValidation(new EventTapConfig().setEventTapThreads(0), "eventTapThreads", "must be greater than or equal to 1", Min.class);
    }

    @Test
    void testEventTapRefreshDurationValidation()
    {
        assertFailsValidation(new EventTapConfig().setEventTapRefreshDuration(null), "eventTapRefreshDuration", "may not be null", NotNull.class);
    }

    @Test
    void testMaxBatchSizeValidation()
    {
        assertFailsValidation(new EventTapConfig().setMaxBatchSize(0), "maxBatchSize", "must be greater than or equal to 1", Min.class);
    }

    @Test
    void testQueueSizeValidation()
    {
        assertFailsValidation(new EventTapConfig().setQueueSize(0), "queueSize", "must be greater than or equal to 1", Min.class);
    }
}
