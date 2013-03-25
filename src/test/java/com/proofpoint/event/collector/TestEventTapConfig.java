/*
 * Copyright 2011-2013 Proofpoint, Inc.
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

import static com.proofpoint.testing.ValidationAssertions.assertFailsValidation;
import static com.proofpoint.testing.ValidationAssertions.assertValidates;

public class TestEventTapConfig
{
    @Test
    public void testDefaults()
    {
        @SuppressWarnings("deprecation")
        EventTapConfig recordedDefaults = ConfigAssertions.recordDefaults(EventTapConfig.class)
                .setEventTapThreads(20)
                .setEventTapRefreshDuration(new Duration(10, TimeUnit.SECONDS))
                .setEventTapQosRetryCount(10)
                .setEventTapQosRetryDelay(new Duration(30, TimeUnit.SECONDS));

        ConfigAssertions.assertRecordedDefaults(recordedDefaults);
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("collector.event-tap.threads", "3")
                .put("collector.event-tap.refresh", "30m")
                .put("collector.event-tap.qos-retry-count", "17")
                .put("collector.event-tap.qos-retry-delay", "1h")
                .build();

        @SuppressWarnings("deprecation")
        EventTapConfig expected = new EventTapConfig()
                .setEventTapThreads(3)
                .setEventTapQosRetryDelay(new Duration(1, TimeUnit.HOURS))
                .setEventTapQosRetryCount(17)
                .setEventTapRefreshDuration(new Duration(30, TimeUnit.MINUTES));

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testValidation()
    {
        @SuppressWarnings("deprecation")
        EventTapConfig config = new EventTapConfig()
                .setEventTapThreads(0)
                .setEventTapRefreshDuration(null)
                .setEventTapQosRetryCount(-1)
                .setEventTapQosRetryDelay(null);

        assertFailsValidation(config, "eventTapThreads", "must be greater than or equal to 1", Min.class);
        assertFailsValidation(config, "eventTapRefreshDuration", "may not be null", NotNull.class);
        assertFailsValidation(config, "eventTapQosRetryCount", "must be greater than or equal to 0", Min.class);
        assertFailsValidation(config, "eventTapQosRetryDelay", "may not be null", NotNull.class);
        assertValidates(new EventTapConfig()
                .setEventTapRefreshDuration(new Duration(1, TimeUnit.SECONDS))
                .setEventTapQosRetryCount(0)
                .setEventTapQosRetryDelay(new Duration(1, TimeUnit.DAYS)));
    }
}
