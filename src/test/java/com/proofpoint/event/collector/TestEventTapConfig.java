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
