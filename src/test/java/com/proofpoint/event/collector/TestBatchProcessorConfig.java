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

import com.google.common.collect.ImmutableMap;
import com.proofpoint.configuration.testing.ConfigAssertions;
import org.apache.bval.constraints.NotEmpty;
import org.testng.annotations.Test;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Map;

import static com.proofpoint.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.proofpoint.configuration.testing.ConfigAssertions.recordDefaults;
import static com.proofpoint.testing.ValidationAssertions.assertFailsValidation;

public class TestBatchProcessorConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(BatchProcessorConfig.class)
                .setMaxBatchSize(1000)
                        .setQueueSize(250000)
                        .setDataDirectory(".")
        );
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("collector.event-tap.batch-size-max", "17")
                .put("collector.event-tap.queue-size", "977")
                .put("collector.event-tap.data-directory", "data")
                .build();

        BatchProcessorConfig expected = new BatchProcessorConfig()
                .setMaxBatchSize(17)
                .setQueueSize(977)
                .setDataDirectory("data");

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    void testMaxBatchSizeValidation()
    {
        assertFailsValidation(new BatchProcessorConfig().setMaxBatchSize(0), "maxBatchSize", "must be greater than or equal to 1", Min.class);
    }

    @Test
    void testQueueSizeValidation()
    {
        assertFailsValidation(new BatchProcessorConfig().setQueueSize(0), "queueSize", "must be greater than or equal to 1", Min.class);
    }

    @Test
    void testDataDirectoryValidation()
    {
        assertFailsValidation(new BatchProcessorConfig().setDataDirectory(""), "dataDirectory", "may not be empty", NotEmpty.class);
        assertFailsValidation(new BatchProcessorConfig().setDataDirectory(null), "dataDirectory", "may not be null", NotNull.class);
    }
}
