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

import com.google.common.collect.ImmutableMap;
import com.proofpoint.configuration.testing.ConfigAssertions;
import com.proofpoint.experimental.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

public class TestCombinerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(CombinerConfig.class)
                .setTargetFileSize(new DataSize(512, DataSize.Unit.MEGABYTE))
                .setAwsAccessKey(null)
                .setAwsSecretKey(null)
                .setS3StagingLocation(null)
                .setS3DataLocation(null)
                .setS3MetadataLocation(null)
                .setCombinerMaxDaysBack(14)
        );
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("combiner.target-file-size", "768MB")
                .put("combiner.aws-access-key", "my-access-key")
                .put("combiner.aws-secret-key", "my-secret-key")
                .put("combiner.s3-staging-location", "s3://example-staging/")
                .put("combiner.s3-data-location", "s3://example-data/")
                .put("combiner.s3-metadata-location", "s3://example-metadata/")
                .put("combiner.max-days-back", "10")
                .build();

        CombinerConfig expected = new CombinerConfig()
                .setTargetFileSize(new DataSize(768, DataSize.Unit.MEGABYTE))
                .setAwsAccessKey("my-access-key")
                .setAwsSecretKey("my-secret-key")
                .setS3StagingLocation("s3://example-staging/")
                .setS3DataLocation("s3://example-data/")
                .setS3MetadataLocation("s3://example-metadata/")
                .setCombinerMaxDaysBack(10);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
