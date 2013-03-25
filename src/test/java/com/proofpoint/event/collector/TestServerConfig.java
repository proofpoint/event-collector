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
import com.google.common.collect.ImmutableSet;
import com.proofpoint.configuration.testing.ConfigAssertions;
import com.proofpoint.units.DataSize;
import com.proofpoint.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.proofpoint.testing.ValidationAssertions.assertFailsValidation;
import static com.proofpoint.testing.ValidationAssertions.assertValidates;

public class TestServerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(ServerConfig.class)
                .setAcceptedEventTypes("")
                .setMaxBufferTime(new Duration(1, TimeUnit.MINUTES))
                .setTargetFileSize(new DataSize(512, DataSize.Unit.MEGABYTE))
                .setLocalStagingDirectory(new File("staging"))
                .setCombinerMetadataDirectory(new File("combiner-metadata"))
                .setMaxUploadThreads(10)
                .setAwsAccessKey(null)
                .setAwsSecretKey(null)
                .setS3StagingLocation(null)
                .setS3DataLocation(null)
                .setS3MetadataLocation(null)
                .setCombinerEnabled(false)
                .setCombinerStartDaysAgo(14)
                .setCombinerEndDaysAgo(-1)
                .setCombinerDateRangeLimitDisabled(false)
                .setCombinerThreadCount(5)
                .setCombinerHighPriorityEventTypes("")
                .setCombinerLowPriorityEventTypes("")
                .setRetryPeriod(new Duration(5, TimeUnit.MINUTES))
                .setRetryDelay(new Duration(0, TimeUnit.MINUTES))
                .setCombinerGroupId("default")
        );
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("collector.accepted-event-types", "Foo,Bar")
                .put("collector.max-buffer-time", "2h")
                .put("collector.target-file-size", "768MB")
                .put("collector.local-staging-directory", "testdir")
                .put("collector.combiner.metadata-directory", "test-combiner")
                .put("collector.max-upload-threads", "42")
                .put("collector.aws-access-key", "my-access-key")
                .put("collector.aws-secret-key", "my-secret-key")
                .put("collector.s3-staging-location", "s3://example-staging/")
                .put("collector.s3-data-location", "s3://example-data/")
                .put("collector.s3-metadata-location", "s3://example-metadata/")
                .put("collector.combiner.enabled", "true")
                .put("collector.combiner.days-ago-to-start", "10")
                .put("collector.combiner.days-ago-to-end", "1")
                .put("collector.combiner.disable-date-range-limit", "true")
                .put("collector.combiner.thread-count", "10")
                .put("collector.combiner.high-priority-event-types", ", TypeA , , TypeB,TypeB,")
                .put("collector.combiner.low-priority-event-types", ", TypeC , ,TypeD,")
                .put("collector.retry-period", "10m")
                .put("collector.retry-delay", "4m")
                .put("collector.combiner.group-id", "someGroupId")
                .build();

        ServerConfig expected = new ServerConfig()
                .setAcceptedEventTypes(ImmutableSet.of("Foo", "Bar"))
                .setMaxBufferTime(new Duration(2, TimeUnit.HOURS))
                .setTargetFileSize(new DataSize(768, DataSize.Unit.MEGABYTE))
                .setLocalStagingDirectory(new File("testdir"))
                .setCombinerMetadataDirectory(new File("test-combiner"))
                .setMaxUploadThreads(42)
                .setAwsAccessKey("my-access-key")
                .setAwsSecretKey("my-secret-key")
                .setS3StagingLocation("s3://example-staging/")
                .setS3DataLocation("s3://example-data/")
                .setS3MetadataLocation("s3://example-metadata/")
                .setCombinerEnabled(true)
                .setCombinerStartDaysAgo(10)
                .setCombinerEndDaysAgo(1)
                .setCombinerDateRangeLimitDisabled(true)
                .setCombinerThreadCount(10)
                .setCombinerHighPriorityEventTypes(ImmutableSet.of("TypeA", "TypeB"))
                .setCombinerLowPriorityEventTypes(ImmutableSet.of("TypeC", "TypeD"))
                .setRetryPeriod(new Duration(10, TimeUnit.MINUTES))
                .setRetryDelay(new Duration(4, TimeUnit.MINUTES))
                .setCombinerGroupId("someGroupId");

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testDeprecatedProperties()
    {
        Map<String, String> currentProperties = new ImmutableMap.Builder<String, String>()
                .put("collector.accepted-event-types", "Foo,Bar")
                .put("collector.max-buffer-time", "2h")
                .put("collector.target-file-size", "768MB")
                .put("collector.local-staging-directory", "testdir")
                .put("collector.combiner.metadata-directory", "test-combiner")
                .put("collector.max-upload-threads", "42")
                .put("collector.aws-access-key", "my-access-key")
                .put("collector.aws-secret-key", "my-secret-key")
                .put("collector.s3-staging-location", "s3://example-staging/")
                .put("collector.s3-data-location", "s3://example-data/")
                .put("collector.s3-metadata-location", "s3://example-metadata/")
                .put("collector.combiner.enabled", "true")
                .put("collector.combiner.days-ago-to-start", "10")
                .put("collector.combiner.days-ago-to-end", "1")
                .put("collector.combiner.disable-date-range-limit", "true")
                .put("collector.retry-period", "10m")
                .put("collector.retry-delay", "4m")
                .put("collector.combiner.group-id", "someGroupId")
                .build();

        Map<String, String> oldProperties = new ImmutableMap.Builder<String, String>()
                .put("collector.accepted-event-types", "Foo,Bar")
                .put("collector.max-buffer-time", "2h")
                .put("collector.target-file-size", "768MB")
                .put("collector.local-staging-directory", "testdir")
                .put("collector.combiner.metadata-directory", "test-combiner")
                .put("collector.max-upload-threads", "42")
                .put("collector.aws-access-key", "my-access-key")
                .put("collector.aws-secret-key", "my-secret-key")
                .put("collector.s3-staging-location", "s3://example-staging/")
                .put("collector.s3-data-location", "s3://example-data/")
                .put("collector.s3-metadata-location", "s3://example-metadata/")
                .put("collector.combiner.enabled", "true")
                .put("collector.combiner.max-days-back", "10")
                .put("collector.combiner.days-ago-to-end", "1")
                .put("collector.combiner.disable-date-range-limit", "true")
                .put("collector.retry-period", "10m")
                .put("collector.retry-delay", "4m")
                .put("collector.combiner.group-id", "someGroupId")
                .build();

        ConfigAssertions.assertDeprecatedEquivalence(ServerConfig.class, currentProperties, oldProperties);
    }

    @Test
    public void testValidations()
    {
        assertFailsValidation(new ServerConfig().setCombinerStartDaysAgo(1).setCombinerEndDaysAgo(1), "combinerStartEndDaysSane", "must be true", AssertTrue.class);
        assertFailsValidation(new ServerConfig().setCombinerGroupId(null), "combinerGroupId", "may not be null", NotNull.class);
        assertFailsValidation(new ServerConfig().setCombinerGroupId(""), "combinerGroupId", "must be non-empty", Size.class);
        assertFailsValidation(new ServerConfig().setCombinerThreadCount(0), "combinerThreadCount", "must be greater than or equal to 1", Min.class);
        assertFailsValidation(new ServerConfig().setCombinerHighPriorityEventTypes(ImmutableSet.of("TypeA", "TypeB")).setCombinerLowPriorityEventTypes(ImmutableSet.of("TypeB", "TypeC")), "highAndLowPriorityEventTypesDisjoint", "High- and Low-Priority event type lists must be disjoint.", AssertTrue.class);

        assertValidates(new ServerConfig()
                .setLocalStagingDirectory(new File("testdir"))
                .setAwsAccessKey("my-access-key")
                .setAwsSecretKey("my-secret-key")
                .setS3StagingLocation("s3://example-staging/")
                .setS3DataLocation("s3://example-data/")
                .setS3MetadataLocation("s3://example-metadata/")
                .setCombinerGroupId("someGroupId"));
    }

    @Test
    public void testS3LocationValidation()
    {
        // TODO: make these tests work
//        assertS3LocationValidates("s3://example-location/");
//        assertS3LocationValidates("s3://example/foo/");
//        assertS3LocationValidates("s3://example/foo/bar/");
//        assertS3LocationValidates("s3://example/foo/bar/blah/");

        assertS3LocationFailsValidation("s3://example-location");
        assertS3LocationFailsValidation("s3://example!/");
        assertS3LocationFailsValidation("s3://example/foo_bar/");
        assertS3LocationFailsValidation("s3://example/foo//bar/");
        assertS3LocationFailsValidation("s3://example/foo.bar/");
    }

    private static void assertS3LocationValidates(String location)
    {
        assertValidates(new ServerConfig().setS3StagingLocation(location));
        assertValidates(new ServerConfig().setS3DataLocation(location));
    }

    private static void assertS3LocationFailsValidation(String location)
    {
        assertFailsValidation(new ServerConfig().setS3StagingLocation(location), "s3StagingLocation", "is malformed", Pattern.class);
        assertFailsValidation(new ServerConfig().setS3DataLocation(location), "s3DataLocation", "is malformed", Pattern.class);
    }
}
