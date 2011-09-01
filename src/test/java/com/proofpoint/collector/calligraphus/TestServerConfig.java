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

import com.google.common.collect.ImmutableMap;
import com.proofpoint.configuration.testing.ConfigAssertions;
import com.proofpoint.experimental.units.DataSize;
import com.proofpoint.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.Pattern;
import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.proofpoint.experimental.testing.ValidationAssertions.assertFailsValidation;
import static com.proofpoint.experimental.testing.ValidationAssertions.assertValidates;

public class TestServerConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(ServerConfig.class)
                .setMaxBufferTime(new Duration(1, TimeUnit.MINUTES))
                .setTargetFileSize(new DataSize(512, DataSize.Unit.MEGABYTE))
                .setLocalStagingDirectory(new File("staging"))
                .setMaxUploadThreads(10)
                .setAwsAccessKey(null)
                .setAwsSecretKey(null)
                .setS3StagingLocation(null)
                .setS3DataLocation(null)
        );
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("collector.max-buffer-time", "2h")
                .put("collector.target-file-size", "768MB")
                .put("collector.local-staging-directory", "testdir")
                .put("collector.max-upload-threads", "42")
                .put("collector.aws-access-key", "my-access-key")
                .put("collector.aws-secret-key", "my-secret-key")
                .put("collector.s3-staging-location", "s3://example-staging/")
                .put("collector.s3-data-location", "s3://example-data/")
                .build();

        ServerConfig expected = new ServerConfig()
                .setMaxBufferTime(new Duration(2, TimeUnit.HOURS))
                .setTargetFileSize(new DataSize(768, DataSize.Unit.MEGABYTE))
                .setLocalStagingDirectory(new File("testdir"))
                .setMaxUploadThreads(42)
                .setAwsAccessKey("my-access-key")
                .setAwsSecretKey("my-secret-key")
                .setS3StagingLocation("s3://example-staging/")
                .setS3DataLocation("s3://example-data/");

        ConfigAssertions.assertFullMapping(properties, expected);
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
