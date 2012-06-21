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

import com.google.common.base.Preconditions;
import com.proofpoint.configuration.Config;
import com.proofpoint.configuration.ConfigDescription;
import com.proofpoint.experimental.units.DataSize;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;

public class CombinerConfig
{
    private static final String S3_PATH_REGEXP = "s3://[A-Za-z0-9-]+/([A-Za-z0-9-]+/)*";

    private DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);
    private String awsAccessKey;
    private String awsSecretKey;
    private String s3StagingLocation;
    private String s3DataLocation;
    private String s3MetadataLocation;
    private int combinerMaxDaysBack = 14;

    @Config("combiner.target-file-size")
    @ConfigDescription("target size of final files in storage area (maximum size can be double)")
    public CombinerConfig setTargetFileSize(DataSize targetFileSize)
    {
        DataSize minSize = new DataSize(5, DataSize.Unit.MEGABYTE);
        DataSize maxSize = new DataSize(2, DataSize.Unit.GIGABYTE);
        Preconditions.checkNotNull(targetFileSize, "targetFileSize must not be null");
        Preconditions.checkArgument(targetFileSize.compareTo(minSize) >= 0, "targetFileSize must be at least " + minSize);
        Preconditions.checkArgument(targetFileSize.compareTo(maxSize) <= 0, "targetFileSize must be at most " + maxSize);
        this.targetFileSize = targetFileSize;
        return this;
    }

    @NotNull
    public DataSize getTargetFileSize()
    {
        return targetFileSize;
    }

    @Config("combiner.aws-access-key")
    public CombinerConfig setAwsAccessKey(String awsAccessKey)
    {
        this.awsAccessKey = awsAccessKey;
        return this;
    }

    @NotNull
    public String getAwsAccessKey()
    {
        return awsAccessKey;
    }

    @Config("combiner.aws-secret-key")
    public CombinerConfig setAwsSecretKey(String awsSecretKey)
    {
        this.awsSecretKey = awsSecretKey;
        return this;
    }

    @NotNull
    public String getAwsSecretKey()
    {
        return awsSecretKey;
    }

    @Config("combiner.s3-staging-location")
    @ConfigDescription("base S3 URI to staging location")
    public CombinerConfig setS3StagingLocation(String s3StagingLocation)
    {
        this.s3StagingLocation = s3StagingLocation;
        return this;
    }

    @NotNull
    @Pattern(regexp = S3_PATH_REGEXP, message = "is malformed")
    public String getS3StagingLocation()
    {
        return s3StagingLocation;
    }

    @Config("combiner.s3-data-location")
    @ConfigDescription("base S3 URI to data location")
    public CombinerConfig setS3DataLocation(String s3DataLocation)
    {
        this.s3DataLocation = s3DataLocation;
        return this;
    }

    @NotNull
    @Pattern(regexp = S3_PATH_REGEXP, message = "is malformed")
    public String getS3DataLocation()
    {
        return s3DataLocation;
    }

    @Config("combiner.s3-metadata-location")
    @ConfigDescription("base S3 URI to metadata location")
    public CombinerConfig setS3MetadataLocation(String s3MetadataLocation)
    {
        this.s3MetadataLocation = s3MetadataLocation;
        return this;
    }

    @NotNull
    @Pattern(regexp = S3_PATH_REGEXP, message = "is malformed")
    public String getS3MetadataLocation()
    {
        return s3MetadataLocation;
    }

    @Config("combiner.max-days-back")
    @ConfigDescription("maximum number of days back from today to combine data")
    public CombinerConfig setCombinerMaxDaysBack(int combinerMaxDaysBack)
    {
        this.combinerMaxDaysBack = combinerMaxDaysBack;
        return this;
    }

    @Min(0)
    public int getCombinerMaxDaysBack()
    {
        return combinerMaxDaysBack;
    }
}
