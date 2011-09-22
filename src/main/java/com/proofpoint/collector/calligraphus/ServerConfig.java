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
import com.proofpoint.experimental.units.DataSize;
import com.proofpoint.units.Duration;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.io.File;
import java.util.concurrent.TimeUnit;

public class ServerConfig
{
    private Duration maxBufferTime = new Duration(1, TimeUnit.MINUTES);
    private DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);
    private File localStagingDirectory = new File("staging");
    private File combinerMetadataDirectory = new File("combiner-metadata");
    private int maxUploadThreads = 10;
    private String awsAccessKey;
    private String awsSecretKey;
    private String s3StagingLocation;
    private String s3DataLocation;
    private boolean combinerEnabled = true;

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

    @Config("collector.target-file-size")
    @ConfigDescription("target size of final files in storage area (maximum size can be double)")
    public ServerConfig setTargetFileSize(DataSize targetFileSize)
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

    @Config("collector.local-staging-directory")
    @ConfigDescription("local directory to store files before they are uploaded -- should persist across restarts")
    public ServerConfig setLocalStagingDirectory(File localStagingDirectory)
    {
        this.localStagingDirectory = localStagingDirectory;
        return this;
    }

    @NotNull
    public File getLocalStagingDirectory()
    {
        return localStagingDirectory;
    }

    @Config("collector.combiner.metadata-directory")
    @ConfigDescription("directory to store information about the file combine process -- should persist across restarts")
    public ServerConfig setCombinerMetadataDirectory(File combinerMetadataDirectory)
    {
        this.combinerMetadataDirectory = combinerMetadataDirectory;
        return this;
    }

    @NotNull
    public File getCombinerMetadataDirectory()
    {
        return combinerMetadataDirectory;
    }

    @Config("collector.max-upload-threads")
    @ConfigDescription("maximum number of concurrent uploads")
    public ServerConfig setMaxUploadThreads(int maxUploadThreads)
    {
        this.maxUploadThreads = maxUploadThreads;
        return this;
    }

    @Min(1)
    @Max(500)
    public int getMaxUploadThreads()
    {
        return maxUploadThreads;
    }

    @Config("collector.aws-access-key")
    public ServerConfig setAwsAccessKey(String awsAccessKey)
    {
        this.awsAccessKey = awsAccessKey;
        return this;
    }

    @NotNull
    public String getAwsAccessKey()
    {
        return awsAccessKey;
    }

    @Config("collector.aws-secret-key")
    public ServerConfig setAwsSecretKey(String awsSecretKey)
    {
        this.awsSecretKey = awsSecretKey;
        return this;
    }

    @NotNull
    public String getAwsSecretKey()
    {
        return awsSecretKey;
    }

    @Config("collector.s3-staging-location")
    @ConfigDescription("base S3 URI to staging location")
    public ServerConfig setS3StagingLocation(String s3StagingLocation)
    {
        this.s3StagingLocation = s3StagingLocation;
        return this;
    }

    @NotNull
    @Pattern(regexp = "s3://[A-Za-z0-9-]+/([A-Za-z0-9-]+/)*", message = "is malformed")
    public String getS3StagingLocation()
    {
        return s3StagingLocation;
    }

    @Config("collector.s3-data-location")
    @ConfigDescription("base S3 URI to data location")
    public ServerConfig setS3DataLocation(String s3DataLocation)
    {
        this.s3DataLocation = s3DataLocation;
        return this;
    }

    @NotNull
    @Pattern(regexp = "s3://[A-Za-z0-9-]+/([A-Za-z0-9-]+/)*", message = "is malformed")
    public String getS3DataLocation()
    {
        return s3DataLocation;
    }

    @Config("collector.combiner.enabled")
    public ServerConfig setCombinerEnabled(boolean combinerEnabled)
    {
        this.combinerEnabled = combinerEnabled;
        return this;
    }

    public boolean isCombinerEnabled()
    {
        return combinerEnabled;
    }
}
