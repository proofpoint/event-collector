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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.configuration.Config;
import com.proofpoint.configuration.ConfigDescription;
import com.proofpoint.configuration.LegacyConfig;
import com.proofpoint.experimental.units.DataSize;
import com.proofpoint.units.Duration;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.io.File;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ServerConfig
{
    private static final String S3_PATH_REGEXP = "s3://[A-Za-z0-9-]+/([A-Za-z0-9-]+/)*";

    private Duration maxBufferTime = new Duration(1, TimeUnit.MINUTES);
    private DataSize targetFileSize = new DataSize(512, DataSize.Unit.MEGABYTE);
    private File localStagingDirectory = new File("staging");
    private File combinerMetadataDirectory = new File("combiner-metadata");
    private int maxUploadThreads = 10;
    private String awsAccessKey;
    private String awsSecretKey;
    private String s3StagingLocation;
    private String s3DataLocation;
    private String s3MetadataLocation;
    private boolean combinerEnabled = false;
    private boolean combinerDateRangeLimitDisabled = false;
    private int combinerStartDaysAgo = 14;
    private int combinerEndDaysAgo = -1;
    private Set<String> acceptedEventTypes = ImmutableSet.of();
    private Duration retryPeriod = new Duration(5, TimeUnit.MINUTES);
    private Duration retryDelay = new Duration(0, TimeUnit.MINUTES);

    @NotNull
    public Set<String> getAcceptedEventTypes()
    {
        return acceptedEventTypes;
    }

    @Config("collector.accepted-event-types")
    @ConfigDescription("Comma separated list of known event types")
    public ServerConfig setAcceptedEventTypes(String acceptedEventTypes)
    {
        this.acceptedEventTypes = ImmutableSet.copyOf(Splitter.on(',').omitEmptyStrings().trimResults().split(acceptedEventTypes));
        return this;
    }

    public ServerConfig setAcceptedEventTypes(Iterable<String> acceptedEventTypes)
    {
        this.acceptedEventTypes = ImmutableSet.copyOf(acceptedEventTypes);
        return this;
    }

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
    @Pattern(regexp = S3_PATH_REGEXP, message = "is malformed")
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
    @Pattern(regexp = S3_PATH_REGEXP, message = "is malformed")
    public String getS3DataLocation()
    {
        return s3DataLocation;
    }

    @Config("collector.s3-metadata-location")
    @ConfigDescription("base S3 URI to metadata location")
    public ServerConfig setS3MetadataLocation(String s3MetadataLocation)
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

    @Config("collector.combiner.days-ago-to-start")
    @ConfigDescription("Combiner will combine data greater than or equal to X days ago")
    public ServerConfig setCombinerStartDaysAgo(int combinerStartDaysAgo)
    {
        this.combinerStartDaysAgo = combinerStartDaysAgo;
        return this;
    }

    public int getCombinerStartDaysAgo()
    {
        return combinerStartDaysAgo;
    }

    @Deprecated
    @LegacyConfig(value = "collector.combiner.max-days-back", replacedBy = "collector.combiner.days-ago-to-start")
    public ServerConfig setCombinerMaxDaysBack(int combinerMaxDaysBack)
    {
        return setCombinerStartDaysAgo(combinerMaxDaysBack);
    }

    @Config("collector.combiner.days-ago-to-end")
    @ConfigDescription("Combiner will combine data before from less than X days ago")
    public ServerConfig setCombinerEndDaysAgo(int combinerEndDaysAgo)
    {
        this.combinerEndDaysAgo = combinerEndDaysAgo;
        return this;
    }

    public int getCombinerEndDaysAgo()
    {
        return combinerEndDaysAgo;
    }

    @AssertTrue
    public boolean isCombinerStartEndDaysSane()
    {
        if (combinerStartDaysAgo > combinerEndDaysAgo) {
            return true;
        }

        return false;
    }

    @Config("collector.combiner.disable-date-range-limit")
    @ConfigDescription("Disable combiner limiting by the days-ago start and end arguments")
    public ServerConfig setCombinerDateRangeLimitDisabled(boolean combinerDateRangeLimitDisabled)
    {
        this.combinerDateRangeLimitDisabled = combinerDateRangeLimitDisabled;
        return this;
    }

    public boolean isCombinerDateRangeLimitDisabled()
    {
        return this.combinerDateRangeLimitDisabled;
    }

    @Config("collector.retry-period")
    @ConfigDescription("period between iterations for retrying files that were not successfully uploaded")
    public ServerConfig setRetryPeriod(Duration retryPeriod)
    {
        this.retryPeriod = retryPeriod;
        return this;
    }

    public Duration getRetryPeriod()
    {
        return retryPeriod;
    }

    @Config("collector.retry-delay")
    @ConfigDescription("delay from startup before the the first retry iteration begins")
    public ServerConfig setRetryDelay(Duration retryDelay)
    {
        this.retryDelay = retryDelay;
        return this;
    }

    public Duration getRetryDelay()
    {
        return retryDelay;
    }
}
