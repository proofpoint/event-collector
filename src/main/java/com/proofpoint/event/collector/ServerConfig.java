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

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.proofpoint.configuration.Config;
import com.proofpoint.configuration.ConfigDescription;
import com.proofpoint.configuration.ConfigSecuritySensitive;
import com.proofpoint.configuration.LegacyConfig;
import com.proofpoint.units.DataSize;
import com.proofpoint.units.Duration;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import java.io.File;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ServerConfig
{
    private static final String S3_PATH_REGEXP = "s3://[A-Za-z0-9-]+/([A-Za-z0-9-]+/)*";
    private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

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
    private int combinerThreadCount = 5;
    private Set<String> combinerHighPriorityEventTypes = ImmutableSet.of();
    private Set<String> combinerLowPriorityEventTypes = ImmutableSet.of();
    private Set<String> acceptedEventTypes = ImmutableSet.of();
    private Duration retryPeriod = new Duration(5, TimeUnit.MINUTES);
    private Duration retryDelay = new Duration(0, TimeUnit.MINUTES);
    private String combinerGroupId = "default";
    private String serviceType = "collector";
    private int distributeFilterPercent = 0;


    @NotNull
    public String getServiceType()
    {
        return serviceType;
    }

    @Config("collector.service-type")
    @ConfigDescription("Name of service to announce into discover service, defaults to 'collector'")
    public ServerConfig setServiceType(String serviceType)
    {
        this.serviceType = serviceType;
        return this;
    }

    @NotNull
    public Set<String> getAcceptedEventTypes()
    {
        return acceptedEventTypes;
    }

    @Config("collector.accepted-event-types")
    @ConfigDescription("Comma separated list of known event types")
    public ServerConfig setAcceptedEventTypes(String acceptedEventTypes)
    {
        this.acceptedEventTypes = toSetOfStrings(acceptedEventTypes);
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
    @ConfigSecuritySensitive
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

    @Max(100)
    @Min(0)
    public int getDistributeFilterPercent()
    {
        return distributeFilterPercent;
    }

    @Config("collector.distribute.filter-percent")
    @ConfigDescription("Filters out the specified percent of messages for all event types when distribute is called. Default is 0 (do not filter)")
    public ServerConfig setDistributeFilterPercent(int distributeFilterPercent)
    {
        this.distributeFilterPercent = distributeFilterPercent;
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

    @Min(1)
    public int getCombinerThreadCount()
    {
        return combinerThreadCount;
    }

    @Config("collector.combiner.thread-count")
    @ConfigDescription("Number of threads the combiner uses for combining events of normal priority.")
    public ServerConfig setCombinerThreadCount(int combinerThreadCount)
    {
        this.combinerThreadCount = combinerThreadCount;
        return this;
    }

    @NotNull
    public Set<String> getCombinerHighPriorityEventTypes()
    {
        return combinerHighPriorityEventTypes;
    }

    @Config("collector.combiner.high-priority-event-types")
    @ConfigDescription("Comma separated list of event types that are high priority for the combiner.")
    public ServerConfig setCombinerHighPriorityEventTypes(String combinerHighPriorityEventTypes)
    {
        this.combinerHighPriorityEventTypes = toSetOfStrings(combinerHighPriorityEventTypes);
        return this;
    }

    public ServerConfig setCombinerHighPriorityEventTypes(Set<String> combinerHighPriorityEventTypes)
    {
        this.combinerHighPriorityEventTypes = ImmutableSet.copyOf(combinerHighPriorityEventTypes);
        return this;
    }

    @NotNull
    public Set<String> getCombinerLowPriorityEventTypes()
    {
        return combinerLowPriorityEventTypes;
    }

    @Config("collector.combiner.low-priority-event-types")
    @ConfigDescription("Comma separated list of event types that are low priority for the combiner.")
    public ServerConfig setCombinerLowPriorityEventTypes(String combinerLowPriorityEventTypes)
    {
        this.combinerLowPriorityEventTypes = toSetOfStrings(combinerLowPriorityEventTypes);
        return this;
    }

    public ServerConfig setCombinerLowPriorityEventTypes(Set<String> combinerLowPriorityEventTypes)
    {
        this.combinerLowPriorityEventTypes = ImmutableSet.copyOf(combinerLowPriorityEventTypes);
        return this;
    }

    @AssertTrue(message = "High- and Low-Priority event type lists must be disjoint.")
    public boolean isHighAndLowPriorityEventTypesDisjoint()
    {
        return Sets.intersection(combinerHighPriorityEventTypes, combinerLowPriorityEventTypes).isEmpty();
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

    @Config("collector.combiner.group-id")
    @ConfigDescription("Group id for combiner installation")
    public ServerConfig setCombinerGroupId(String combinerGroupId)
    {
        this.combinerGroupId = combinerGroupId;
        return this;
    }

    @NotNull
    @Size(min=1, message="must be non-empty")
    public String getCombinerGroupId()
    {
        return combinerGroupId;
    }

    private Set<String> toSetOfStrings(String commaSeparatedList)
    {
        return ImmutableSet.copyOf(COMMA_SPLITTER.split(commaSeparatedList));
    }
}
