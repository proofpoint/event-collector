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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.proofpoint.event.collector.combiner.StorageSystem;
import com.proofpoint.event.collector.combiner.StoredObject;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.log.Logger;
import com.proofpoint.stats.SparseTimeStat;
import com.proofpoint.units.Duration;
import org.iq80.snappy.SnappyInputStream;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.proofpoint.event.collector.S3UploaderStats.FileProcessedStatus.CORRUPT;
import static com.proofpoint.event.collector.S3UploaderStats.FileProcessedStatus.UPLOADED;
import static com.proofpoint.event.collector.S3UploaderStats.FileUploadStatus.FAILURE;
import static com.proofpoint.event.collector.S3UploaderStats.FileUploadStatus.SUCCESS;
import static com.proofpoint.event.collector.combiner.S3StorageHelper.buildS3Location;
import static com.proofpoint.json.JsonCodec.jsonCodec;

public class S3Uploader
        implements Uploader
{
    private static final Logger log = Logger.get(S3Uploader.class);
    private static final JsonCodec<Event> codec = jsonCodec(Event.class);
    private static final String UNKNOWN_EVENT_TYPE = "unknown";
    private final StorageSystem storageSystem;
    private final File localStagingDirectory;
    private final ExecutorService uploadExecutor;
    private final ScheduledExecutorService retryExecutor;
    private final String s3StagingLocation;
    private final EventPartitioner partitioner;
    private final File failedFileDir;
    private final File retryFileDir;
    private final Duration retryPeriod;
    private final Duration retryDelay;
    private final S3UploaderStats s3UploaderStats;

    @Inject
    public S3Uploader(StorageSystem storageSystem,
            ServerConfig config,
            EventPartitioner partitioner,
            @UploaderExecutorService ExecutorService uploadExecutor,
            @PendingFileExecutorService ScheduledExecutorService retryExecutor,
            S3UploaderStats s3UploaderStats)
    {
        this.storageSystem = storageSystem;
        this.localStagingDirectory = config.getLocalStagingDirectory();
        this.s3StagingLocation = config.getS3StagingLocation();
        this.partitioner = partitioner;
        this.uploadExecutor = uploadExecutor;
        this.retryExecutor = retryExecutor;
        this.failedFileDir = new File(localStagingDirectory.getPath(), "failed");
        this.retryFileDir = new File(localStagingDirectory.getPath(), "retry");
        this.retryPeriod = config.getRetryPeriod();
        this.retryDelay = config.getRetryDelay();
        this.s3UploaderStats = checkNotNull(s3UploaderStats, "s3UploaderStats is null");

        //noinspection ResultOfMethodCallIgnored
        localStagingDirectory.mkdirs();
        Preconditions.checkArgument(localStagingDirectory.isDirectory(), "localStagingDirectory is not a directory (%s)", localStagingDirectory);
        failedFileDir.mkdir();
        Preconditions.checkArgument(failedFileDir.isDirectory(), "failedFileDir is not a directory (%s)", failedFileDir);
        retryFileDir.mkdir();
        Preconditions.checkArgument(retryFileDir.isDirectory(), "retryFileDir is not a directory (%s)", retryFileDir);
    }

    @PostConstruct
    public void start()
    {
        File[] pendingFiles = localStagingDirectory.listFiles();

        for (final File file : pendingFiles) {
            if (file.isFile()) {
                enqueueLocalFileForUpload(file);
            }
        }

        scheduleRetryTask();
    }

    @Override
    public File generateNextFilename()
    {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        return new File(localStagingDirectory, uuid + ".json.snappy");
    }

    @Override
    public void enqueueUpload(final EventPartition partition, final File file)
    {
        uploadExecutor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    verifyFile(file);
                }
                catch (Exception e) {
                    log.error(e, "error verifying file: %s: %s. Marking as failed", partition, file);
                    handleFailure(file);
                    return;
                }

                try {
                    upload(partition, file);
                    String eventType = partition.getEventType();
                    s3UploaderStats.processedFiles(eventType, UPLOADED).add(1);
                    s3UploaderStats.uploadAttempts(eventType, SUCCESS).add(1);
                }
                catch (Exception e) {
                    log.error(e, "upload failed: %s: %s. Sending for retry", partition, file);
                    handleRetry(partition, file);
                }
            }
        });
    }

    @PreDestroy
    public void destroy()
            throws IOException
    {
        uploadExecutor.shutdown();
        retryExecutor.shutdown();
    }

    private void upload(EventPartition partition, File file)
            throws IOException
    {
        URI location = buildS3Location(s3StagingLocation,
                partition.getEventType(),
                partition.getMajorTimeBucket(),
                partition.getMinorTimeBucket(),
                file.getName());
        StoredObject target = new StoredObject(location);

        try (SparseTimeStat.BlockTimer ignored = s3UploaderStats.processedTime(partition.getEventType()).time()) {
            storageSystem.putObject(target.getLocation(), file);
        }

        if (!file.delete()) {
            log.warn("failed to delete local staging file: %s", file.getAbsolutePath());
        }
    }

    private void verifyFile(File file)
            throws IOException
    {
        SnappyInputStream input = new SnappyInputStream(new FileInputStream(file));
        JsonParser parser = new ObjectMapper().getFactory().createParser(input);
        try {
            while (parser.readValueAsTree() != null) {
                // ignore contents
            }
        }
        finally {
            parser.close();
        }
    }

    @VisibleForTesting
    public void enqueueLocalFileForUpload(final File file)
    {
        retryExecutor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                BufferedReader in = null;
                FileInputStream filein = null;
                try {
                    filein = new FileInputStream(file);
                    in = new BufferedReader(new InputStreamReader(new SnappyInputStream(filein), Charsets.UTF_8));
                    Event event = codec.fromJson(in.readLine());
                    EventPartition partition = partitioner.getPartition(event);
                    enqueueUpload(partition, file);
                }
                catch (Exception e) {
                    log.error(e, "Error while reading file %s before upload. Marking as failed", file.getName());
                    handleFailure(file);
                }
                finally {
                    try {
                        Closeables.close(filein, true);
                        Closeables.close(in, true);
                    }
                    catch (IOException e) {
                        log.error(e, "Error closing event file");
                    }
                }
            }
        });
    }

    private void scheduleRetryTask()
    {
        retryExecutor.scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run()
            {
                File[] retryFiles = retryFileDir.listFiles();
                for (final File file : retryFiles) {
                    if (file.isFile()) {
                        //moving out of retry to avoid requeueing the file before the uploader gets to it
                        File stagingFile = moveToStaging(file);
                        enqueueLocalFileForUpload(stagingFile);
                    }
                }
            }
        }, (long) retryDelay.toMillis(), (long) retryPeriod.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void handleFailure(File file)
    {
        moveToFailed(file);
        s3UploaderStats.processedFiles(UNKNOWN_EVENT_TYPE, CORRUPT).add(1);
    }

    private void handleRetry(EventPartition partition, File file)
    {
        moveToRetry(file);
        s3UploaderStats.uploadAttempts(partition.getEventType(), FAILURE).add(1);
    }

    private void moveToFailed(File file)
    {
        moveFile(file, failedFileDir);
    }

    private void moveToRetry(File file)
    {
        moveFile(file, retryFileDir);
    }

    private File moveToStaging(File file)
    {
        return moveFile(file, localStagingDirectory);
    }

    private File moveFile(File file, File targetDirectory)
    {
        File targetFile = new File(targetDirectory, file.getName());
        try {
            if (!file.renameTo(targetFile)) {
                log.error("Error renaming file %s to %s", file, targetFile);
            }
        }
        catch (Exception e) {
            log.error("Error renaming file %s to %s", file, targetFile);
        }
        return targetFile;
    }
}
