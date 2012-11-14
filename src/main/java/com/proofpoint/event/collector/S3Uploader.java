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
package com.proofpoint.event.collector;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Closeables;
import com.proofpoint.event.collector.combiner.StorageSystem;
import com.proofpoint.event.collector.combiner.StoredObject;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.log.Logger;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.iq80.snappy.SnappyInputStream;

import javax.annotation.PreDestroy;
import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static com.proofpoint.event.collector.combiner.S3StorageHelper.buildS3Location;
import static com.proofpoint.json.JsonCodec.jsonCodec;

public class S3Uploader
        implements Uploader
{
    private static final Logger log = Logger.get(S3Uploader.class);
    private static final JsonCodec<Event> codec = jsonCodec(Event.class);
    private final StorageSystem storageSystem;
    private final File localStagingDirectory;
    private final ExecutorService uploadExecutor;
    private final ExecutorService pendingFileExecutor;
    private final String s3StagingLocation;
    private final EventPartitioner partitioner;
    private final File failedFileDir;

    @Inject
    public S3Uploader(StorageSystem storageSystem,
            ServerConfig config,
            EventPartitioner partitioner,
            @UploaderExecutorService ExecutorService uploadExecutor,
            @PendingFileExecutorService ExecutorService pendingFileExecutor)
    {
        this.storageSystem = storageSystem;
        this.localStagingDirectory = config.getLocalStagingDirectory();
        this.s3StagingLocation = config.getS3StagingLocation();
        this.partitioner = partitioner;
        this.uploadExecutor = uploadExecutor;
        this.pendingFileExecutor = pendingFileExecutor;
        this.failedFileDir = new File(localStagingDirectory.getPath(), "failed");

        //noinspection ResultOfMethodCallIgnored
        localStagingDirectory.mkdirs();
        Preconditions.checkArgument(localStagingDirectory.isDirectory(), "localStagingDirectory is not a directory (%s)", localStagingDirectory);
        failedFileDir.mkdir();
        Preconditions.checkArgument(failedFileDir.isDirectory(), "failedFileDir is not a directory (%s)", failedFileDir);
    }

    @PostConstruct
    public void start()
    {
        File[] pendingFiles = localStagingDirectory.listFiles();

        for (final File file : pendingFiles) {
            enqueuePendingFile(file);
        }
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
                    upload(partition, file);
                }
                catch (Exception e) {
                    log.error(e, "upload failed: %s: %s", partition, file);
                    enqueuePendingFile(file);
                }
            }
        });
    }

    @PreDestroy
    public void destroy()
            throws IOException
    {
        uploadExecutor.shutdown();
        pendingFileExecutor.shutdown();
    }

    private void upload(EventPartition partition, File file)
            throws IOException
    {
        verifyFile(file);

        URI location = buildS3Location(s3StagingLocation,
                partition.getEventType(),
                partition.getMajorTimeBucket(),
                partition.getMinorTimeBucket(),
                file.getName());
        StoredObject target = new StoredObject(location);

        storageSystem.putObject(target.getLocation(), file);

        if (!file.delete()) {
            log.warn("failed to delete local staging file: %s", file.getAbsolutePath());
        }
    }

    private void verifyFile(File file)
            throws IOException
    {
        SnappyInputStream input = new SnappyInputStream(new FileInputStream(file));
        JsonParser parser = new ObjectMapper().getJsonFactory().createJsonParser(input);
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
    public void enqueuePendingFile(final File file)
    {
        pendingFileExecutor.submit(new Runnable()
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
                    log.error(e, "Error while uploading file %s", file.getName());
                    file.renameTo(new File(failedFileDir, file.getName()));
                }
                finally {
                    Closeables.closeQuietly(filein);
                    Closeables.closeQuietly(in);
                }
            }
        });
    }
}
