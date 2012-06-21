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

import com.amazonaws.services.s3.AmazonS3;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proofpoint.log.Logger;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.iq80.snappy.SnappyInputStream;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.proofpoint.event.collector.S3StorageHelper.buildS3Location;
import static com.proofpoint.event.collector.S3StorageHelper.getS3Bucket;
import static com.proofpoint.event.collector.S3StorageHelper.getS3ObjectKey;

public class S3Uploader
        implements Uploader
{
    private static final Logger log = Logger.get(S3Uploader.class);
    private final AmazonS3 s3Service;
    private final File localStagingDirectory;
    private final ExecutorService executor;
    private final String s3StagingLocation;

    @Inject
    public S3Uploader(AmazonS3 s3Service, CollectorConfig config)
    {
        this.s3Service = s3Service;
        this.localStagingDirectory = config.getLocalStagingDirectory();
        s3StagingLocation = config.getS3StagingLocation();
        this.executor = Executors.newFixedThreadPool(config.getMaxUploadThreads(),
                new ThreadFactoryBuilder().setNameFormat("S3Uploader-%s").build());

        //noinspection ResultOfMethodCallIgnored
        localStagingDirectory.mkdirs();
        Preconditions.checkArgument(localStagingDirectory.isDirectory(), "localStagingDirectory is not a directory (%s)", localStagingDirectory);
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
        executor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    upload(partition, file);
                }
                catch (Exception e) {
                    log.error(e, "upload failed: %s: %s", partition, file);
                }
            }
        });
    }

    @PreDestroy
    public void destroy()
            throws IOException
    {
        executor.shutdown();
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

        log.info("starting upload: %s", location);
        s3Service.putObject(getS3Bucket(location), getS3ObjectKey(location), file);
        log.info("completed upload: %s", location);

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
}
