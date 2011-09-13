package com.proofpoint.collector.calligraphus;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proofpoint.collector.calligraphus.combiner.StorageSystem;
import com.proofpoint.collector.calligraphus.combiner.StoredObject;
import com.proofpoint.log.Logger;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.buildS3Location;

public class S3Uploader
{
    private static final Logger log = Logger.get(S3Uploader.class);
    private final StorageSystem storageSystem;
    private final File localStagingDirectory;
    private final ExecutorService executor;
    private final String s3StagingLocation;

    @Inject
    public S3Uploader(StorageSystem storageSystem, ServerConfig config)
    {
        this.storageSystem = storageSystem;
        this.localStagingDirectory = config.getLocalStagingDirectory();
        s3StagingLocation = config.getS3StagingLocation();
        this.executor = Executors.newFixedThreadPool(config.getMaxUploadThreads(),
                new ThreadFactoryBuilder().setNameFormat("S3Uploader-%s").build());

        //noinspection ResultOfMethodCallIgnored
        localStagingDirectory.mkdirs();
        Preconditions.checkArgument(localStagingDirectory.isDirectory(), "localStagingDirectory is not a directory (%s)", localStagingDirectory);
    }

    public File generateNextFilename()
    {
        String uuid = UUID.randomUUID().toString().replace("-", "");
        return new File(localStagingDirectory, uuid + ".json.snappy");
    }

    public void enqueueUpload(final EventPartition partition, final File file)
    {
        executor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                upload(partition, file);
            }
        });
    }

    @SuppressWarnings("UnusedDeclaration")
    @PreDestroy
    public void destroy()
            throws IOException
    {
        executor.shutdown();
    }

    private void upload(EventPartition partition, File file)
    {
        URI location = buildS3Location(s3StagingLocation,
                partition.getEventType(),
                partition.getMajorTimeBucket(),
                partition.getMinorTimeBucket(),
                file.getName());
        StoredObject target = new StoredObject(location);

        log.info("starting upload: %s", target.getLocation());
        storageSystem.putObject(target, file);
        log.info("completed upload: %s", target.getLocation());

        if (!file.delete()) {
            log.warn("failed to delete local staging file: %s", file.getAbsolutePath());
        }
    }
}
