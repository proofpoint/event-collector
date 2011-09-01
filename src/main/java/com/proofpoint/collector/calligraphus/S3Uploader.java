package com.proofpoint.collector.calligraphus;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proofpoint.collector.calligraphus.combiner.StorageSystem;
import com.proofpoint.collector.calligraphus.combiner.StoredObject;
import com.proofpoint.experimental.units.DataSize;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.buildS3StorageArea;

public class S3Uploader
{
    private final static long SMALL_FILE_CUTOFF = new DataSize(5, DataSize.Unit.MEGABYTE).toBytes();
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
        String stageType = (file.length() >= SMALL_FILE_CUTOFF) ? "large" : "small";
        URI storageArea = buildS3StorageArea(s3StagingLocation, partition.getEventType(), partition.getTimeBucket(), stageType);
        StoredObject target = new StoredObject(file.getName(), storageArea);
        storageSystem.putObject(target, file);
    }
}