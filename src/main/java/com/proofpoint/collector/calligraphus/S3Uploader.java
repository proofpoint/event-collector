package com.proofpoint.collector.calligraphus;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proofpoint.collector.calligraphus.combiner.StorageSystem;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class S3Uploader
{
    private final File localStagingDirectory;
    private final ExecutorService executor;
    private final StorageSystem storageSystem;

    @Inject
    public S3Uploader(StorageSystem storageSystem, ServerConfig config)
    {
        this.storageSystem = storageSystem;
        this.localStagingDirectory = config.getLocalStagingDirectory();
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
// TODO: implement
//        StoredObject target;
//        storageSystem.putObject(target, file);
    }
}
