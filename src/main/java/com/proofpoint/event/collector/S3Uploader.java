package com.proofpoint.event.collector;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proofpoint.event.collector.combiner.StorageSystem;
import com.proofpoint.event.collector.combiner.StoredObject;
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

import static com.proofpoint.event.collector.combiner.S3StorageHelper.buildS3Location;

public class S3Uploader
        implements Uploader
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
}
