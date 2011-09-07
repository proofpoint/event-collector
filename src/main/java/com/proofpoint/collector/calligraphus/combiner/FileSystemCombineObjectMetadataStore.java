package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import com.proofpoint.json.JsonCodec;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.UUID;

import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3Bucket;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3ObjectKey;

public class FileSystemCombineObjectMetadataStore implements CombineObjectMetadataStore
{
    private final JsonCodec<CombinedStoredObject> jsonCodec = JsonCodec.jsonCodec(CombinedStoredObject.class);
    private final String nodeId;
    private final File directory;

    public FileSystemCombineObjectMetadataStore(String nodeId, File directory)
    {
        this.nodeId = nodeId;
        this.directory = directory;
    }

    @Override
    public CombinedStoredObject getCombinedObjectManifest(URI combinedObjectLocation)
    {
        File metadataFile = createMetadataFile(combinedObjectLocation);
        CombinedStoredObject combinedStoredObject = readMetadataFile(metadataFile);
        if (combinedStoredObject != null) {
            return combinedStoredObject;
        }

        return new CombinedStoredObject(combinedObjectLocation, nodeId);
    }

    @Override
    public boolean replaceCombinedObjectManifest(CombinedStoredObject currentCombinedObject, List<StoredObject> newCombinedObjectParts)
    {
        File metadataFile = createMetadataFile(currentCombinedObject.getLocation());
        CombinedStoredObject persistentCombinedStoredObject = readMetadataFile(metadataFile);
        if (persistentCombinedStoredObject != null) {
            if (!persistentCombinedStoredObject.getETag().endsWith(currentCombinedObject.getETag())) {
                return false;
            }
        }
        else if (currentCombinedObject.getETag() != null) {
            return false;
        }

        long totalSize = 0;
        for (StoredObject storedObject : newCombinedObjectParts) {
            totalSize += storedObject.getSize();
        }

        CombinedStoredObject newCombinedObject = new CombinedStoredObject(
                currentCombinedObject.getLocation(),
                UUID.randomUUID().toString(),
                totalSize,
                System.currentTimeMillis(),
                nodeId,
                System.currentTimeMillis(),
                newCombinedObjectParts
        );
        String json = jsonCodec.toJson(newCombinedObject);
        try {
            Files.write(json, metadataFile, Charsets.UTF_8);
            return true;
        }
        catch (IOException e) {
            return false;
        }
    }

    private File createMetadataFile(URI location)
    {
        File file = new File(directory, getS3Bucket(location) + "/" + getS3ObjectKey(location) + ".metadata");
        return file;
    }

    private CombinedStoredObject readMetadataFile(File metadataFile)
    {
        try {
            String json = Files.toString(metadataFile, Charsets.UTF_8);
            CombinedStoredObject combinedStoredObject = jsonCodec.fromJson(json);
            return combinedStoredObject;
        }
        catch (IOException e) {
            // todo what to do here?
            throw Throwables.propagate(e);
        }
    }
}
