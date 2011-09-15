package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.proofpoint.collector.calligraphus.ServerConfig;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.node.NodeInfo;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URI;

import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3Bucket;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3ObjectKey;

public class FileSystemCombineObjectMetadataStore implements CombineObjectMetadataStore
{
    private final JsonCodec<CombinedStoredObject> jsonCodec = JsonCodec.jsonCodec(CombinedStoredObject.class);
    private final String nodeId;
    private final File metadataDirectory;

    @Inject
    public FileSystemCombineObjectMetadataStore(NodeInfo nodeInfo, ServerConfig config)
    {
        this.nodeId = nodeInfo.getNodeId();
        this.metadataDirectory = config.getCombinerMetadataDirectory();
    }

    public FileSystemCombineObjectMetadataStore(String nodeId, File metadataDirectory)
    {
        this.nodeId = nodeId;
        this.metadataDirectory = metadataDirectory;
    }

    @Override
    public CombinedStoredObject getCombinedObjectManifest(URI combinedObjectLocation)
    {
        CombinedStoredObject combinedStoredObject = readMetadataFile(combinedObjectLocation);
        if (combinedStoredObject != null) {
            return combinedStoredObject;
        }

        return CombinedStoredObject.createInitialCombinedStoredObject(combinedObjectLocation, nodeId);
    }

    @Override
    public boolean replaceCombinedObjectManifest(CombinedStoredObject currentCombinedObject, CombinedStoredObject newCombinedObject)
    {
        Preconditions.checkNotNull(currentCombinedObject, "currentCombinedObject is null");
        Preconditions.checkNotNull(newCombinedObject, "newCombinedObject is null");
        Preconditions.checkArgument(currentCombinedObject.getLocation().equals(newCombinedObject.getLocation()), "newCombinedObject location is different from currentCombineObject location ");

        CombinedStoredObject persistentCombinedStoredObject = readMetadataFile(newCombinedObject.getLocation());
        if (persistentCombinedStoredObject != null) {
            if (persistentCombinedStoredObject.getVersion() != currentCombinedObject.getVersion()) {
                return false;
            }
        }
        else if (currentCombinedObject.getVersion() != 0) {
            return false;
        }

        return writeMetadataFile(newCombinedObject);
    }

    private boolean writeMetadataFile(CombinedStoredObject newCombinedObject)
    {
        String json = jsonCodec.toJson(newCombinedObject);
        try {
            File metadataFile = toMetadataFile(newCombinedObject.getLocation());
            metadataFile.getParentFile().mkdirs();
            Files.write(json, metadataFile, Charsets.UTF_8);
            return true;
        }
        catch (IOException e) {
            return false;
        }
    }

    private File toMetadataFile(URI location)
    {
        File file = new File(metadataDirectory, getS3Bucket(location) + "/" + getS3ObjectKey(location) + ".metadata");
        return file;
    }

    private CombinedStoredObject readMetadataFile(URI location)
    {
        File metadataFile = toMetadataFile(location);
        if (!metadataFile.exists()) {
            return null;
        }
        try {
            String json = Files.toString(metadataFile, Charsets.UTF_8);
            CombinedStoredObject combinedStoredObject = jsonCodec.fromJson(json);
            return combinedStoredObject;
        }
        catch (IOException e) {
            throw new RuntimeException("Metadata file for " + location + " is corrupt");
        }
    }
}
