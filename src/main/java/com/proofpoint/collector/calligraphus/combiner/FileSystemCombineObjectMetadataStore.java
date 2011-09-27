package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.proofpoint.collector.calligraphus.ServerConfig;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.node.NodeInfo;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URI;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.proofpoint.collector.calligraphus.combiner.CombinedGroup.createInitialCombinedGroup;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3Bucket;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3ObjectKey;

public class FileSystemCombineObjectMetadataStore
        implements CombineObjectMetadataStore
{
    private final JsonCodec<CombinedGroup> jsonCodec = JsonCodec.jsonCodec(CombinedGroup.class);
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
    public CombinedGroup getCombinedGroupManifest(URI combinedGroupPrefix)
    {
        CombinedGroup combinedGroup = readMetadataFile(combinedGroupPrefix);
        if (combinedGroup != null) {
            return combinedGroup;
        }

        return createInitialCombinedGroup(combinedGroupPrefix, nodeId);
    }

    @Override
    public boolean replaceCombinedGroupManifest(CombinedGroup currentGroup, CombinedGroup newGroup)
    {
        checkNotNull(currentGroup, "currentGroup is null");
        checkNotNull(newGroup, "newGroup is null");
        checkArgument(currentGroup.getLocationPrefix().equals(newGroup.getLocationPrefix()), "newGroup location is different from currentGroup location");

        CombinedGroup persistentGroup = readMetadataFile(newGroup.getLocationPrefix());
        if (persistentGroup != null) {
            if (persistentGroup.getVersion() != currentGroup.getVersion()) {
                return false;
            }
        }
        else if (currentGroup.getVersion() != 0) {
            return false;
        }

        return writeMetadataFile(newGroup);
    }

    private boolean writeMetadataFile(CombinedGroup combinedGroup)
    {
        String json = jsonCodec.toJson(combinedGroup);
        try {
            File metadataFile = toMetadataFile(combinedGroup.getLocationPrefix());
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
        return new File(metadataDirectory, getS3Bucket(location) + "/" + getS3ObjectKey(location) + ".metadata");
    }

    private CombinedGroup readMetadataFile(URI location)
    {
        File metadataFile = toMetadataFile(location);
        if (!metadataFile.exists()) {
            return null;
        }
        try {
            String json = Files.toString(metadataFile, Charsets.UTF_8);
            return jsonCodec.fromJson(json);
        }
        catch (IOException e) {
            throw new RuntimeException("Metadata file for " + location + " is corrupt");
        }
    }
}
