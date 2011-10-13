package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.proofpoint.collector.calligraphus.EventPartition;
import com.proofpoint.collector.calligraphus.ServerConfig;
import com.proofpoint.json.JsonCodec;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class FileSystemCombineObjectMetadataStore
        implements CombineObjectMetadataStore
{
    private final JsonCodec<CombinedGroup> jsonCodec = JsonCodec.jsonCodec(CombinedGroup.class);
    private final File metadataDirectory;

    @Inject
    public FileSystemCombineObjectMetadataStore(ServerConfig config)
    {
        this.metadataDirectory = config.getCombinerMetadataDirectory();
    }

    public FileSystemCombineObjectMetadataStore(File metadataDirectory)
    {
        this.metadataDirectory = metadataDirectory;
    }

    @Override
    public CombinedGroup getCombinedGroupManifest(EventPartition eventPartition, String sizeName)
    {
        return readMetadataFile(eventPartition, sizeName);
    }

    @Override
    public boolean replaceCombinedGroupManifest(EventPartition eventPartition, String sizeName, CombinedGroup currentGroup, CombinedGroup newGroup) {
        checkNotNull(currentGroup, "currentGroup is null");
        checkNotNull(newGroup, "newGroup is null");
        checkArgument(currentGroup.getLocationPrefix().equals(newGroup.getLocationPrefix()), "newGroup location is different from currentGroup location");

        CombinedGroup persistentGroup = readMetadataFile(eventPartition, sizeName);
        if (persistentGroup != null) {
            if (persistentGroup.getVersion() != currentGroup.getVersion()) {
                return false;
            }
        }
        else if (currentGroup.getVersion() != 0) {
            return false;
        }

        return writeMetadataFile(eventPartition, sizeName, newGroup);
    }

    private boolean writeMetadataFile(EventPartition eventPartition, String sizeName, CombinedGroup combinedGroup)
    {
        String json = jsonCodec.toJson(combinedGroup);
        try {
            File metadataFile = toMetadataFile(eventPartition, sizeName);
            metadataFile.getParentFile().mkdirs();
            Files.write(json, metadataFile, Charsets.UTF_8);
            return true;
        }
        catch (IOException e) {
            return false;
        }
    }

    private File toMetadataFile(EventPartition eventPartition, String sizeName)
    {
        return new File(metadataDirectory, eventPartition.getEventType() + "/" +
                eventPartition.getMajorTimeBucket() + "/" +
                eventPartition.getMinorTimeBucket() +"."+ sizeName + ".metadata");
    }


    private CombinedGroup readMetadataFile(EventPartition eventPartition, String sizeName)
    {
        File metadataFile = toMetadataFile(eventPartition, sizeName);
        if (!metadataFile.exists()) {
            return null;
        }
        try {
            String json = Files.toString(metadataFile, Charsets.UTF_8);
            return jsonCodec.fromJson(json);
        }
        catch (IOException e) {
            throw new RuntimeException("Metadata at " + metadataFile + " file for " + eventPartition + " " + sizeName + " is corrupt");
        }
    }
}
