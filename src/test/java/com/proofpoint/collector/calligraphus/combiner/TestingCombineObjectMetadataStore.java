package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.collect.Maps;

import java.net.URI;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.proofpoint.collector.calligraphus.combiner.CombinedGroup.createInitialCombinedGroup;

public class TestingCombineObjectMetadataStore
        implements CombineObjectMetadataStore
{
    private final String nodeId;
    private final ConcurrentMap<URI, CombinedGroup> metadata = Maps.newConcurrentMap();

    TestingCombineObjectMetadataStore(String nodeId)
    {
        this.nodeId = nodeId;
    }

    @Override
    public CombinedGroup getCombinedGroupManifest(URI combinedGroupPrefix)
    {
        checkNotNull(combinedGroupPrefix, "combinedGroupPrefix is null");

        CombinedGroup combinedGroup = metadata.get(combinedGroupPrefix);
        if (combinedGroup == null) {
            combinedGroup = createInitialCombinedGroup(combinedGroupPrefix, nodeId);
        }

        return combinedGroup;
    }

    @Override
    public boolean replaceCombinedGroupManifest(CombinedGroup currentGroup, CombinedGroup newGroup)
    {
        checkNotNull(currentGroup, "currentGroup is null");
        checkNotNull(newGroup, "newGroup is null");
        checkArgument(currentGroup.getLocationPrefix().equals(newGroup.getLocationPrefix()), "currentGroup location is different from newGroup location");

        CombinedGroup persistentGroup = metadata.get(newGroup.getLocationPrefix());
        if (persistentGroup != null) {
            if (persistentGroup.getVersion() != currentGroup.getVersion()) {
                return false;
            }
        }
        else if (currentGroup.getVersion() != 0) {
            return false;
        }

        URI location = currentGroup.getLocationPrefix();
        if (currentGroup.getVersion() == 0) {
            return metadata.putIfAbsent(location, newGroup) == null;
        }
        else {
            return metadata.replace(location, currentGroup, newGroup);
        }
    }
}
