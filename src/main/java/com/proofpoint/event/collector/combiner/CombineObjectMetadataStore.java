package com.proofpoint.event.collector.combiner;

import com.proofpoint.event.collector.EventPartition;

public interface CombineObjectMetadataStore
{
    CombinedGroup getCombinedGroupManifest(EventPartition eventPartition, String sizeName);

    boolean replaceCombinedGroupManifest(EventPartition eventPartition, String sizeName, CombinedGroup currentGroup, CombinedGroup newGroup);
}
