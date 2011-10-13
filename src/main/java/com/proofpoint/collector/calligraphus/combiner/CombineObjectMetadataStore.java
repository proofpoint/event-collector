package com.proofpoint.collector.calligraphus.combiner;

import com.proofpoint.collector.calligraphus.EventPartition;

public interface CombineObjectMetadataStore
{
    CombinedGroup getCombinedGroupManifest(EventPartition eventPartition, String sizeName);

    boolean replaceCombinedGroupManifest(EventPartition eventPartition, String sizeName, CombinedGroup currentGroup, CombinedGroup newGroup);
}
