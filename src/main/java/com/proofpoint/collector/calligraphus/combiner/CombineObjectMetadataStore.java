package com.proofpoint.collector.calligraphus.combiner;

import java.net.URI;

public interface CombineObjectMetadataStore
{
    CombinedGroup getCombinedGroupManifest(URI combinedGroupPrefix);

    boolean replaceCombinedGroupManifest(CombinedGroup currentGroup, CombinedGroup newGroup);
}
