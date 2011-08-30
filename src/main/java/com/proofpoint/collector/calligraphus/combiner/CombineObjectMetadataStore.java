package com.proofpoint.collector.calligraphus.combiner;

import java.util.List;

public interface CombineObjectMetadataStore
{
    CombinedStoredObject getCombinedObjectManifest(StorageArea stagingArea, StorageArea targetArea);

    boolean replaceCombinedObjectManifest(CombinedStoredObject currentCombinedObject, List<StoredObject> newCombinedObjectParts);
}
