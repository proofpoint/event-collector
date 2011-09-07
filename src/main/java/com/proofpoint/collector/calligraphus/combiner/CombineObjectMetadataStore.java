package com.proofpoint.collector.calligraphus.combiner;

import java.net.URI;
import java.util.List;

public interface CombineObjectMetadataStore
{
    CombinedStoredObject getCombinedObjectManifest(URI combinedObjectLocation);

    boolean replaceCombinedObjectManifest(CombinedStoredObject currentCombinedObject, List<StoredObject> newCombinedObjectParts);
}
