package com.proofpoint.collector.calligraphus.combiner;

import java.net.URI;
import java.util.List;

public interface CombineObjectMetadataStore
{
    CombinedStoredObject getCombinedObjectManifest(URI stagingArea, URI targetArea);

    boolean replaceCombinedObjectManifest(CombinedStoredObject currentCombinedObject, List<StoredObject> newCombinedObjectParts);
}
