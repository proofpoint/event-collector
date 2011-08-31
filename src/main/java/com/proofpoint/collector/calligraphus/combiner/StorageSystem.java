package com.proofpoint.collector.calligraphus.combiner;

import java.net.URI;
import java.util.List;

public interface StorageSystem
{
    List<StoredObject> listObjects(URI storageArea);

    StoredObject createCombinedObject(StoredObject target, List<StoredObject> newCombinedObjectParts);
}
