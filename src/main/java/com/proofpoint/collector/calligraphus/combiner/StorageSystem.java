package com.proofpoint.collector.calligraphus.combiner;

import java.util.List;

public interface StorageSystem
{
    List<StoredObject> listObjects(StorageArea storageArea);

    StoredObject createCombinedObject(StoredObject target, List<StoredObject> newCombinedObjectParts);
}
