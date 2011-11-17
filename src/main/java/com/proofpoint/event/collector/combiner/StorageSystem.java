package com.proofpoint.event.collector.combiner;

import java.io.File;
import java.net.URI;
import java.util.List;

public interface StorageSystem
{
    List<URI> listDirectories(URI storageArea);

    List<StoredObject> listObjects(URI storageArea);

    StoredObject createCombinedObject(CombinedStoredObject combinedObject);

    StoredObject putObject(URI location, File source);
}
