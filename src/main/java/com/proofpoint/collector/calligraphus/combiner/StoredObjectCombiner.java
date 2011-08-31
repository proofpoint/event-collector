package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;

public class StoredObjectCombiner
{
    private final String nodeId;
    private final CombineObjectMetadataStore metadataStore;
    private final StorageSystem storageSystem;

    public StoredObjectCombiner(String nodeId, CombineObjectMetadataStore metadataStore, StorageSystem storageSystem)
    {
        this.nodeId = nodeId;
        this.metadataStore = metadataStore;
        this.storageSystem = storageSystem;
    }

    public CombinedStoredObject combineObjects(URI stagingArea, URI targetArea)
    {
        CombinedStoredObject currentCombinedObject;
        List<StoredObject> newCombinedObjectParts;
        do {
            // list objects in staging area
            List<StoredObject> stagedObjects = storageSystem.listObjects(stagingArea);
            List<String> stagedObjectNames = Lists.transform(stagedObjects, StoredObject.GET_NAME_FUNCTION);

            // Only update the object if the this node is was the last writer or 5 minutes have passed
            currentCombinedObject = metadataStore.getCombinedObjectManifest(stagingArea, targetArea);
            if (!nodeId.equals(currentCombinedObject.getCreator()) && System.currentTimeMillis() - currentCombinedObject.getCreatedTimestamp() <= TimeUnit.MINUTES.toMillis(5)) {
                return null;
            }

            // get list of objects that makeup current targetCombinedObject
            List<StoredObject> currentCombinedObjectManifest = currentCombinedObject.getSourceParts();
            List<String> existingCombinedObjectPartNames = Lists.transform(currentCombinedObjectManifest, StoredObject.GET_NAME_FUNCTION);

            // GOTO late data handling: if objects in staging are does NOT contain ANY objects in the current targetCombinedObject
            if (!currentCombinedObjectManifest.isEmpty() && Collections.disjoint(existingCombinedObjectPartNames, stagedObjectNames)) {
                processLateData(stagingArea, targetArea);
                return null;
            }

            // RETRY later: if objects in staging are does NOT contain all objects in the current targetCombinedObject (eventually consistent)
            if (!stagedObjectNames.containsAll(existingCombinedObjectPartNames)) {
                // retry later
                return null;
            }

            // ERROR: if objects in staging do NOT have the same md5s in the current targetCombinedObject
            if (!stagedObjects.containsAll(currentCombinedObjectManifest)) {
                throw new IllegalStateException(String.format("MD5 hashes for combined objects in %s do not match MD5 hashes in staging area",
                        currentCombinedObject.getName()));
            }

            // newObjectList = current targetCombinedObject list + new objects not contained in this list
            List<StoredObject> missingObjects = newArrayList(stagedObjects);
            missingObjects.removeAll(currentCombinedObjectManifest);
            newCombinedObjectParts = ImmutableList.<StoredObject>builder().addAll(currentCombinedObjectManifest).addAll(missingObjects).build();

            // write new combined object manifest
        } while (!metadataStore.replaceCombinedObjectManifest(currentCombinedObject, newCombinedObjectParts));

        // perform combination
        StoredObject combinedObject = storageSystem.createCombinedObject(currentCombinedObject.getStoredObject(), newCombinedObjectParts);
        return new CombinedStoredObject(combinedObject, nodeId, System.currentTimeMillis(), newCombinedObjectParts);
    }

    private void processLateData(URI stagingArea, URI targetArea)
    {
        // todo do something :)
    }
}
