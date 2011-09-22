package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proofpoint.collector.calligraphus.ServerConfig;
import com.proofpoint.log.Logger;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.buildS3Location;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3FileName;

public class StoredObjectCombiner
{
    private static final Logger log = Logger.get(StoredObjectCombiner.class);

    private static final Duration CHECK_DELAY = new Duration(10, TimeUnit.SECONDS);

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("S3ObjectCombiner-%s").build());

    private final String nodeId;
    private final CombineObjectMetadataStore metadataStore;
    private final StorageSystem storageSystem;
    private final URI stagingBaseUri;
    private final URI targetBaseUri;
    private final boolean enabled;

    @Inject
    public StoredObjectCombiner(NodeInfo nodeInfo, CombineObjectMetadataStore metadataStore, StorageSystem storageSystem, ServerConfig config)
    {
        Preconditions.checkNotNull(nodeInfo, "nodeInfo is null");
        Preconditions.checkNotNull(metadataStore, "metadataStore is null");
        Preconditions.checkNotNull(storageSystem, "storageSystem is null");
        Preconditions.checkNotNull(config, "config is null");

        this.nodeId = nodeInfo.getNodeId();
        this.metadataStore = metadataStore;
        this.storageSystem = storageSystem;
        this.stagingBaseUri = URI.create(config.getS3StagingLocation());
        this.targetBaseUri = URI.create(config.getS3DataLocation());
        this.enabled = config.isCombinerEnabled();
    }

    public StoredObjectCombiner(String nodeId, CombineObjectMetadataStore metadataStore, StorageSystem storageSystem, URI stagingBaseUri, URI targetBaseUri, boolean enabled)
    {
        Preconditions.checkNotNull(nodeId, "nodeId is null");
        Preconditions.checkNotNull(metadataStore, "metadataStore is null");
        Preconditions.checkNotNull(storageSystem, "storageSystem is null");
        Preconditions.checkNotNull(stagingBaseUri, "stagingBaseUri is null");
        Preconditions.checkNotNull(targetBaseUri, "targetBaseUri is null");

        this.nodeId = nodeId;
        this.metadataStore = metadataStore;
        this.storageSystem = storageSystem;
        this.stagingBaseUri = stagingBaseUri;
        this.targetBaseUri = targetBaseUri;
        this.enabled = enabled;
    }

    @PostConstruct
    public void start()
    {
        if (!enabled) {
            return;
        }
        Runnable combiner = new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    combineObjects();
                }
                catch (Exception e) {
                    log.error(e, "combine failed");
                }
            }
        };
        executor.scheduleAtFixedRate(combiner, 0, (long) CHECK_DELAY.toMillis(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void destroy()
            throws IOException
    {
        executor.shutdown();
    }

    public void combineObjects()
    {
        for (URI eventBaseUri : storageSystem.listDirectories(stagingBaseUri)) {
            for (URI timeSliceBaseUri : storageSystem.listDirectories(eventBaseUri)) {
                for (URI hourBaseUri : storageSystem.listDirectories(timeSliceBaseUri)) {
                    String hour = getS3FileName(hourBaseUri);
                    URI stagingArea = buildS3Location(timeSliceBaseUri, hour + "/");
                    List<StoredObject> stagedObjects = storageSystem.listObjects(stagingArea);
                    if (!stagedObjects.isEmpty()) {
                        URI targetObjectLocation = buildS3Location(targetBaseUri, getS3FileName(eventBaseUri), getS3FileName(timeSliceBaseUri), hour);
                        combineObjects(targetObjectLocation, stagedObjects);
                    }
                }
            }
        }
    }

    public void combineObjects(URI baseURI, List<StoredObject> stagedObjects)
    {
        // Split files based on size size
        List<StoredObject> smallFiles = Lists.newArrayListWithCapacity(stagedObjects.size());
        List<StoredObject> largeFiles = Lists.newArrayListWithCapacity(stagedObjects.size());
        for (StoredObject stagedObject : stagedObjects) {
            if (stagedObject.getSize() < 5 * 1024 * 1024) {
                smallFiles.add(stagedObject);
            } else {
                largeFiles.add(stagedObject);
            }
        }
        if (!smallFiles.isEmpty()) {
            URI targetObjectLocation = URI.create(baseURI.toString() + ".small.json.snappy");
            combineObjectSet(targetObjectLocation, smallFiles);
        }
        if (!largeFiles.isEmpty()) {
            URI targetObjectLocation = URI.create(baseURI.toString() + ".large.json.snappy");
            combineObjectSet(targetObjectLocation, largeFiles);
        }
    }

    private CombinedStoredObject combineObjectSet(URI targetObjectLocation, List<StoredObject> stagedObjects)
    {
        CombinedStoredObject currentCombinedObject;
        CombinedStoredObject newCombinedObject;
        do {
            // gets the file names
            List<String> stagedObjectNames = Lists.transform(stagedObjects, StoredObject.GET_NAME_FUNCTION);

            // Only update the object if the this node is was the last writer or 5 minutes have passed
            currentCombinedObject = metadataStore.getCombinedObjectManifest(targetObjectLocation);
            if (!nodeId.equals(currentCombinedObject.getCreator()) && System.currentTimeMillis() - currentCombinedObject.getCreatedTimestamp() <= TimeUnit.MINUTES.toMillis(5)) {
                return null;
            }

            // get list of objects that makeup current targetCombinedObject
            List<StoredObject> currentCombinedObjectManifest = currentCombinedObject.getSourceParts();
            List<String> existingCombinedObjectPartNames = Lists.transform(currentCombinedObjectManifest, StoredObject.GET_NAME_FUNCTION);

            // if files already combined, return
            if (newHashSet(existingCombinedObjectPartNames).equals(newHashSet(stagedObjectNames))) {
                return null;
            }

            // GOTO late data handling: if objects in staging does NOT contain ANY objects in the current targetCombinedObject
            if (!currentCombinedObjectManifest.isEmpty() && Collections.disjoint(existingCombinedObjectPartNames, stagedObjectNames)) {
                processLateData(targetObjectLocation, stagedObjects);
                return null;
            }

            // RETRY later: if objects in staging do NOT contain all objects in the current targetCombinedObject (eventually consistent)
            if (!stagedObjectNames.containsAll(existingCombinedObjectPartNames)) {
                // retry later
                return null;
            }

            // ERROR: if objects in staging do NOT have the same md5s in the current targetCombinedObject
            if (!stagedObjects.containsAll(currentCombinedObjectManifest)) {
                throw new IllegalStateException(String.format("MD5 hashes for combined objects in %s do not match MD5 hashes in staging area",
                        targetObjectLocation));
            }

            // newObjectList = current targetCombinedObject list + new objects not contained in this list
            List<StoredObject> missingObjects = newArrayList(stagedObjects);
            missingObjects.removeAll(currentCombinedObjectManifest);
            List<StoredObject> newCombinedObjectParts = ImmutableList.<StoredObject>builder().addAll(currentCombinedObjectManifest).addAll(missingObjects).build();

            newCombinedObject = currentCombinedObject.update(nodeId, newCombinedObjectParts);

            // write new combined object manifest
        } while (!metadataStore.replaceCombinedObjectManifest(currentCombinedObject, newCombinedObject));

        // perform combination
        storageSystem.createCombinedObject(newCombinedObject.getLocation(), newCombinedObject.getSourceParts());
        return newCombinedObject;
    }

    private void processLateData(URI targetArea, List<StoredObject> stagedObjects)
    {
        // todo do something :)
    }
}
