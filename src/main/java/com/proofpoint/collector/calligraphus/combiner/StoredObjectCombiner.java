package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proofpoint.collector.calligraphus.ServerConfig;
import com.proofpoint.experimental.units.DataSize;
import com.proofpoint.log.Logger;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.units.Duration;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.appendSuffix;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.buildS3Location;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3Directory;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3FileName;
import static java.lang.System.currentTimeMillis;

public class StoredObjectCombiner
{
    private static final Logger log = Logger.get(StoredObjectCombiner.class);

    private static final Duration CHECK_DELAY = new Duration(10, TimeUnit.SECONDS);

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("StoredObjectCombiner-%s").build());

    private final Set<URI> badManifests = new ConcurrentSkipListSet<URI>();

    private final String nodeId;
    private final CombineObjectMetadataStore metadataStore;
    private final StorageSystem storageSystem;
    private final URI stagingBaseUri;
    private final URI targetBaseUri;
    private final long targetFileSize;
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
        this.targetFileSize = config.getTargetFileSize().toBytes();
        this.enabled = config.isCombinerEnabled();
    }

    public StoredObjectCombiner(String nodeId,
            CombineObjectMetadataStore metadataStore,
            StorageSystem storageSystem,
            URI stagingBaseUri,
            URI targetBaseUri,
            DataSize targetFileSize,
            boolean enabled)
    {
        Preconditions.checkNotNull(nodeId, "nodeId is null");
        Preconditions.checkNotNull(metadataStore, "metadataStore is null");
        Preconditions.checkNotNull(storageSystem, "storageSystem is null");
        Preconditions.checkNotNull(stagingBaseUri, "stagingBaseUri is null");
        Preconditions.checkNotNull(targetBaseUri, "targetBaseUri is null");
        Preconditions.checkNotNull(targetFileSize, "targetFileSize is null");

        this.nodeId = nodeId;
        this.metadataStore = metadataStore;
        this.storageSystem = storageSystem;
        this.stagingBaseUri = stagingBaseUri;
        this.targetBaseUri = targetBaseUri;
        this.targetFileSize = targetFileSize.toBytes();
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

    @Managed
    public Set<URI> getBadManifests()
    {
        return badManifests;
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
        // split files into small and large groups based on size
        List<StoredObject> smallFiles = Lists.newArrayListWithCapacity(stagedObjects.size());
        List<StoredObject> largeFiles = Lists.newArrayListWithCapacity(stagedObjects.size());
        for (StoredObject stagedObject : stagedObjects) {
            if (stagedObject.getSize() == 0) {
                // ignore empty object
            }
            else if (stagedObject.getSize() < 5 * 1024 * 1024) {
                smallFiles.add(stagedObject);
            }
            else {
                largeFiles.add(stagedObject);
            }
        }
        combineObjectGroup(appendSuffix(baseURI, "small"), smallFiles);
        combineObjectGroup(appendSuffix(baseURI, "large"), largeFiles);
    }

    private void combineObjectGroup(URI baseURI, List<StoredObject> stagedObjects)
    {
        if (stagedObjects.isEmpty()) {
            return;
        }

        CombinedGroup currentGroup = metadataStore.getCombinedGroupManifest(baseURI);

        // only update the object if this node was the last writer or some time has passed
        if ((!nodeId.equals(currentGroup.getCreator())) && !groupIsMinutesOld(currentGroup, 5)) {
            return;
        }

        // create new manifest from existing manifest with new staged objects
        CombinedGroup newGroup = buildCombinedGroup(currentGroup, stagedObjects);

        // attempt to write new manifest
        if (!metadataStore.replaceCombinedGroupManifest(currentGroup, newGroup)) {
            return;
        }

        // get list of existing combined files on target
        Map<URI, StoredObject> existingObjects = newHashMap();
        for (StoredObject object : storageSystem.listObjects(getS3Directory(baseURI))) {
            existingObjects.put(object.getLocation(), object);
        }

        // execute combines for any objects that don't exist or match manifest size
        for (CombinedStoredObject newObject : newGroup.getCombinedObjects()) {
            // only combine if all source parts are available
            if (allPartsAvailable(stagedObjects, newObject.getSourceParts())) {
                StoredObject existingObject = existingObjects.get(newObject.getLocation());
                if ((existingObject == null) || (existingObject.getSize() != newObject.getSize())) {
                    createCombinedObject(newObject);
                }
            }
        }
    }

    private CombinedGroup buildCombinedGroup(CombinedGroup group, List<StoredObject> stagedObjects)
    {
        // get all objects that have already been combined
        Set<StoredObject> alreadyCombinedObjects = newHashSet();
        for (CombinedStoredObject combinedObject : group.getCombinedObjects()) {
            alreadyCombinedObjects.addAll(combinedObject.getSourceParts());
        }

        // get new objects that still need to be assigned to a combined object
        List<StoredObject> newObjects = getNewObjects(stagedObjects, alreadyCombinedObjects);

        // add each new object to a combined object
        for (StoredObject newObject : newObjects) {
            boolean added = false;

            // try to find an open combined object
            for (CombinedStoredObject combinedObject : group.getCombinedObjects()) {
                // skip if combined object is at target size
                if (combinedObject.getSize() >= targetFileSize) {
                    continue;
                }

                // skip if any parts are no longer available
                if (!allPartsAvailable(stagedObjects, combinedObject.getSourceParts())) {
                    continue;
                }

                // skip if objects in staging do not match the current combined object
                if (!stagedObjects.containsAll(combinedObject.getSourceParts())) {
                    if (badManifests.add(group.getLocationPrefix())) {
                        log.error("manifest source objects do not match objects in staging area: %s", group.getLocationPrefix());
                    }
                    continue;
                }

                // add object to combined object
                group = group.updateCombinedObject(nodeId, combinedObject.addPart(newObject));
                added = true;
                break;
            }

            // create new combined object if necessary
            if (!added) {
                group = group.addNewCombinedObject(nodeId, ImmutableList.of(newObject));
            }
        }

        return group;
    }

    private void createCombinedObject(CombinedStoredObject object)
    {
        try {
            storageSystem.createCombinedObject(object);
        }
        catch (Exception e) {
            log.error(e, "create combined object failed");
        }
    }

    private static List<StoredObject> getNewObjects(List<StoredObject> stagedObjects, Set<StoredObject> alreadyCombinedObjects)
    {
        Set<URI> combined = newHashSet(Iterables.transform(alreadyCombinedObjects, StoredObject.GET_LOCATION_FUNCTION));
        List<StoredObject> newObjects = Lists.newArrayList();
        for (StoredObject stagedObject : stagedObjects) {
            if (!combined.contains(stagedObject.getLocation())) {
                newObjects.add(stagedObject);
            }
        }
        return newObjects;
    }

    private static boolean allPartsAvailable(Collection<StoredObject> stagedObjects, Collection<StoredObject> sourceParts)
    {
        Collection<URI> stagedNames = Collections2.transform(stagedObjects, StoredObject.GET_LOCATION_FUNCTION);
        Collection<URI> sourceNames = Collections2.transform(sourceParts, StoredObject.GET_LOCATION_FUNCTION);
        return stagedNames.containsAll(sourceNames);
    }

    private static boolean groupIsMinutesOld(CombinedGroup group, int minutes)
    {
        return (currentTimeMillis() - group.getUpdatedTimestamp()) <= TimeUnit.MINUTES.toMillis(minutes);
    }
}
