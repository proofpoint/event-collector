/*
 * Copyright 2011-2013 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.proofpoint.event.collector.combiner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.proofpoint.event.client.EventClient;
import com.proofpoint.event.collector.EventPartition;
import com.proofpoint.event.collector.ServerConfig;
import com.proofpoint.experimental.units.DataSize;
import com.proofpoint.log.Logger;
import com.proofpoint.node.NodeInfo;
import org.joda.time.DateMidnight;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.weakref.jmx.Managed;

import javax.inject.Inject;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.proofpoint.event.collector.combiner.CombinedGroup.createInitialCombinedGroup;
import static com.proofpoint.event.collector.combiner.S3StorageHelper.appendSuffix;
import static com.proofpoint.event.collector.combiner.S3StorageHelper.buildS3Location;
import static com.proofpoint.event.collector.combiner.S3StorageHelper.getS3Directory;
import static com.proofpoint.event.collector.combiner.S3StorageHelper.getS3FileName;
import static java.lang.System.currentTimeMillis;
import static org.joda.time.DateTimeZone.UTC;

public class StoredObjectCombiner
{
    private static final Logger log = Logger.get(StoredObjectCombiner.class);

    private static final DataSize S3_MINIMUM_COMBINABLE_SIZE = new DataSize(5, DataSize.Unit.MEGABYTE);

    private static final DateTimeFormatter DATE_FORMAT = ISODateTimeFormat.date().withZone(UTC);

    private final Set<URI> badManifests = new ConcurrentSkipListSet<URI>();

    private final String nodeId;
    private final CombineObjectMetadataStore metadataStore;
    private final StorageSystem storageSystem;
    private final EventClient eventClient;
    private final URI stagingBaseUri;
    private final URI targetBaseUri;
    private final long targetFileSize;
    private final boolean ignoreErrors;
    private final boolean disableStartEndFiltering;
    private final int startDaysAgo;
    private final int endDaysAgo;
    private final String groupId;

    @Inject
    public StoredObjectCombiner(
            NodeInfo nodeInfo,
            CombineObjectMetadataStore metadataStore,
            StorageSystem storageSystem,
            ServerConfig config,
            EventClient eventClient)
    {
        Preconditions.checkNotNull(nodeInfo, "nodeInfo is null");
        Preconditions.checkNotNull(metadataStore, "metadataStore is null");
        Preconditions.checkNotNull(storageSystem, "storageSystem is null");
        Preconditions.checkNotNull(eventClient, "eventClient is null");
        Preconditions.checkNotNull(config, "config is null");
        Preconditions.checkArgument(config.isCombinerStartEndDaysSane(), "combinerStartDaysAgo must be greater than endDaysAgo");

        this.nodeId = nodeInfo.getNodeId();
        this.metadataStore = metadataStore;
        this.storageSystem = storageSystem;
        this.eventClient = eventClient;
        this.stagingBaseUri = URI.create(config.getS3StagingLocation());
        this.targetBaseUri = URI.create(config.getS3DataLocation());
        this.targetFileSize = config.getTargetFileSize().toBytes();
        this.ignoreErrors = true;
        this.disableStartEndFiltering = config.isCombinerDateRangeLimitDisabled();
        this.startDaysAgo = config.getCombinerStartDaysAgo();
        this.endDaysAgo = config.getCombinerEndDaysAgo();
        this.groupId = config.getCombinerGroupId();
    }

    public StoredObjectCombiner(
            String nodeId,
            CombineObjectMetadataStore metadataStore,
            StorageSystem storageSystem,
            EventClient eventClient,
            URI stagingBaseUri,
            URI targetBaseUri,
            DataSize targetFileSize,
            int startDaysAgo,
            int endDaysAgo,
            String groupId)
    {
        Preconditions.checkNotNull(nodeId, "nodeId is null");
        Preconditions.checkNotNull(metadataStore, "metadataStore is null");
        Preconditions.checkNotNull(storageSystem, "storageSystem is null");
        Preconditions.checkNotNull(eventClient, "eventClient is null");
        Preconditions.checkNotNull(stagingBaseUri, "stagingBaseUri is null");
        Preconditions.checkNotNull(targetBaseUri, "targetBaseUri is null");
        Preconditions.checkNotNull(targetFileSize, "targetFileSize is null");
        Preconditions.checkArgument(startDaysAgo > endDaysAgo, "startDaysAgo must be greater than endDaysAgo");

        this.nodeId = nodeId;
        this.metadataStore = metadataStore;
        this.storageSystem = storageSystem;
        this.eventClient = eventClient;
        this.stagingBaseUri = stagingBaseUri;
        this.targetBaseUri = targetBaseUri;
        this.targetFileSize = targetFileSize.toBytes();
        this.ignoreErrors = false;
        this.startDaysAgo = startDaysAgo;
        this.endDaysAgo = endDaysAgo;
        this.disableStartEndFiltering = false;
        this.groupId = groupId;
    }

    @Managed
    public Set<URI> getBadManifests()
    {
        return badManifests;
    }

    /**
     * Iterate over all event partitions, find staged objects in each partition,
     * then call {@link #combineObjects} to combine them.
     */
    public void combineAllObjects()
    {
        String startDate = createPartitionForDate(getStartDate());
        String endDate = createPartitionForDate(getEndDate());
        for (URI eventTypeBaseUri : storageSystem.listDirectories(stagingBaseUri)) {
            combineObjects(eventTypeBaseUri, startDate, endDate);
        }
    }

    /**
     * Split staged objects into small and large groups based on size,
     * then call {@link #combineObjectGroup} to combine the group.
     * We need two groups because we want to perform server-side combines,
     * but S3 has a minimum allowable size for that feature.
     *
     * @param eventPartition the event partition to combine
     * @param baseURI base target filename
     * @param stagedObjects list of all staged objects in partition
     */
    public void combineObjects(EventPartition eventPartition, URI baseURI, List<StoredObject> stagedObjects)
    {
        List<StoredObject> smallFiles = Lists.newArrayListWithCapacity(stagedObjects.size());
        List<StoredObject> largeFiles = Lists.newArrayListWithCapacity(stagedObjects.size());
        for (StoredObject stagedObject : stagedObjects) {
            if (stagedObject.getSize() == 0) {
                // ignore empty object
            }
            else if (stagedObject.getSize() < S3_MINIMUM_COMBINABLE_SIZE.toBytes()) {
                smallFiles.add(stagedObject);
            }
            else {
                largeFiles.add(stagedObject);
            }
        }
        combineObjectGroup(eventPartition, "small", baseURI, smallFiles);
        combineObjectGroup(eventPartition, "large", baseURI, largeFiles);
    }

    /**
     * Combine objects of a single type that fall between two dates.
     *
     * @param eventTypeBaseUri The base URI containing all of the events of a single type to combine.
     * @param startDate The earliest date of events to combine.
     * @param endDate The latest date of events to combine.
     */
    private void combineObjects(URI eventTypeBaseUri, String startDate, String endDate)
    {

        String eventType = getS3FileName(eventTypeBaseUri);
        log.info("starting combining objects of type %s", eventType);
        for (URI timeSliceBaseUri : storageSystem.listDirectories(eventTypeBaseUri)) {
            String dateBucket = getS3FileName(timeSliceBaseUri);
            if (!disableStartEndFiltering) {
                if (olderThanThreshold(dateBucket, startDate) || newerThanOrEqualToThreshold(dateBucket, endDate)) {
                    continue;
                }
            }
            for (URI hourBaseUri : storageSystem.listDirectories(timeSliceBaseUri)) {
                log.info("combining staging bucket: %s", hourBaseUri);
                String hour = getS3FileName(hourBaseUri);
                URI stagingArea = buildS3Location(timeSliceBaseUri, hour + "/");
                List<StoredObject> stagedObjects = storageSystem.listObjects(stagingArea);
                if (!stagedObjects.isEmpty()) {
                    EventPartition eventPartition = new EventPartition(eventType, dateBucket, hour);
                    URI targetObjectLocation = buildS3Location(targetBaseUri, eventType, dateBucket, hour);
                    combineObjects(eventPartition, targetObjectLocation, stagedObjects);
                }
            }
        }
        log.info("finished combining objects of type %s", eventType);
        eventClient.post(new CombineCompleted(groupId, eventType));
    }

    /**
     * Add new objects to the manifest using {@link #buildCombinedGroup},
     * write the updated manifest, determine what targets need updating
     * and perform the combines using {@link #createCombinedObject}.
     *
     * @param eventPartition the event partition to combine
     * @param sizeName either "small" or "large"
     * @param baseURI base target filename
     * @param stagedObjects list of all staged objects in partition for size group
     */
    private void combineObjectGroup(EventPartition eventPartition, String sizeName, URI baseURI, List<StoredObject> stagedObjects)
    {
        if (stagedObjects.isEmpty()) {
            return;
        }
        baseURI = appendSuffix(baseURI, sizeName);

        CombinedGroup currentGroup = metadataStore.getCombinedGroupManifest(eventPartition, sizeName);
        if (currentGroup == null) {
            currentGroup = createInitialCombinedGroup(baseURI, nodeId);
            log.info("creating new combined group: %s", currentGroup);
        }

        // only update the object if this node was the last writer or some time has passed
        if ((!nodeId.equals(currentGroup.getCreator())) && !groupIsMinutesOld(currentGroup, 5)) {
            log.warn("this node cannot update this group: %s", currentGroup);
            return;
        }

        // create new manifest from existing manifest with new staged objects
        CombinedGroup newGroup = buildCombinedGroup(currentGroup, stagedObjects);

        // attempt to write new manifest
        if (!metadataStore.replaceCombinedGroupManifest(eventPartition, sizeName, currentGroup, newGroup)) {
            log.warn("failed to write manifest: %s.%s", eventPartition, sizeName);
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
                    log.info("creating combined object: %s", newObject);
                    createCombinedObject(newObject);
                }
            }
        }
    }

    /**
     * Add new staged objects to the existing manifest and return the new
     * manifest. Staged objects are added to an existing combined object
     * if possible, otherwise a new one is created.
     *
     * @param group the existing manifest
     * @param stagedObjects list of staged objects to add to manifest
     * @return the new manifest
     */
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
                if (!containsAll(stagedObjects, combinedObject.getSourceParts())) {
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
        if (ignoreErrors) {
            createCombinedObjectIgnoringErrors(object);
        }
        else {
            storageSystem.createCombinedObject(object);
        }
    }

    private void createCombinedObjectIgnoringErrors(CombinedStoredObject object)
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
        return containsAll(stagedNames, sourceNames);
    }

    private static boolean containsAll(Iterable<?> source, Iterable<?> target)
    {
        return ImmutableSet.copyOf(source).containsAll(ImmutableSet.copyOf(target));
    }

    private static boolean groupIsMinutesOld(CombinedGroup group, int minutes)
    {
        return (currentTimeMillis() - group.getUpdatedTimestamp()) >= TimeUnit.MINUTES.toMillis(minutes);
    }

    private static boolean olderThanThreshold(String dateBucket, String thresholdDate)
    {
        return dateBucket.compareTo(thresholdDate) < 0;
    }

    private static boolean newerThanOrEqualToThreshold(String dateBucket, String thresholdDate)
    {
        return dateBucket.compareTo(thresholdDate) >= 0;
    }

    private String createPartitionForDate(DateMidnight startDateMidnight)
    {
        return DATE_FORMAT.print(startDateMidnight);
    }

    private DateMidnight getEndDate()
    {
        return DateMidnight.now(UTC).minusDays(endDaysAgo);
    }

    private DateMidnight getStartDate()
    {
        return DateMidnight.now(UTC).minusDays(startDaysAgo);
    }
}
