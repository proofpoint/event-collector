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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CopyPartRequest;
import com.amazonaws.services.s3.model.CopyPartResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.ProgressEvent;
import com.amazonaws.services.s3.model.ProgressListener;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.proofpoint.log.Logger;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.collect.Lists.newArrayList;
import static com.proofpoint.event.collector.combiner.S3StorageHelper.buildS3Location;
import static com.proofpoint.event.collector.combiner.S3StorageHelper.getS3Bucket;
import static com.proofpoint.event.collector.combiner.S3StorageHelper.getS3ObjectKey;
import static com.proofpoint.event.collector.combiner.S3StorageHelper.updateStoredObject;

public class S3StorageSystem
        implements StorageSystem
{
    private static final Logger log = Logger.get(S3StorageSystem.class);
    private final TransferManager s3TransferManager;
    private final AmazonS3 s3Service;

    @Inject
    public S3StorageSystem(AmazonS3 s3Service, TransferManager s3TransferManager)
    {
        Preconditions.checkNotNull(s3Service, "s3Service is null");
        this.s3Service = s3Service;
        this.s3TransferManager = s3TransferManager;
    }

    @Override
    public List<URI> listDirectories(URI storageArea)
    {
        S3StorageHelper.checkValidS3Uri(storageArea);

        String bucket = getS3Bucket(storageArea);
        String key = getS3ObjectKey(storageArea);
        Iterator<String> iter = new S3PrefixListing(s3Service,
                new ListObjectsRequest(bucket, key, null, "/", null)).iterator();

        ImmutableList.Builder<URI> builder = ImmutableList.builder();
        while (iter.hasNext()) {
            builder.add(buildS3Location("s3://", bucket, iter.next()));
        }
        return builder.build();
    }

    @Override
    public List<StoredObject> listObjects(URI storageArea)
    {
        S3StorageHelper.checkValidS3Uri(storageArea);

        String s3Path = getS3ObjectKey(storageArea);
        Iterator<S3ObjectSummary> iter = new S3ObjectListing(s3Service,
                new ListObjectsRequest(getS3Bucket(storageArea), s3Path, null, "/", null)).iterator();

        ImmutableList.Builder<StoredObject> builder = ImmutableList.builder();
        while (iter.hasNext()) {
            S3ObjectSummary summary = iter.next();
            builder.add(new StoredObject(
                    buildS3Location(storageArea, summary.getKey().substring(s3Path.length())),
                    summary.getETag(),
                    summary.getSize(),
                    summary.getLastModified().getTime()));
        }
        return builder.build();
    }

    @Override
    public StoredObject createCombinedObject(CombinedStoredObject combinedObject)
    {
        Preconditions.checkNotNull(combinedObject, "combinedObject is null");
        Preconditions.checkArgument(!combinedObject.getSourceParts().isEmpty(), "combinedObject sourceParts is empty");

        boolean setIsSmall = combinedObject.getSourceParts().get(0).getSize() < 5 * 1024 * 1024;

        // verify size
        for (StoredObject newCombinedObjectPart : combinedObject.getSourceParts()) {
            boolean fileIsSmall = newCombinedObjectPart.getSize() < 5 * 1024 * 1024;
            Preconditions.checkArgument(fileIsSmall == setIsSmall, "combinedObject sourceParts contains mixed large and small files");
        }

        return setIsSmall ? createCombinedObjectSmall(combinedObject) : createCombinedObjectLarge(combinedObject);
    }

    private StoredObject createCombinedObjectLarge(CombinedStoredObject combinedObject)
    {
        URI location = combinedObject.getLocation();
        log.info("starting multipart upload: %s", location);

        String bucket = getS3Bucket(location);
        String key = getS3ObjectKey(location);

        String uploadId = s3Service.initiateMultipartUpload(
                new InitiateMultipartUploadRequest(bucket, key)).getUploadId();

        try {
            List<PartETag> parts = newArrayList();
            int partNumber = 1;
            for (StoredObject newCombinedObjectPart : combinedObject.getSourceParts()) {
                CopyPartResult part = s3Service.copyPart(new CopyPartRequest()
                        .withUploadId(uploadId)
                        .withPartNumber(partNumber)
                        .withDestinationBucketName(bucket)
                        .withDestinationKey(key)
                        .withSourceBucketName(getS3Bucket(newCombinedObjectPart.getLocation()))
                        .withSourceKey(getS3ObjectKey(newCombinedObjectPart.getLocation()))
                );
                parts.add(new PartETag(partNumber, part.getETag()));
                partNumber++;
            }

            String etag = s3Service.completeMultipartUpload(
                    new CompleteMultipartUploadRequest(bucket, key, uploadId, parts)).getETag();

            ObjectMetadata newObject = s3Service.getObjectMetadata(bucket, key);
            log.info("completed multipart upload: %s", location);

            if (!etag.equals(newObject.getETag())) {
                // this might happen in rare cases due to S3's eventual consistency
                throw new IllegalStateException("completed etag is different from combined object etag");
            }

            return updateStoredObject(location, newObject);
        }
        catch (AmazonClientException e) {
            try {
                s3Service.abortMultipartUpload(new AbortMultipartUploadRequest(bucket, key, uploadId));
            }
            catch (AmazonClientException ignored) {
            }
            throw Throwables.propagate(e);
        }
    }

    private StoredObject createCombinedObjectSmall(CombinedStoredObject combinedObject)
    {
        ImmutableList.Builder<ByteSource> builder = ImmutableList.builder();
        List<URI> sourceParts = Lists.transform(combinedObject.getSourceParts(), StoredObject.GET_LOCATION_FUNCTION);
        for (URI sourcePart : sourceParts) {
            builder.add(getInputSupplier(sourcePart));
        }
        ByteSource source = ByteSource.concat(builder.build());

        File tempFile = null;
        try {
            tempFile = File.createTempFile(S3StorageHelper.getS3FileName(combinedObject.getLocation()), ".small.s3.data");
            source.copyTo(Files.asByteSink(tempFile));
            StoredObject result = putObject(combinedObject.getLocation(), tempFile);
            return result;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            if (tempFile != null) {
                tempFile.delete();
            }
        }
    }

    @Override
    public StoredObject putObject(final URI location, File source)
    {
        try {
            log.info("starting upload: %s", location);
            final AtomicLong totalTransferred = new AtomicLong();
            Upload upload = s3TransferManager.upload(getS3Bucket(location), getS3ObjectKey(location), source);
            upload.addProgressListener(new ProgressListener()
            {
                @Override
                public void progressChanged(ProgressEvent progressEvent)
                {
                    // NOTE: This may be invoked by multiple threads.
                    long transferred = totalTransferred.addAndGet(progressEvent.getBytesTransferred());
                    log.debug("upload progress: %s: transferred=%d code=%d", location, transferred, progressEvent.getEventCode());
                }
            });
            UploadResult uploadResult = upload.waitForUploadResult();
            ObjectMetadata metadata = s3Service.getObjectMetadata(getS3Bucket(location), getS3ObjectKey(location));
            if (!uploadResult.getETag().equals(metadata.getETag())) {
                // this might happen in rare cases due to S3's eventual consistency
                throw new IllegalStateException("uploaded etag is different from retrieved object etag");
            }
            log.info("completed upload: %s (size=%d bytes)", location, totalTransferred.get());
            return updateStoredObject(location, metadata);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public StoredObject getObjectDetails(URI target)
    {
        StoredObject storedObject = new StoredObject(target);
        ObjectMetadata metadata = s3Service.getObjectMetadata(getS3Bucket(target), getS3ObjectKey(target));
        return updateStoredObject(storedObject.getLocation(), metadata);
    }

    public ByteSource getInputSupplier(URI target)
    {
        return new S3InputSupplier(s3Service, target);
    }
}
