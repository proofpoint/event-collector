package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.MultipartCompleted;
import org.jets3t.service.model.MultipartUpload;
import org.jets3t.service.model.S3Object;

import javax.inject.Inject;
import java.io.File;
import java.net.URI;
import java.util.List;

import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3Bucket;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3ObjectName;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3Path;

public class S3StorageSystem
        implements StorageSystem
{
    private final ExtendedRestS3Service s3Service;

    @Inject
    public S3StorageSystem(ExtendedRestS3Service s3Service)
    {
        Preconditions.checkNotNull(s3Service, "s3Service is null");
        this.s3Service = s3Service;
    }

    @Override
    public List<StoredObject> listObjects(URI storageArea)
    {
        S3StorageHelper.checkValidS3Uri(storageArea);

        try {
            String s3Path = getS3Path(storageArea);
            S3Object[] s3Objects = s3Service.listObjects(getS3Bucket(storageArea), s3Path, "/", 1000L);
            ImmutableList.Builder<StoredObject> builder = ImmutableList.builder();
            for (S3Object s3Object : s3Objects) {
                builder.add(new StoredObject(s3Object.getName().substring(s3Path.length()),
                        storageArea,
                        s3Object.getETag(),
                        s3Object.getContentLength(),
                        s3Object.getLastModifiedDate().getTime()));
            }
            return builder.build();
        }
        catch (S3ServiceException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public StoredObject createCombinedObject(StoredObject target, List<StoredObject> newCombinedObjectParts)
    {
        URI targetStorageArea = target.getStorageArea();
        MultipartUpload upload;
        try {
            upload = s3Service.multipartStartUpload(getS3Bucket(targetStorageArea), new S3Object(getS3ObjectName(target)));
        }
        catch (S3ServiceException e) {
            throw Throwables.propagate(e);
        }

        try {
            int partNumber = 1;
            for (StoredObject newCombinedObjectPart : newCombinedObjectParts) {
                URI sourceStorageArea = newCombinedObjectPart.getStorageArea();
                String sourceBucket = getS3Bucket(sourceStorageArea);
                String sourcePath = getS3Path(sourceStorageArea);

                s3Service.multipartUploadPartCopy(
                        upload,
                        partNumber,
                        sourceBucket,
                        sourcePath + newCombinedObjectPart.getName(),
                        null, // ifModifiedSince
                        null, // ifUnmodifiedSince
                        null, // ifMatchTags
                        null, // ifNoneMatchTags
                        null, // byteRangeStart
                        null, // byteRangeEnd
                        null); // versionId
                partNumber++;
            }

            MultipartCompleted completed = s3Service.multipartCompleteUpload(upload);
            S3Object combinedObject = s3Service.getObject(getS3Bucket(targetStorageArea), getS3ObjectName(target));

            if (!completed.getEtag().equals(combinedObject.getETag())) {
                // this might happen in rare cases due to S3's eventual consistency
                throw new IllegalStateException("completed etag is different from combined object etag");
            }

            return S3StorageHelper.getStoredObject(targetStorageArea, combinedObject);
        }
        catch (ServiceException e) {
            try {
                s3Service.multipartAbortUpload(upload);
            }
            catch (S3ServiceException ignored) {
            }
            throw Throwables.propagate(e);
        }
    }

    @Override
    public StoredObject putObject(StoredObject target, File source)
    {
        try {
            S3Object object = new S3Object(source);
            object.setKey(getS3ObjectName(target));
            S3Object result = s3Service.putObject(getS3Bucket(target.getStorageArea()), object);
            return S3StorageHelper.getStoredObject(target.getStorageArea(), result);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
