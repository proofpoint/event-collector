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

import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.buildS3Location;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3Bucket;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3Path;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.updateStoredObject;

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
                builder.add(new StoredObject(
                        buildS3Location(storageArea, s3Object.getName().substring(s3Path.length())),
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
        MultipartUpload upload;
        try {
            upload = s3Service.multipartStartUpload(getS3Bucket(target.getLocation()), new S3Object(getS3Path(target.getLocation())));
        }
        catch (S3ServiceException e) {
            throw Throwables.propagate(e);
        }

        try {
            int partNumber = 1;
            for (StoredObject newCombinedObjectPart : newCombinedObjectParts) {

                s3Service.multipartUploadPartCopy(
                        upload,
                        partNumber,
                        getS3Bucket(newCombinedObjectPart.getLocation()),
                        getS3Path(newCombinedObjectPart.getLocation()),
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
            S3Object combinedObject = s3Service.getObject(getS3Bucket(target.getLocation()), getS3Path(target.getLocation()));

            if (!completed.getEtag().equals(combinedObject.getETag())) {
                // this might happen in rare cases due to S3's eventual consistency
                throw new IllegalStateException("completed etag is different from combined object etag");
            }

            return updateStoredObject(target, combinedObject);
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
            object.setKey(target.getLocation().toString());
            S3Object result = s3Service.putObject(getS3Bucket(target.getLocation()), object);
            return updateStoredObject(target, result);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}
