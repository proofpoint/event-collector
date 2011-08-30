package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.MultipartCompleted;
import org.jets3t.service.model.MultipartUpload;
import org.jets3t.service.model.S3Object;

import java.util.List;

public class S3StorageSystem implements StorageSystem
{
    private final ExtendedRestS3Service s3Service;

    public S3StorageSystem(ExtendedRestS3Service s3Service)
    {
        Preconditions.checkNotNull(s3Service, "s3Service is null");
        this.s3Service = s3Service;
    }

    @Override
    public List<StoredObject> listObjects(StorageArea storageArea)
    {
        Preconditions.checkArgument(storageArea instanceof S3StorageArea,
                "storageArea is not an instance of %s, but is a %s",
                S3StorageArea.class.getName(),
                storageArea.getClass().getName());

        try {

            S3StorageArea s3StorageArea = (S3StorageArea) storageArea;
            S3Object[] s3Objects = s3Service.listObjects(s3StorageArea.getBucket(), s3StorageArea.getPath(), "/", 1000L);
            ImmutableList.Builder<StoredObject> builder = ImmutableList.builder();
            for (S3Object s3Object : s3Objects) {
                builder.add(new StoredObject(s3Object.getName().substring(s3StorageArea.getPath().length()),
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
        S3StorageArea targetStorageArea = (S3StorageArea) target.getStorageArea();
        MultipartUpload upload;
        try {
            upload = s3Service.multipartStartUpload(targetStorageArea.getBucket(), new S3Object(targetStorageArea.getPath() + target));
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
                        targetStorageArea.getBucket(),
                        targetStorageArea.getPath() + newCombinedObjectPart.getName(),
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
            return new StoredObject(completed.getObjectKey(), targetStorageArea, completed.getEtag(), target.getSize(), target.getLastModified());
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

}
