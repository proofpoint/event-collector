package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageObjectsChunk;
import org.jets3t.service.model.MultipartCompleted;
import org.jets3t.service.model.MultipartUpload;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.model.StorageObject;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;

import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.buildS3Location;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3Bucket;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3ObjectKey;
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
    public List<URI> listDirectories(URI storageArea)
    {
        S3StorageHelper.checkValidS3Uri(storageArea);

        try {
            String s3Path = getS3ObjectKey(storageArea);
            String s3Bucket = getS3Bucket(storageArea);
            StorageObjectsChunk storageObjectsChunk = s3Service.listObjectsChunked(s3Bucket, s3Path, "/", 0, null, true);
            ImmutableList.Builder<URI> builder = ImmutableList.builder();
            for (String prefix : storageObjectsChunk.getCommonPrefixes()) {
                builder.add(buildS3Location("s3://", s3Bucket, prefix));
            }
            return builder.build();
        }
        catch (ServiceException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<StoredObject> listObjects(URI storageArea)
    {
        S3StorageHelper.checkValidS3Uri(storageArea);

        try {
            String s3Path = getS3ObjectKey(storageArea);
            StorageObjectsChunk storageObjectsChunk = s3Service.listObjectsChunked(getS3Bucket(storageArea), s3Path, "/", 0, null, true);
            ImmutableList.Builder<StoredObject> builder = ImmutableList.builder();
            for (StorageObject s3Object : storageObjectsChunk.getObjects()) {
                builder.add(new StoredObject(
                        buildS3Location(storageArea, s3Object.getName().substring(s3Path.length())),
                        s3Object.getETag(),
                        s3Object.getContentLength(),
                        s3Object.getLastModifiedDate().getTime()));
            }
            return builder.build();
        }
        catch (ServiceException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public StoredObject createCombinedObject(URI location, List<StoredObject> newCombinedObjectParts)
    {
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkNotNull(newCombinedObjectParts, "newCombinedObjectParts is null");
        Preconditions.checkArgument(!newCombinedObjectParts.isEmpty(), "newCombinedObjectParts is empty");

        boolean setIsSmall = newCombinedObjectParts.get(0).getSize() < 5 * 1024 * 1024;

        // verify size
        for (StoredObject newCombinedObjectPart : newCombinedObjectParts) {
            boolean fileIsSmall = newCombinedObjectPart.getSize() < 5 * 1024 * 1024;
            Preconditions.checkArgument(fileIsSmall == setIsSmall, "newCombinedObjectParts contains mixes large and small files");
        }

        if (setIsSmall) {
            return createCombinedObjectSmall(location, newCombinedObjectParts);
        }
        else {
            return createCombinedObjectLarge(location, newCombinedObjectParts);
        }
    }

    private StoredObject createCombinedObjectLarge(URI location, List<StoredObject> newCombinedObjectParts)
    {
        MultipartUpload upload;
        try {
            upload = s3Service.multipartStartUpload(getS3Bucket(location), new S3Object(getS3ObjectKey(location)));
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
                        getS3ObjectKey(newCombinedObjectPart.getLocation()),
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
            S3Object combinedObject = (S3Object) s3Service.getObjectDetails(getS3Bucket(location), getS3ObjectKey(location));

            // jets3t doesn't strip the quotes from the etag in the multipart api
            if (!completed.getEtag().equals('\"' + combinedObject.getETag() + '\"')) {
                // this might happen in rare cases due to S3's eventual consistency
                throw new IllegalStateException("completed etag is different from combined object etag");
            }

            return updateStoredObject(location, combinedObject);
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

    private StoredObject createCombinedObjectSmall(URI location, List<StoredObject> newCombinedObjectParts)
    {
        ImmutableList.Builder<InputSupplier<InputStream>> builder = ImmutableList.builder();
        List<URI> sourceParts = Lists.transform(newCombinedObjectParts, StoredObject.GET_LOCATION_FUNCTION);
        for (URI sourcePart : sourceParts) {
            builder.add(getInputSupplier(sourcePart));
        }
        InputSupplier<InputStream> source = ByteStreams.join(builder.build());

        File tempFile = null;
        try {
            tempFile = File.createTempFile(S3StorageHelper.getS3FileName(location), ".small.s3.data");
            Files.copy(source, tempFile);
            StoredObject result = putObject(location, tempFile);
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
    public StoredObject putObject(URI location, File source)
    {
        try {
            S3Object object = new S3Object(source);
            object.setKey(S3StorageHelper.getS3ObjectKey(location));
            S3Object result = s3Service.putObject(getS3Bucket(location), object);
            return updateStoredObject(location, result);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public StoredObject getObjectDetails(URI target)
    {
        try {
            final StoredObject storedObject = new StoredObject(target);
            return updateStoredObject(storedObject.getLocation(), (S3Object) s3Service.getObjectDetails(getS3Bucket(target), getS3ObjectKey(target)));
        }
        catch (ServiceException e) {
            throw Throwables.propagate(e);
        }
    }

    public InputSupplier<InputStream> getInputSupplier(URI target) {
        return new S3InputSupplier(s3Service, target);
    }


    private static class S3InputSupplier implements InputSupplier<InputStream>
    {
        private final ExtendedRestS3Service service;
        private final URI target;

        private S3InputSupplier(ExtendedRestS3Service service, URI target)
        {
            this.service = service;
            this.target = target;
        }

        @Override
        public InputStream getInput()
                throws IOException
        {
            try {
                S3Object object = service.getObject(getS3Bucket(target), getS3ObjectKey(target));
                return object.getDataInputStream();
            }
            catch (ServiceException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
