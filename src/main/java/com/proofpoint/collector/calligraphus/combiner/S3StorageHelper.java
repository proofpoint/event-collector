package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import org.jets3t.service.model.S3Object;

import java.net.URI;

public final class S3StorageHelper
{
    private S3StorageHelper()
    {
    }

    public static String getS3ObjectName(StoredObject storedObject) {
        return getS3Path(storedObject.getStorageArea()) + storedObject.getName();
    }

    public static String getS3Bucket(URI s3StorageArea)
    {
        checkValidS3Uri(s3StorageArea);
        return s3StorageArea.getAuthority();
    }

    public static String getS3Path(URI s3StorageArea)
    {
        checkValidS3Uri(s3StorageArea);
        String path = s3StorageArea.getPath();
        if (!path.endsWith("/")) {
            path += "/";
        }
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return path;
    }

    public static String getS3Name(URI s3StorageArea)
    {
        checkValidS3Uri(s3StorageArea);
        String name = s3StorageArea.getPath().substring(s3StorageArea.getPath().lastIndexOf('/') + 1);
        if (name.isEmpty()) {
            return null;
        }
        return name;
    }

    public static void checkValidS3Uri(URI s3StorageArea)
    {
        Preconditions.checkArgument("s3".equals(s3StorageArea.getScheme()),
                "s3StorageArea is not a S3 uri, but is a %s",
                s3StorageArea);

        Preconditions.checkArgument(s3StorageArea.isAbsolute(),
                "s3StorageArea is not an absolute uri, but is a %s",
                s3StorageArea);

        String authority = s3StorageArea.getAuthority();
        Preconditions.checkArgument(authority == null || !authority.isEmpty(),
                "s3StorageArea does not contain a bucket, but is a %s",
                s3StorageArea);

    }

    public static StoredObject toStoredObject(URI s3StorageArea, S3Object s3Object)
    {
        checkValidS3Uri(s3StorageArea);
        return new StoredObject(
                s3Object.getKey(),
                s3StorageArea,
                s3Object.getETag(),
                s3Object.getContentLength(),
                s3Object.getLastModifiedDate().getTime());
    }

    public static URI buildS3StorageArea(String base, String... parts)
    {
        if (!base.endsWith("/")) {
            base += "/";
        }
        URI uri = URI.create(base + Joiner.on('/').join(parts));
        checkValidS3Uri(uri);
        return uri;
    }
}