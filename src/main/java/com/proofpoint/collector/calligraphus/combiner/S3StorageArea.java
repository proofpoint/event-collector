package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Preconditions;

public class S3StorageArea implements StorageArea
{
    private final String bucket;
    private final String path;

    public S3StorageArea(String bucket, String path)
    {
        Preconditions.checkNotNull(bucket, "bucket is null");
        Preconditions.checkArgument(!bucket.isEmpty(), "bucket name is empty");
        Preconditions.checkArgument(!bucket.contains("/"), "bucket name contains a '/'");
        Preconditions.checkNotNull(path, "path is null");
        Preconditions.checkArgument(!path.isEmpty(), "path is empty");
        if (!path.endsWith("/")) {
            path += '/';
        }
        this.bucket = bucket;
        this.path = path;
    }

    @Override
    public String getName()
    {
        return bucket + "/" + path;
    }

    public String getBucket()
    {
        return bucket;
    }

    public String getPath()
    {
        return path;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        S3StorageArea that = (S3StorageArea) o;

        if (!bucket.equals(that.bucket)) {
            return false;
        }
        if (!path.equals(that.path)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = bucket.hashCode();
        result = 31 * result + path.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return getName();
    }
}
