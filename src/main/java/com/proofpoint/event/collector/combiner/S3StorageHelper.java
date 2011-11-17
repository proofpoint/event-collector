/*
 * Copyright 2011 Proofpoint, Inc.
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

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import org.jets3t.service.model.S3Object;

import java.net.URI;
import java.net.URISyntaxException;

public final class S3StorageHelper
{
    private S3StorageHelper()
    {
    }

    public static String getS3Bucket(URI location)
    {
        checkValidS3Uri(location);
        return location.getAuthority();
    }

    public static String getS3ObjectKey(URI location)
    {
        checkValidS3Uri(location);
        String path = location.getPath();
        if (path.startsWith("/")) {
            path = path.substring(1);
        }
        return path;
    }

    public static String getS3FileName(URI location)
    {
        checkValidS3Uri(location);

        String path = location.getPath();
        if (path .endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }

        String name = path.substring(path.lastIndexOf('/') + 1);
        if (name.isEmpty()) {
            return null;
        }
        return name;
    }

    public static URI getS3Directory(URI location)
    {
        checkValidS3Uri(location);
        String path = location.getPath();
        if (path.contains("/")) {
            path = path.substring(0, path.lastIndexOf('/') + 1);
        }
        try {
            return new URI(location.getScheme(), location.getHost(), path, null);
        }
        catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        }
    }

    public static void checkValidS3Uri(URI location)
    {
        Preconditions.checkArgument("s3".equals(location.getScheme()),
                "location is not a S3 uri, but is a %s",
                location);

        Preconditions.checkArgument(location.isAbsolute(),
                "location is not an absolute uri, but is a %s",
                location);

        String authority = location.getAuthority();
        Preconditions.checkArgument(authority == null || !authority.isEmpty(),
                "location does not contain a bucket, but is a %s",
                location);
    }

    public static StoredObject updateStoredObject(URI location, S3Object s3Object)
    {
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkNotNull(s3Object, "s3Object is null");
        Preconditions.checkArgument(S3StorageHelper.getS3ObjectKey(location).equals(s3Object.getKey()));
        // jets3t doesn't set the bucket name in responses
        if (s3Object.getBucketName() != null) {
            Preconditions.checkArgument(S3StorageHelper.getS3Bucket(location).equals(s3Object.getBucketName()));
        }

        return new StoredObject(
                location,
                s3Object.getETag(),
                s3Object.getContentLength(),
                s3Object.getLastModifiedDate().getTime());
    }

    public static URI buildS3Location(URI base, String... parts)
    {
        return buildS3Location(base.toString(), parts);
    }

    public static URI buildS3Location(String base, String... parts)
    {
        if (!base.endsWith("/")) {
            base += "/";
        }
        URI uri = URI.create(base + Joiner.on('/').join(parts));
        checkValidS3Uri(uri);
        return uri;
    }

    public static URI appendSuffix(URI base, String suffix)
    {
        if (base.toString().endsWith(".")) {
            throw new IllegalArgumentException("base ends with dot: " + base);
        }
        if (suffix.startsWith(".")) {
            throw new IllegalArgumentException("suffix starts with dot: " + base);
        }
        return URI.create(base + "." + suffix);
    }
}
