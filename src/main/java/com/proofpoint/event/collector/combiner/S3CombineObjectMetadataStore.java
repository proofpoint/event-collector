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

import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.proofpoint.event.collector.EventPartition;
import com.proofpoint.event.collector.ServerConfig;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.log.Logger;
import org.apache.commons.codec.digest.DigestUtils;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Object;

import javax.inject.Inject;
import javax.ws.rs.core.MediaType;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.proofpoint.event.collector.combiner.S3StorageHelper.getS3Bucket;

public class S3CombineObjectMetadataStore
        implements CombineObjectMetadataStore
{
    private static final Logger log = Logger.get(S3CombineObjectMetadataStore.class);
    private final JsonCodec<CombinedGroup> jsonCodec = JsonCodec.jsonCodec(CombinedGroup.class);
    private final ExtendedRestS3Service s3Service;
    private final URI storageArea;

    @Inject
    public S3CombineObjectMetadataStore(ServerConfig config, ExtendedRestS3Service s3Service)
    {
        this(URI.create(checkNotNull(config, "config is null").getS3MetadataLocation()), s3Service);
    }

    public S3CombineObjectMetadataStore(URI storageArea, ExtendedRestS3Service s3Service)
    {
        this.storageArea = checkNotNull(storageArea, "storageArea is null");
        this.s3Service = checkNotNull(s3Service, "s3Service is null");
    }

    @Override
    public CombinedGroup getCombinedGroupManifest(EventPartition eventPartition, String sizeName)
    {
        return  readMetadataFile(eventPartition, sizeName);
    }

    @Override
    public boolean replaceCombinedGroupManifest(EventPartition eventPartition, String sizeName, CombinedGroup currentGroup, CombinedGroup newGroup)
    {
        checkNotNull(currentGroup, "currentGroup is null");
        checkNotNull(newGroup, "newGroup is null");
        checkArgument(currentGroup.getLocationPrefix().equals(newGroup.getLocationPrefix()), "newGroup location is different from currentGroup location");

        CombinedGroup persistentGroup = readMetadataFile(eventPartition, sizeName);
        if (persistentGroup != null) {
            if (persistentGroup.getVersion() != currentGroup.getVersion()) {
                return false;
            }
        }
        else if (currentGroup.getVersion() != 0) {
            return false;
        }

        return writeMetadataFile(eventPartition, newGroup, sizeName);
    }

    private boolean writeMetadataFile(EventPartition eventPartition, CombinedGroup combinedGroup, String sizeName)
    {
        byte[] json = jsonCodec.toJson(combinedGroup).getBytes(Charsets.UTF_8);
        URI metadataFile = toMetadataLocation(eventPartition, sizeName);
        try {
            S3Object object = new S3Object();
            object.setKey(S3StorageHelper.getS3ObjectKey(metadataFile));
            object.setDataInputStream(new ByteArrayInputStream(json));
            object.setContentLength(json.length);
            object.setMd5Hash(DigestUtils.md5(json));
            object.setContentType(MediaType.APPLICATION_JSON);

            s3Service.putObject(getS3Bucket(metadataFile), object);

            return true;
        }
        catch (S3ServiceException e) {
            log.warn(e, "error writing metadata file: %s", metadataFile);
            return false;
        }
    }

    private URI toMetadataLocation(EventPartition eventPartition, String sizeName)
    {
        return storageArea.resolve(eventPartition.getEventType() + "/" +
                eventPartition.getMajorTimeBucket() + "/" +
                eventPartition.getMinorTimeBucket() +"."+ sizeName + ".metadata");
    }

    private CombinedGroup readMetadataFile(EventPartition eventPartition, String sizeName)
    {
        URI metadataFile = toMetadataLocation(eventPartition, sizeName);
        String json;
        try {
            json = CharStreams.toString(CharStreams.newReaderSupplier(new S3InputSupplier(s3Service, metadataFile), Charsets.UTF_8));
        }
        catch (IOException e) {
            if (e.getCause() instanceof S3ServiceException) {
                S3ServiceException serviceException = (S3ServiceException) e.getCause();
                if ("NoSuchKey".equals(serviceException.getS3ErrorCode())) {
                    return null;
                }
            }
            throw new RuntimeException("Could not load metadata at " + metadataFile + " file for " + eventPartition + " " + sizeName);
        }
        try {
            return jsonCodec.fromJson(json);
        }
        catch (IllegalArgumentException e) {
            throw new RuntimeException("Metadata at " + metadataFile + " file for " + eventPartition + " " + sizeName + " is corrupt");
        }
    }
}
