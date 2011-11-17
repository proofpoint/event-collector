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

import com.google.common.io.InputSupplier;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static com.proofpoint.event.collector.combiner.S3StorageHelper.getS3Bucket;
import static com.proofpoint.event.collector.combiner.S3StorageHelper.getS3ObjectKey;

class S3InputSupplier implements InputSupplier<InputStream>
{
    private final ExtendedRestS3Service service;
    private final URI target;

    S3InputSupplier(ExtendedRestS3Service service, URI target)
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
            throw new IOException(e);
        }
    }
}
