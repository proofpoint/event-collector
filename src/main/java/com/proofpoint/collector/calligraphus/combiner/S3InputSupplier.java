package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.io.InputSupplier;
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3Bucket;
import static com.proofpoint.collector.calligraphus.combiner.S3StorageHelper.getS3ObjectKey;

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
