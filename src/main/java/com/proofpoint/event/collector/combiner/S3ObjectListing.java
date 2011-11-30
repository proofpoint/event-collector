package com.proofpoint.event.collector.combiner;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Function;
import com.google.common.collect.AbstractLinkedIterator;
import com.google.common.collect.Iterators;

import java.util.Iterator;

class S3ObjectListing
        implements Iterable<S3ObjectSummary>
{
    private final AmazonS3 s3Client;
    private final ListObjectsRequest listObjectsRequest;

    public S3ObjectListing(AmazonS3 s3Client, ListObjectsRequest listObjectsRequest)
    {
        this.s3Client = s3Client;
        this.listObjectsRequest = listObjectsRequest;
    }

    @Override
    public Iterator<S3ObjectSummary> iterator()
    {
        Iterator<ObjectListing> objectListings = new AbstractLinkedIterator<ObjectListing>(s3Client.listObjects(listObjectsRequest))
        {
            @Override
            protected ObjectListing computeNext(ObjectListing previous)
            {
                if (!previous.isTruncated()) {
                    return null;
                }
                return s3Client.listNextBatchOfObjects(previous);
            }
        };

        return Iterators.concat(Iterators.transform(objectListings, new Function<ObjectListing, Iterator<S3ObjectSummary>>()
        {
            @Override
            public Iterator<S3ObjectSummary> apply(ObjectListing input)
            {
                return input.getObjectSummaries().iterator();
            }
        }));
    }
}
