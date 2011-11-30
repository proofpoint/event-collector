package com.proofpoint.event.collector.combiner;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.google.common.base.Function;
import com.google.common.collect.AbstractLinkedIterator;
import com.google.common.collect.Iterators;

import java.util.Iterator;

class S3PrefixListing
        implements Iterable<String>
{
    private final AmazonS3 s3Client;
    private final ListObjectsRequest listObjectsRequest;

    public S3PrefixListing(AmazonS3 s3Client, ListObjectsRequest listObjectsRequest)
    {
        this.s3Client = s3Client;
        this.listObjectsRequest = listObjectsRequest;
    }

    @Override
    public Iterator<String> iterator()
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

        return Iterators.concat(Iterators.transform(objectListings, new Function<ObjectListing, Iterator<String>>()
        {
            @Override
            public Iterator<String> apply(ObjectListing input)
            {
                return input.getCommonPrefixes().iterator();
            }
        }));
    }
}
