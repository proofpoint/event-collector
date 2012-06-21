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
package com.proofpoint.event.combiner;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.proofpoint.event.client.InMemoryEventClient;
import com.proofpoint.experimental.units.DataSize;
import com.proofpoint.json.JsonCodec;
import org.joda.time.DateTime;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newTreeMap;
import static com.proofpoint.event.combiner.StoredObject.GET_LOCATION_FUNCTION;
import static org.apache.commons.codec.binary.Hex.encodeHexString;

@Test(groups = "aws")
public class TestS3Combine
{
    private static final String EVENT_TYPE = "TestEvent";
    private static final String TIME_SLICE = ISODateTimeFormat.yearMonthDay().print(new DateTime());
    private static final int MIN_LARGE_FILE_LENGTH = 5 * 1024 * 1024;
    private static final int MIN_SMALL_FILE_LENGTH = 10 * 1024;
    private static final String HOUR = "08";

    private String testBucket;
    private AmazonS3 service;
    private StoredObjectCombiner objectCombiner;
    private URI stagingBaseUri;
    private URI targetBaseUri;
    private S3StorageSystem storageSystem;
    private TestingCombineObjectMetadataStore metadataStore;
    private InMemoryEventClient eventClient;

    @BeforeClass
    @Parameters({"aws-credentials-file", "aws-test-bucket"})
    public void setUpClass(String awsCredentialsFile, String awsTestBucket)
            throws Exception
    {
        String credentialsJson = Files.toString(new File(awsCredentialsFile), Charsets.UTF_8);
        Map<String, String> map = JsonCodec.mapJsonCodec(String.class, String.class).fromJson(credentialsJson);
        String awsAccessKey = map.get("access-id");
        String awsSecretKey = map.get("private-key");

        AWSCredentials awsCredentials = new BasicAWSCredentials(awsAccessKey, awsSecretKey);
        service = new AmazonS3Client(awsCredentials);

        testBucket = awsTestBucket;
        if (!service.doesBucketExist(testBucket)) {
            service.createBucket(testBucket);
        }
    }

    @BeforeMethod
    public void setUpMethod()
    {
        String randomPart = "CombineTest-" + UUID.randomUUID().toString().replace("-", "");
        stagingBaseUri = S3StorageHelper.buildS3Location("s3://", testBucket, randomPart, "staging/");
        targetBaseUri = S3StorageHelper.buildS3Location("s3://", testBucket, randomPart, "target/");

        eventClient = new InMemoryEventClient();
        storageSystem = new S3StorageSystem(service);
        metadataStore = new TestingCombineObjectMetadataStore();
        objectCombiner = new StoredObjectCombiner("test",
                metadataStore,
                storageSystem,
                eventClient,
                stagingBaseUri,
                targetBaseUri,
                new DataSize(512, DataSize.Unit.MEGABYTE),
                14);
    }

    @Test
    public void testLargeCombine()
            throws Exception
    {
        EventPartition eventPartition = new EventPartition(EVENT_TYPE, TIME_SLICE, HOUR);
        String sizeName = "large";
        URI groupPrefix = S3StorageHelper.buildS3Location(targetBaseUri, EVENT_TYPE, TIME_SLICE, HOUR + ".large");
        URI target = S3StorageHelper.appendSuffix(groupPrefix, "00000.json.snappy");

        // upload two 5 MB files
        String base = UUID.randomUUID().toString().replace("-", "");
        Map<URI, InputSupplier<? extends InputStream>> files = newHashMap();
        for (int i = 0; i < 2; i++) {
            URI name = createStagingFileName(base, i + 10);
            File file = uploadFile(name, MIN_LARGE_FILE_LENGTH);
            files.put(name, Files.newInputStreamSupplier(file));
        }

        // combine the files
        objectCombiner.combineAllObjects();

        // verify the contents
        InputSupplier<? extends InputStream> s3InputSupplier = storageSystem.getInputSupplier(target);

        InputSupplier<InputStream> combinedInputs = getCombinedInputsSupplier(eventPartition, sizeName, files, groupPrefix, target);
        if (!ByteStreams.equal(combinedInputs, s3InputSupplier)) {
            Assert.fail("broken");
        }

        // upload two more chunks
        for (int i = 0; i < 2; i++) {
            URI name = createStagingFileName(base, i);
            File file = uploadFile(name, MIN_LARGE_FILE_LENGTH);
            files.put(name, Files.newInputStreamSupplier(file));
        }

        // combine the files
        objectCombiner.combineAllObjects();

        // verify the contents
        s3InputSupplier = storageSystem.getInputSupplier(target);

        combinedInputs = getCombinedInputsSupplier(eventPartition, sizeName, files, groupPrefix, target);
        if (!ByteStreams.equal(combinedInputs, s3InputSupplier)) {
            Assert.fail("broken");
        }

        // verify version combiner doesn't recombine unchanged files
        CombinedGroup combinedObjectManifest = metadataStore.getCombinedGroupManifest(eventPartition, sizeName);
        long currentVersion = combinedObjectManifest.getVersion();
        objectCombiner.combineAllObjects();
        CombinedGroup newCombinedStoredObjectManifest = metadataStore.getCombinedGroupManifest(eventPartition, sizeName);
        Assert.assertEquals(newCombinedStoredObjectManifest.getVersion(), currentVersion);

        // verify that events were fired
        Assert.assertEquals(eventClient.getEvents().size(), 3);
    }

    @Test
    public void testSmallCombine()
            throws Exception
    {
        EventPartition eventPartition = new EventPartition(EVENT_TYPE, TIME_SLICE, HOUR);
        String sizeName = "small";
        URI groupPrefix = S3StorageHelper.buildS3Location(targetBaseUri, EVENT_TYPE, TIME_SLICE, HOUR + ".small");
        URI target = S3StorageHelper.appendSuffix(groupPrefix, "00000.json.snappy");

        // upload two 10 KB file for to each name
        String base = UUID.randomUUID().toString().replace("-", "");
        Map<URI, InputSupplier<? extends InputStream>> files = newHashMap();
        for (int i = 0; i < 2; i++) {
            URI name = createStagingFileName(base, i + 10);
            File file = uploadFile(name, MIN_SMALL_FILE_LENGTH);
            files.put(name, Files.newInputStreamSupplier(file));
        }

        // combine the files
        objectCombiner.combineAllObjects();

        // verify the contents
        StoredObject combinedObject = storageSystem.getObjectDetails(target);

        InputSupplier<InputStream> combinedInputs = getCombinedInputsSupplier(eventPartition, sizeName, files, groupPrefix, target);
        String sourceMD5 = encodeHexString(ByteStreams.getDigest(combinedInputs, MessageDigest.getInstance("MD5")));
        if (!sourceMD5.equals(combinedObject.getETag())) {
            Assert.fail("broken");
        }

        // upload two more chunks
        for (int i = 0; i < 2; i++) {
            URI name = createStagingFileName(base, i);
            File file = uploadFile(name, MIN_SMALL_FILE_LENGTH);
            files.put(name, Files.newInputStreamSupplier(file));
        }

        // combine the files
        objectCombiner.combineAllObjects();

        // verify the contents
        combinedObject = storageSystem.getObjectDetails(target);

        combinedInputs = getCombinedInputsSupplier(eventPartition, sizeName, files, groupPrefix, target);
        sourceMD5 = encodeHexString(ByteStreams.getDigest(combinedInputs, MessageDigest.getInstance("MD5")));
        if (!sourceMD5.equals(combinedObject.getETag())) {
            Assert.fail("broken");
        }

        // verify version combiner doesn't recombine unchanged files
        CombinedGroup combinedObjectManifest = metadataStore.getCombinedGroupManifest(eventPartition, sizeName);
        long currentVersion = combinedObjectManifest.getVersion();
        objectCombiner.combineAllObjects();
        CombinedGroup newCombinedStoredObjectManifest = metadataStore.getCombinedGroupManifest(eventPartition, sizeName);
        Assert.assertEquals(newCombinedStoredObjectManifest.getVersion(), currentVersion);

        // verify that events were fired
        Assert.assertEquals(eventClient.getEvents().size(), 3);
    }

    private InputSupplier<InputStream> getCombinedInputsSupplier(EventPartition eventPartition, String sizeName, Map<URI, InputSupplier<? extends InputStream>> files, URI groupPrefix, URI target)
    {
        // get the manifest for the group prefix
        CombinedGroup combinedObjectManifest = metadataStore.getCombinedGroupManifest(eventPartition, sizeName);

        // get the combined stored object for the target
        CombinedStoredObject combinedObject = combinedObjectManifest.getCombinedObject(target);
        Assert.assertNotNull(combinedObject);

        // get the locations of each part (in order)
        List<URI> sourcePartsLocation = Lists.transform(combinedObject.getSourceParts(), GET_LOCATION_FUNCTION);

        // sort the supplied files map based on this explicit order
        Map<URI, InputSupplier<? extends InputStream>> parts = newTreeMap(Ordering.explicit(sourcePartsLocation));
        parts.putAll(files);

        // join the parts
        return ByteStreams.join(parts.values());
    }

    private URI createStagingFileName(String base, int i)
    {
        return S3StorageHelper.buildS3Location(stagingBaseUri, EVENT_TYPE, TIME_SLICE, HOUR, String.format("part-%s-%04d", base, i));
    }

    private File uploadFile(URI location, int minFileLength)
            throws IOException
    {
        File tempFile = File.createTempFile(S3StorageHelper.getS3FileName(location), ".s3.data");
        CountingOutputStream countingOutputStream = null;
        try {
            // write contents to a temp file
            countingOutputStream = new CountingOutputStream(new FileOutputStream(tempFile));
            while (countingOutputStream.getCount() < minFileLength) {
                String line = "This is object " + location + " at offset " + countingOutputStream.getCount() + "\n";
                countingOutputStream.write(line.getBytes(Charsets.UTF_8));
            }
            countingOutputStream.flush();
            countingOutputStream.close();

            // upload
            final StoredObject target = new StoredObject(location);
            storageSystem.putObject(target.getLocation(), tempFile);
        }
        catch (Throwable t) {
            Closeables.closeQuietly(countingOutputStream);
            tempFile.delete();
            throw Throwables.propagate(t);
        }
        return tempFile;
    }
}
