package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.CountingOutputStream;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.proofpoint.json.JsonCodec;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.security.AWSCredentials;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newTreeMap;
import static com.proofpoint.collector.calligraphus.combiner.StoredObject.GET_LOCATION_FUNCTION;

public class TestS3Combine
{
    public static final String BUCKET_NAME = "proofpoint-test-jets3t";
    public static final String EVENT_TYPE = "test-event";
    public static final String TIME_SLICE = "2011-08-01";
    private static final int MIN_LARGE_FILE_LENGTH = 5 * 1024 * 1024;
    private static final int MIN_SMALL_FILE_LENGTH = 10 * 1024;

    private StoredObjectCombiner objectCombiner;
    private URI stagingBaseUri;
    private URI targetBaseUri;
    private S3StorageSystem storageSystem;
    private TestingCombineObjectMetadataStore metadataStore;

    @BeforeClass(enabled = false)
    public void setUp()
            throws Exception
    {
        String credentialsJson = Files.toString(new File("/Users/dain/bin/emr/credentials.json"), Charsets.UTF_8);
        Map<String, String> map = JsonCodec.mapJsonCodec(String.class, String.class).fromJson(credentialsJson);
        String awsAccessKey = map.get("access-id");
        String awsSecretKey = map.get("private-key");

        AWSCredentials awsCredentials = new AWSCredentials(awsAccessKey, awsSecretKey);
        ExtendedRestS3Service service = new ExtendedRestS3Service(awsCredentials);

        service.getOrCreateBucket(BUCKET_NAME);

        String randomPart = "CombineTest-" + UUID.randomUUID().toString().replace("-", "");
        stagingBaseUri = S3StorageHelper.buildS3Location("s3://", BUCKET_NAME, randomPart, "staging/");
        targetBaseUri = S3StorageHelper.buildS3Location("s3://", BUCKET_NAME, randomPart, "target/");

        storageSystem = new S3StorageSystem(service);
        metadataStore = new TestingCombineObjectMetadataStore("test");
        objectCombiner = new StoredObjectCombiner("test",
                metadataStore,
                storageSystem,
                stagingBaseUri,
                targetBaseUri);
    }

    @Test(enabled = false)
    public void testLargeCombine()
            throws Exception
    {
        URI target = S3StorageHelper.buildS3Location(targetBaseUri, EVENT_TYPE, TIME_SLICE, "large.json.snappy");

        //  create 2 file names for staging
        String base = UUID.randomUUID().toString().replace("-", "");
        List<URI> objectNames = Lists.newArrayList();
        for (int i = 0; i < 2; i++) {
            objectNames.add(createStagingFileName(base, i, "large"));
        }

        // upload 5 MB file for to each name
        Map<URI, InputSupplier<? extends InputStream>> files = newHashMap();
        for (URI name : objectNames) {
            File file = uploadFile(name, MIN_LARGE_FILE_LENGTH);
            files.put(name, Files.newInputStreamSupplier(file));
        }

        // combine the files
        objectCombiner.combineObjects();

        // verify the contents
        InputSupplier<? extends InputStream> s3InputSupplier = storageSystem.getInputSupplier(target);

        InputSupplier<InputStream> combinedInputs = getCombinedInputsSupplier(files, target);
        if (!ByteStreams.equal(combinedInputs, s3InputSupplier)) {
            Assert.fail("broken");
        }

        // upload more chunks
        for (int i = 0; i < 2; i++) {
            objectNames.add(createStagingFileName(base, i, "large"));
        }
        for (URI name : objectNames) {
            File file = uploadFile(name, MIN_LARGE_FILE_LENGTH);
            files.put(name, Files.newInputStreamSupplier(file));
        }

        // combine the files
        objectCombiner.combineObjects();

        // verify the contents
        s3InputSupplier = storageSystem.getInputSupplier(target);

        combinedInputs = getCombinedInputsSupplier(files, target);
        if (!ByteStreams.equal(combinedInputs, s3InputSupplier)) {
            Assert.fail("broken");
        }
    }

    @Test(enabled = false)
    public void testSmallCombine()
            throws Exception
    {
        URI target = S3StorageHelper.buildS3Location(targetBaseUri, EVENT_TYPE, TIME_SLICE, "small.json.snappy");

        //  create 2 file names for staging
        String base = UUID.randomUUID().toString().replace("-", "");
        List<URI> objectNames = Lists.newArrayList();
        for (int i = 0; i < 2; i++) {
            objectNames.add(createStagingFileName(base, i, "small"));
        }

        // upload 10 KB file for to each name
        Map<URI, InputSupplier<? extends InputStream>> files = newHashMap();
        for (URI name : objectNames) {
            File file = uploadFile(name, 10 * 1024);
            files.put(name, Files.newInputStreamSupplier(file));
        }

        // combine the files
        objectCombiner.combineObjects();

        // verify the contents
        StoredObject combinedObject = storageSystem.getObjectDetails(target);

        InputSupplier<InputStream> combinedInputs = getCombinedInputsSupplier(files, target);
        String sourceMD5 = encodeHex(ByteStreams.getDigest(combinedInputs, MessageDigest.getInstance("MD5")));
        if (!sourceMD5.equals(combinedObject.getETag())) {
            Assert.fail("broken");
        }

        // upload more chunks
        for (int i = 0; i < 2; i++) {
            objectNames.add(createStagingFileName(base, i, "small"));
        }
        for (URI name : objectNames) {
            File file = uploadFile(name, MIN_SMALL_FILE_LENGTH);
            files.put(name, Files.newInputStreamSupplier(file));
        }

        // combine the files
        objectCombiner.combineObjects();

        // verify the contents
        combinedObject = storageSystem.getObjectDetails(target);

        combinedInputs = getCombinedInputsSupplier(files, target);
        sourceMD5 = encodeHex(ByteStreams.getDigest(combinedInputs, MessageDigest.getInstance("MD5")));
        if (!sourceMD5.equals(combinedObject.getETag())) {
            Assert.fail("broken");
        }
    }

    private InputSupplier<InputStream> getCombinedInputsSupplier(Map<URI, InputSupplier<? extends InputStream>> files, URI target)
    {
        // get the manifest for the target
        CombinedStoredObject combinedObjectManifest = metadataStore.getCombinedObjectManifest(target);

        // get the locations of each part (in order)
        List<URI> sourcePartsLocation = Lists.transform(combinedObjectManifest.getSourceParts(), GET_LOCATION_FUNCTION);

        // sort the supplied files map based on this explicit order
        Map<URI, InputSupplier<? extends InputStream>> parts = newTreeMap(Ordering.explicit(sourcePartsLocation));
        parts.putAll(files);

        // join the parts
        return ByteStreams.join(parts.values());
    }

    private static final char[] DIGITS = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    public static String encodeHex(byte[] data)
    {
        char[] out = new char[data.length * 2];
        for (int i = 0, j = 0; i < data.length; i++) {
            out[j++] = DIGITS[(0xF0 & data[i]) >>> 4];
            out[j++] = DIGITS[0x0F & data[i]];
        }
        return new String(out);
    }

    private URI createStagingFileName(String base, int i, String size)
    {
        return S3StorageHelper.buildS3Location(stagingBaseUri, EVENT_TYPE, TIME_SLICE, size, String.format("part-%s-%04d", base, i));
    }

    private File uploadFile(URI location, int minFileLength)
            throws NoSuchAlgorithmException, IOException, S3ServiceException
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
            storageSystem.putObject(new StoredObject(location), tempFile);
        }
        catch (Throwable t) {
            Closeables.closeQuietly(countingOutputStream);
            tempFile.delete();
            throw Throwables.propagate(t);
        }
        return tempFile;
    }
}
