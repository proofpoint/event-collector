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
import org.jets3t.service.ServiceException;
import org.jets3t.service.model.S3Object;
import org.jets3t.service.security.AWSCredentials;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
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
    private static final int MIN_FILE_LENGTH = 5 * 1024 * 1024;

    private ExtendedRestS3Service service;
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
        service = new ExtendedRestS3Service(awsCredentials);

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
    public void test()
            throws Exception
    {
        URI target = S3StorageHelper.buildS3Location(targetBaseUri, EVENT_TYPE, TIME_SLICE, "large.json.snappy");

        //  create 2 file names for staging
        String base = UUID.randomUUID().toString().replace("-", "");
        List<URI> objectNames = Lists.newArrayList();
        for (int i = 0; i < 2; i++) {
            objectNames.add(createStagingFileName(base, i));
        }

        // upload 5 MB file for to each name
        Map<URI, InputSupplier<? extends InputStream>> files = newHashMap();
        for (URI name : objectNames) {
            File file = uploadFile(name);
            files.put(name, Files.newInputStreamSupplier(file));
        }

        // combine the files
        objectCombiner.combineObjects();

        // verify the contents
        S3InputSupplier s3InputSupplier = new S3InputSupplier(service, target);

        InputSupplier<InputStream> combinedInputs = getCombinedInputsSupplier(files, target);
        if (!ByteStreams.equal(combinedInputs, s3InputSupplier)) {
            Assert.fail("broken");
        }

        // upload more chunks
        for (int i = 0; i < 2; i++) {
            objectNames.add(createStagingFileName(base, i));
        }
        for (URI name : objectNames) {
            File file = uploadFile(name);
            files.put(name, Files.newInputStreamSupplier(file));
        }

        // combine the files
        objectCombiner.combineObjects();

        // verify the contents
        s3InputSupplier = new S3InputSupplier(service, target);

        combinedInputs = getCombinedInputsSupplier(files, target);
        if (!ByteStreams.equal(combinedInputs, s3InputSupplier)) {
            Assert.fail("broken");
        }
    }

    private InputSupplier<InputStream> getCombinedInputsSupplier(Map<URI, InputSupplier<? extends InputStream>> files, URI target)
    {
        CombinedStoredObject combinedObjectManifest = metadataStore.getCombinedObjectManifest(target);
        List<StoredObject> sourceParts = combinedObjectManifest.getSourceParts();
        List<URI> sourcePartsLocation = Lists.transform(sourceParts, GET_LOCATION_FUNCTION);
        Map<URI, InputSupplier<? extends InputStream>> parts = newTreeMap(Ordering.explicit(sourcePartsLocation));
        parts.putAll(files);

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

    private URI createStagingFileName(String base, int i)
    {
        return S3StorageHelper.buildS3Location(stagingBaseUri, EVENT_TYPE, TIME_SLICE, "large", String.format("part-%s-%04d", base, i));
    }

    private File uploadFile(URI location)
            throws NoSuchAlgorithmException, IOException, S3ServiceException
    {

        File tempFile = File.createTempFile(S3StorageHelper.getS3FileName(location), ".s3.data");
        CountingOutputStream countingOutputStream = null;
        try {
            // write contents to a temp file
            countingOutputStream = new CountingOutputStream(new FileOutputStream(tempFile));
            while (countingOutputStream.getCount() < MIN_FILE_LENGTH) {
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

    private static class S3InputSupplier implements InputSupplier<InputStream>
    {
        private final ExtendedRestS3Service service;
        private final URI target;

        private S3InputSupplier(ExtendedRestS3Service service, URI target)
        {
            this.service = service;
            this.target = target;
        }

        @Override
        public InputStream getInput()
                throws IOException
        {
            try {
                S3Object object = service.getObject(S3StorageHelper.getS3Bucket(target), S3StorageHelper.getS3ObjectKey(target));
                return object.getDataInputStream();
            }
            catch (ServiceException e) {
                throw Throwables.propagate(e);
            }
        }
    }
}
