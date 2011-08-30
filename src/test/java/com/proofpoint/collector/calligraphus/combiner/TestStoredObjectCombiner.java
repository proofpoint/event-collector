package com.proofpoint.collector.calligraphus.combiner;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.UUID;

public class TestStoredObjectCombiner
{
    @Test
    public void test()
            throws Exception
    {

        TestStorageArea stagingArea = new TestStorageArea("staging");
        TestStorageArea targetArea = new TestStorageArea("target");

        TestingStorageSystem storageSystem = new TestingStorageSystem();
        StoredObject a = new StoredObject("a", stagingArea, UUID.randomUUID().toString(), 1000, 0);
        storageSystem.addObject(stagingArea, a);
        StoredObject b = new StoredObject("b", stagingArea, UUID.randomUUID().toString(), 1000, 0);
        storageSystem.addObject(stagingArea, b);

        TestingCombineObjectMetadataStore metadataStore = new TestingCombineObjectMetadataStore("test");
        StoredObjectCombiner objectCombiner = new StoredObjectCombiner("nodeId", metadataStore, storageSystem);
        objectCombiner.combineObjects(stagingArea, targetArea);

        CombinedStoredObject combinedObjectManifest = metadataStore.getCombinedObjectManifest(stagingArea, targetArea);
        Assert.assertEquals(combinedObjectManifest.getSourceParts(), ImmutableList.<Object>of(a, b));

    }

    private static class TestStorageArea implements StorageArea
    {
        private final String name;

        private TestStorageArea(String name)
        {
            Preconditions.checkNotNull(name, "name is null");
            this.name = name;
        }

        @Override
        public String getName()
        {
            return name;
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

            TestStorageArea that = (TestStorageArea) o;

            if (!name.equals(that.name)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            return name.hashCode();
        }

        @Override
        public String toString()
        {
            final StringBuilder sb = new StringBuilder();
            sb.append("TestStorageArea");
            sb.append("{name='").append(name).append('\'');
            sb.append('}');
            return sb.toString();
        }
    }
}
