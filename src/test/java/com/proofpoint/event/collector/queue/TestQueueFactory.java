/*
 * Copyright 2011-2014 Proofpoint, Inc.
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
package com.proofpoint.event.collector.queue;

import com.leansoft.bigqueue.utils.FileUtil;
import com.proofpoint.event.collector.BatchProcessorConfig;
import com.proofpoint.reporting.ReportExporter;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import static org.testng.AssertJUnit.assertFalse;

public class TestQueueFactory
{
    private QueueFactory queueFactory;

    @BeforeMethod
    public void setup()
    {
        queueFactory = new QueueFactory(new BatchProcessorConfig(), mock(ReportExporter.class));
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "config is null")
    public void testConstructorNullConfigInvalid()
    {
        new QueueFactory(null, mock(ReportExporter.class));
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "reportExporter is null")
    public void testConstructorNullReporterExporterInvalid()
    {
        new QueueFactory(new BatchProcessorConfig(), null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "name is null or empty")
    public void testCreateNullNameInvalid()
            throws IOException
    {
        queueFactory.create(null);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "name is null or empty")
    public void testCreateEmptyNameInvalid()
            throws IOException
    {
        queueFactory.create("");
    }

    @Test
    public void testTerminateQueueNotCreated()
            throws IOException
    {
        queueFactory.terminate("bogus");
    }

    @Test
    public void testTerminateQueueCannotDeleteDirectory()
            throws IOException
    {
        String queueName = "queueCannotDelete";
        File queueDir = new File("target", queueName);
        ReportExporter exporter = mock(ReportExporter.class);
        BatchProcessorConfig config = mock(BatchProcessorConfig.class);
        when(config.getDataDirectory()).thenReturn("target");
        when(config.getQueueSize()).thenReturn(5);
        FileUtil.deleteDirectory(queueDir);

        queueFactory = new QueueFactory(config, exporter);

        Set<PosixFilePermission> permissions = new HashSet<>();
        permissions.add(PosixFilePermission.OWNER_READ);

        queueFactory.create(queueName);
        assertTrue(queueDir.exists());
        Files.setPosixFilePermissions(queueDir.toPath(), permissions);

        try {
            queueFactory.terminate(queueName);
            fail("Expected IllegalStateException");
        }
        catch (IllegalStateException e) {
            assertTrue(true);
        }

        permissions = new HashSet<>();
        permissions.add(PosixFilePermission.OWNER_WRITE);
        permissions.add(PosixFilePermission.OWNER_READ);
        permissions.add(PosixFilePermission.OWNER_EXECUTE);
        Files.setPosixFilePermissions(queueDir.toPath(), permissions);
        FileUtil.deleteDirectory(queueDir);
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    @Test
    public void testTerminate()
            throws IOException
    {
        String queueName = "queueTerminate";
        File queueDir = new File("target", queueName);
        FileUtil.deleteDirectory(queueDir);
        assertFalse(queueDir.exists());

        ReportExporter exporter = mock(ReportExporter.class);
        BatchProcessorConfig config = mock(BatchProcessorConfig.class);
        when(config.getDataDirectory()).thenReturn("target");
        when(config.getQueueSize()).thenReturn(5);

        queueFactory = new QueueFactory(config, exporter);

        Queue queue = queueFactory.create(queueName);
        assertTrue(queueDir.exists());

        queueFactory.terminate(queueName);
        assertFalse(queueDir.exists());
        verify(exporter).unexport(QueueFactory.getMetricName(queue.getName()));
    }


}