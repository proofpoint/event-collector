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

import com.proofpoint.event.collector.BatchProcessorConfig;
import com.proofpoint.reporting.ReportExporter;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;

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
}