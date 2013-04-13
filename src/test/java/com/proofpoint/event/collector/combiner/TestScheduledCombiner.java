/*
 * Copyright 2011-2013 Proofpoint, Inc.
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

import com.proofpoint.event.collector.ServerConfig;
import org.logicalshift.concurrent.SerialScheduledExecutorService;
import org.testng.annotations.Test;

import java.io.IOException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TestScheduledCombiner
{
    private static final long CHECK_DELAY = SECONDS.toMillis(10);

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "objectCombiner is null")
    public void testConstructorNullObjectCombiner()
    {
        new ScheduledCombiner(null, new SerialScheduledExecutorService(), new ServerConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "executorService is null")
    public void testConstructorNullExecutorService()
    {
        new ScheduledCombiner(mock(StoredObjectCombiner.class), null, new ServerConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "config is null")
    public void testConstructorNullConfig()
    {
        new ScheduledCombiner(mock(StoredObjectCombiner.class), new SerialScheduledExecutorService(), null);
    }

    @Test
    public void testScheduleFrequency()
            throws IOException
    {
        SerialScheduledExecutorService executorService = new SerialScheduledExecutorService();
        StoredObjectCombiner storedObjectCombiner = mock(StoredObjectCombiner.class);
        ScheduledCombiner scheduledCombiner = new ScheduledCombiner(storedObjectCombiner, executorService, new ServerConfig().setCombinerEnabled(true));
        scheduledCombiner.start();

        // Initial delay is 0, frequency is CHECK_DELAY
        verify(storedObjectCombiner, times(1)).combineAllObjects();
        for (int i = 1; i <= 10; ++i) {
            executorService.elapseTime(CHECK_DELAY - 1, MILLISECONDS);
            verify(storedObjectCombiner, times(i)).combineAllObjects();
            executorService.elapseTime(1, MILLISECONDS);
            verify(storedObjectCombiner, times(i + 1)).combineAllObjects();
            verifyNoMoreInteractions(storedObjectCombiner);
        }
        scheduledCombiner.destroy();
    }

    @Test
    public void testScheduleFrequencyWhenDisabled()
            throws IOException
    {
        SerialScheduledExecutorService executorService = new SerialScheduledExecutorService();
        StoredObjectCombiner storedObjectCombiner = mock(StoredObjectCombiner.class);
        ScheduledCombiner scheduledCombiner = new ScheduledCombiner(storedObjectCombiner, executorService, new ServerConfig().setCombinerEnabled(false));
        scheduledCombiner.start();

        executorService.elapseTime(CHECK_DELAY * 10, MILLISECONDS);
        verifyZeroInteractions(storedObjectCombiner);
        scheduledCombiner.destroy();
    }

    @Test
    public void testScheduleAgainAfterException()
            throws IOException
    {
        SerialScheduledExecutorService executorService = new SerialScheduledExecutorService();
        StoredObjectCombiner storedObjectCombiner = mock(StoredObjectCombiner.class);
        ScheduledCombiner scheduledCombiner = new ScheduledCombiner(storedObjectCombiner, executorService, new ServerConfig().setCombinerEnabled(true));
        scheduledCombiner.start();

        doThrow(new RuntimeException()).doNothing().when(storedObjectCombiner).combineAllObjects();
        verify(storedObjectCombiner, times(1)).combineAllObjects();
        executorService.elapseTime(CHECK_DELAY, MILLISECONDS);
        verify(storedObjectCombiner, times(2)).combineAllObjects();
        executorService.elapseTime(CHECK_DELAY, MILLISECONDS);
        verify(storedObjectCombiner, times(3)).combineAllObjects();
        verifyNoMoreInteractions(storedObjectCombiner);
        scheduledCombiner.destroy();
    }
}
