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

import com.google.common.collect.ImmutableList;
import com.proofpoint.event.collector.ServerConfig;
import org.logicalshift.concurrent.SerialScheduledExecutorService;
import org.mockito.stubbing.Stubber;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TestScheduledCombiner
{
    private static final int CHECK_DELAY_MILLIS = (int) MINUTES.toMillis(10);
    private static final int PER_TYPE_CHECK_DELAY_MILLIS = (int) SECONDS.toMillis(10);
    private static final int PER_TYPE_ITERATIONS_PER_CHECK = CHECK_DELAY_MILLIS / PER_TYPE_CHECK_DELAY_MILLIS;
    private static final String EVENT_TYPE_A = "eventA";
    private static final String EVENT_TYPE_B = "eventB";
    private static final String EVENT_TYPE_C = "eventC";

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
        StoredObjectCombiner storedObjectCombiner = createStoredObjectCombiner(ImmutableList.of(EVENT_TYPE_A), ImmutableList.of(EVENT_TYPE_A, EVENT_TYPE_B), ImmutableList.of(EVENT_TYPE_A, EVENT_TYPE_B, EVENT_TYPE_C));
        ScheduledCombiner scheduledCombiner = new ScheduledCombiner(storedObjectCombiner, executorService, createServerConfig());
        scheduledCombiner.start();

        // Initial delay to check for new event types is 0, frequency is CHECK_DELAY_MILLIS.
        // Initial delay to check for events of a given type is 0, delay between
        // iterations is PER_TYPE_CHECK_DELAY_MILLIS.
        verify(storedObjectCombiner, times(1)).combineObjects(EVENT_TYPE_A);

        for (int i = 1; i < PER_TYPE_ITERATIONS_PER_CHECK; ++i) {
            executorService.elapseTime(PER_TYPE_CHECK_DELAY_MILLIS - 1, MILLISECONDS);
            verify(storedObjectCombiner, times(i)).combineObjects(EVENT_TYPE_A);
            verify(storedObjectCombiner, never()).combineObjects(EVENT_TYPE_B);
            executorService.elapseTime(1, MILLISECONDS);
            verify(storedObjectCombiner, times(i + 1)).combineObjects(EVENT_TYPE_A);
            verify(storedObjectCombiner, never()).combineObjects(EVENT_TYPE_B);
        }

        executorService.elapseTime(PER_TYPE_CHECK_DELAY_MILLIS - 1, MILLISECONDS);
        verify(storedObjectCombiner, times(PER_TYPE_ITERATIONS_PER_CHECK)).combineObjects(EVENT_TYPE_A);
        executorService.elapseTime(1, MILLISECONDS);

        verify(storedObjectCombiner, times(PER_TYPE_ITERATIONS_PER_CHECK + 1)).combineObjects(EVENT_TYPE_A);
        verify(storedObjectCombiner, times(1)).combineObjects(EVENT_TYPE_B);
        verify(storedObjectCombiner, never()).combineObjects(EVENT_TYPE_C);

        scheduledCombiner.destroy();
    }

    @Test
    public void testScheduleFrequencyWhenDisabled()
            throws IOException
    {
        SerialScheduledExecutorService executorService = new SerialScheduledExecutorService();
        StoredObjectCombiner storedObjectCombiner = createStoredObjectCombiner();
        ScheduledCombiner scheduledCombiner = new ScheduledCombiner(storedObjectCombiner, executorService, createServerConfig().setCombinerEnabled(false));
        scheduledCombiner.start();

        executorService.elapseTime(CHECK_DELAY_MILLIS * 10, MILLISECONDS);
        verifyZeroInteractions(storedObjectCombiner);
        scheduledCombiner.destroy();
    }

    @Test
    public void testScheduleAgainAfterException()
            throws IOException
    {
        SerialScheduledExecutorService executorService = new SerialScheduledExecutorService();
        StoredObjectCombiner storedObjectCombiner = mock(StoredObjectCombiner.class);
        ScheduledCombiner scheduledCombiner = new ScheduledCombiner(storedObjectCombiner, executorService, createServerConfig());

        doReturn(ImmutableList.of())
                .doThrow(new RuntimeException())
                .doReturn(ImmutableList.of(EVENT_TYPE_A))
                .when(storedObjectCombiner).listEventTypes();
        scheduledCombiner.start();

        executorService.elapseTime(CHECK_DELAY_MILLIS, MILLISECONDS);
        verify(storedObjectCombiner, never()).combineObjects(any(String.class));

        executorService.elapseTime(CHECK_DELAY_MILLIS, MILLISECONDS);
        verify(storedObjectCombiner, times(1)).combineObjects(EVENT_TYPE_A);

        scheduledCombiner.destroy();
    }

    @Test
    public void testScheduleTypeAfterException()
    {
        SerialScheduledExecutorService executorService = new SerialScheduledExecutorService();
        StoredObjectCombiner storedObjectCombiner = createStoredObjectCombiner(EVENT_TYPE_A);
        ScheduledCombiner scheduledCombiner = new ScheduledCombiner(storedObjectCombiner, executorService, createServerConfig());
        scheduledCombiner.start();

        verify(storedObjectCombiner, times(1)).combineObjects(EVENT_TYPE_A);

        doThrow(new RuntimeException())
                .doNothing().when(storedObjectCombiner).combineObjects(any(String.class));
        executorService.elapseTime(PER_TYPE_CHECK_DELAY_MILLIS, MILLISECONDS);
        verify(storedObjectCombiner, times(2)).combineObjects(EVENT_TYPE_A);
        executorService.elapseTime(PER_TYPE_CHECK_DELAY_MILLIS, MILLISECONDS);
        verify(storedObjectCombiner, times(3)).combineObjects(EVENT_TYPE_A);
    }

    private StoredObjectCombiner createStoredObjectCombiner()
    {
        StoredObjectCombiner result = mock(StoredObjectCombiner.class);
        doReturn(ImmutableList.of()).when(result).listEventTypes();
        return result;
    }

    private StoredObjectCombiner createStoredObjectCombiner(List<String>... eventTypeLists)
    {
        StoredObjectCombiner result = mock(StoredObjectCombiner.class);
        if (eventTypeLists.length > 0) {
            Stubber stubber = doReturn(ImmutableList.copyOf(eventTypeLists[0]));
            for (int i = 1; i < eventTypeLists.length; ++i) {
                stubber = stubber.doReturn(eventTypeLists[i]);
            }
            stubber.when(result).listEventTypes();
        }
        return result;
    }

    private StoredObjectCombiner createStoredObjectCombiner(String... eventTypes)
    {
        StoredObjectCombiner result = mock(StoredObjectCombiner.class);
        doReturn(ImmutableList.copyOf(eventTypes)).when(result).listEventTypes();
        return result;
    }

    private ServerConfig createServerConfig()
    {
        return new ServerConfig().setCombinerEnabled(true);
    }
}
