/*
 * Copyright 2011-2012 Proofpoint, Inc.
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
package com.proofpoint.event.collector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.event.collector.BatchProcessor.BatchHandler;
import org.testng.annotations.Test;

import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;

public class TestBatchCloner
{
    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "newDestinations is null")
    public void testSetDestinationsNull()
    {
        BatchCloner<Object> cloner = new BatchCloner<Object>();
        cloner.setDestinations(null);
    }

    @Test
    public void testSetDesintations()
    {
        BatchHandler<Object> handler1 = createMockHandler();
        BatchHandler<Object> handler2 = createMockHandler();
        BatchCloner<Object> cloner = new BatchCloner<Object>();
        List<Object> events1 = ImmutableList.of(new Object());
        List<Object> events2 = ImmutableList.of(new Object());
        List<Object> events3 = ImmutableList.of(new Object());

        cloner.processBatch(events1);
        verifyZeroInteractions(handler1);
        verifyZeroInteractions(handler2);

        cloner.setDestinations(ImmutableSet.of(handler1, handler2));
        cloner.processBatch(events2);
        verify(handler1, times(1)).processBatch(events2);
        verifyNoMoreInteractions(handler1);
        verify(handler2, times(1)).processBatch(events2);
        verifyNoMoreInteractions(handler2);

        reset(handler1, handler2);
        cloner.setDestinations(ImmutableSet.of(handler1));
        cloner.processBatch(events3);
        verify(handler1).processBatch(events3);
        verifyNoMoreInteractions(handler1);
        verifyZeroInteractions(handler2);
    }

    @Test
    public void testClear()
    {
        BatchHandler<Object> handler = createMockHandler();
        BatchCloner<Object> cloner = new BatchCloner<Object>();
        List<Object> events1 = ImmutableList.of(new Object());
        List<Object> events2 = ImmutableList.of(new Object());

        cloner.setDestinations(ImmutableSet.of(handler));
        cloner.processBatch(events1);
        verify(handler, times(1)).processBatch(events1);
        verifyNoMoreInteractions(handler);

        reset(handler);
        cloner.clear();
        cloner.processBatch(events2);
        verifyZeroInteractions(handler);
    }

    private BatchHandler<Object> createMockHandler()
    {
        return mock(BatchHandler.class);
    }
}
