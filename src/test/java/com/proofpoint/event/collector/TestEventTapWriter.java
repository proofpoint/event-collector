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
package com.proofpoint.event.collector;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceState;
import com.proofpoint.discovery.client.testing.StaticServiceSelector;
import com.proofpoint.event.collector.BatchProcessor.BatchHandler;
import com.proofpoint.event.collector.EventCollectorStats.Status;
import com.proofpoint.event.collector.EventTapFlow.Observer;
import com.proofpoint.log.Logger;
import com.proofpoint.stats.CounterStat;
import com.proofpoint.testing.SerialScheduledExecutorService;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Objects.firstNonNull;
import static com.google.common.base.Strings.nullToEmpty;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;

public class TestEventTapWriter
{
    private static final String typeA = "typeA";
    private static final String typeB = "typeB";
    private static final String typeC = "typeC";
    private static final String flowId1 = "1";
    private static final String flowId2 = "2";
    private static final String flowId3 = "3";
    private static final String instanceA = "a";
    private static final String instanceB = "b";
    private static final Event[] eventsA = createEvents(typeA, 10);
    private static final Event[] eventsB = createEvents(typeB, 10);
    private static final Event[] eventsC = createEvents(typeC, 10);
    private static final ServiceDescriptor tapA = createServiceDescriptor(typeA, flowId1, instanceA);
    private static final ServiceDescriptor tapA1 = tapA;
    private static final ServiceDescriptor tapA1a = tapA1;
    private static final ServiceDescriptor tapA1b = createServiceDescriptor(typeA, flowId1, instanceB);
    private static final ServiceDescriptor tapA2 = createServiceDescriptor(typeA, flowId2, instanceA);
    private static final ServiceDescriptor tapA2a = tapA2;
    private static final ServiceDescriptor tapA2b = createServiceDescriptor(typeA, flowId2, instanceB);
    private static final ServiceDescriptor tapB = createServiceDescriptor(typeB, flowId1, instanceA);
    private static final ServiceDescriptor tapB1 = tapB;
    private static final ServiceDescriptor tapB2 = createServiceDescriptor(typeB, flowId2, instanceA);
    private static final ServiceDescriptor tapB2a = tapB2;
    private static final ServiceDescriptor tapB2b = createServiceDescriptor(typeB, flowId2, instanceB);
    private static final ServiceDescriptor tapC = createServiceDescriptor(typeC, flowId1, instanceA);
    private static final ServiceDescriptor qtapA = createQosServiceDescriptor(typeA, flowId1, instanceA);
    private static final ServiceDescriptor qtapA1 = qtapA;
    private static final ServiceDescriptor qtapA2 = createQosServiceDescriptor(typeA, flowId2, instanceA);
    private static final ServiceDescriptor qtapA2a = qtapA2;
    private static final ServiceDescriptor qtapA2b = createQosServiceDescriptor(typeA, flowId2, instanceB);
    private static final ServiceDescriptor qtapA3 = createQosServiceDescriptor(typeA, flowId3, instanceA);
    private static final ServiceDescriptor qtapB = createQosServiceDescriptor(typeB, flowId1, instanceA);
    private static final ServiceDescriptor qtapB1 = qtapB;
    private static final ServiceDescriptor qtapB2 = createQosServiceDescriptor(typeB, flowId2, instanceA);
    private static final ServiceDescriptor qtapB3 = createQosServiceDescriptor(typeB, flowId3, instanceA);
    private static final ServiceDescriptor qtapB2a = qtapB2;
    private static final ServiceDescriptor qtapB2b = createQosServiceDescriptor(typeB, flowId2, instanceB);
    private static final ServiceDescriptor qtapC = createQosServiceDescriptor(typeC, flowId1, instanceA);

    private CounterStat counterForDelivered;
    private CounterStat counterForDropped;
    private CounterStat counterForRejected;
    private CounterStat counterForLost;
    private ServiceSelector serviceSelector;
    private Map<String, Boolean> currentProcessors;
    private SerialScheduledExecutorService executorService;
    private BatchProcessorFactory batchProcessorFactory = new MockBatchProcessorFactory();
    private Multimap<String, MockBatchProcessor<Event>> batchProcessors;
    private Map<String, Integer> expectedBatchProcessorDiscards;
    private EventTapFlowFactory eventTapFlowFactory = new MockEventTapFlowFactory();
    private Multimap<List<String>, MockEventTapFlow> nonQosEventTapFlows;
    private Multimap<List<String>, MockEventTapFlow> qosEventTapFlows;
    private EventTapConfig eventTapConfig;
    private EventTapWriter eventTapWriter;
    private EventCollectorStats eventCollectorStats;

    @BeforeMethod
    public void setup()
    {
        counterForDropped = new CounterStat();
        counterForLost = new CounterStat();
        counterForDelivered = new CounterStat();
        counterForRejected = new CounterStat();
        serviceSelector = new StaticServiceSelector(ImmutableSet.<ServiceDescriptor>of());
        currentProcessors = ImmutableMap.of();
        executorService = new SerialScheduledExecutorService();
        batchProcessors = LinkedListMultimap.create();      // Insertion order per-key matters
        expectedBatchProcessorDiscards = new HashMap<>();
        nonQosEventTapFlows = LinkedListMultimap.create();  // Insertion order per-key matters
        qosEventTapFlows = LinkedListMultimap.create();     // Insertion order per-key matters
        eventTapConfig = new EventTapConfig();
        serviceSelector = mock(ServiceSelector.class);
        eventCollectorStats = mock(EventCollectorStats.class);

        when(eventCollectorStats.outboundEvents(anyString(), anyString(), eq(Status.DELIVERED))).thenReturn(counterForDelivered);
        when(eventCollectorStats.outboundEvents(anyString(), anyString(), eq(Status.DROPPED))).thenReturn(counterForDropped);
        when(eventCollectorStats.outboundEvents(anyString(), anyString(), eq(Status.LOST))).thenReturn(counterForLost);
        when(eventCollectorStats.outboundEvents(anyString(), anyString(), anyString(), eq(Status.REJECTED))).thenReturn(counterForRejected);

        eventTapWriter = new EventTapWriter(
                serviceSelector, executorService,
                batchProcessorFactory, eventTapFlowFactory,
                eventTapConfig, eventCollectorStats);
        eventTapWriter.start();
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "selector is null")
    public void testConstructorNullSelector()
    {
        new EventTapWriter(null, executorService, batchProcessorFactory, eventTapFlowFactory, new EventTapConfig(), eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "executorService is null")
    public void testConstructorNullExecutorService()
    {
        new EventTapWriter(serviceSelector, null, batchProcessorFactory, eventTapFlowFactory, new EventTapConfig(), eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "batchProcessorFactory is null")
    public void testConstructorNullBatchProcessorFactory()
    {
        new EventTapWriter(serviceSelector, executorService, null, eventTapFlowFactory, new EventTapConfig(), eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventTapFlowFactory is null")
    public void testConstructorNullEventTapFlowFactory()
    {
        new EventTapWriter(serviceSelector, executorService, batchProcessorFactory, null, new EventTapConfig(), eventCollectorStats);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "config is null")
    public void testConstructorNullConfig()
    {
        new EventTapWriter(serviceSelector, executorService, batchProcessorFactory, eventTapFlowFactory, null, eventCollectorStats);
    }

    @Test
    public void testRefreshFlowsCreateNonQosTapFromExistingTap()
    {
        // [] -> tapA -> []
        // tapA -> tapA, tapB -> tapA
        testRefreshFlowsCreateOneTapFromExistingTap(tapA, tapB);
    }

    @Test
    public void testRefreshFlowsCreateQosTapFromExistingTap()
    {
        // [] -> tapA -> []
        // tapA -> tapA, qtapB -> tapA
        testRefreshFlowsCreateOneTapFromExistingTap(tapA, qtapB);
    }

    @Test
    public void testRefreshFlowsCreateNonQosTapFromExistingQosTap()
    {
        // [] -> qtapA -> []
        // qtapA -> qtapA, tapB -> qtapA
        testRefreshFlowsCreateOneTapFromExistingTap(qtapA, tapB);
    }

    @Test
    public void testRefreshFlowsCreateQosTapFromExistingQosTap()
    {
        // [] -> qtapA -> []
        // qtapA -> qtapA, qtapB -> qtapA
        testRefreshFlowsCreateOneTapFromExistingTap(qtapA, qtapB);
    }

    private void testRefreshFlowsCreateOneTapFromExistingTap(ServiceDescriptor tapA, ServiceDescriptor tapB)
    {
        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[0], eventsB[0]);
        forTap(tapA).verifyEvents(eventsA[0]);
        forTap(tapB).verifyNoFlow();

        updateThenRefreshFlowsThenCheck(tapA, tapB);
        writeEvents(eventsA[1], eventsB[1]);
        forTap(tapA).verifyEvents(eventsA[0], eventsA[1]);
        forTap(tapB).verifyEvents(eventsB[1]);

        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[2], eventsB[2]);
        forTap(tapA).verifyEvents(eventsA[0], eventsA[1], eventsA[2]);
        forTap(tapB).verifyEvents(eventsB[1]);

        updateThenRefreshFlowsThenCheck();
        writeEvents(eventsA[3], eventsB[3]);
        forTap(tapA).verifyEvents(eventsA[0], eventsA[1], eventsA[2]);
        forTap(tapB).verifyEvents(eventsB[1]);
    }

    @Test
    public void testRefreshFlowsCreateNonQosNonQosTaps()
    {
        // [] -> tapA, tapB -> []
        testRefreshFlowsCreateTwoTaps(tapA, tapB);
    }

    @Test
    public void testRefreshFlowsCreateQosQosTaps()
    {
        // [] -> qtapA, qtapB -> []
        testRefreshFlowsCreateTwoTaps(qtapA, qtapB);
    }

    @Test
    public void testRefreshFlowsCreateQosNonQosTaps()
    {
        // [] -> qtapA, tapB -> []
        testRefreshFlowsCreateTwoTaps(qtapA, tapB);
    }

    private void testRefreshFlowsCreateTwoTaps(ServiceDescriptor tapA, ServiceDescriptor tapB)
    {
        updateThenRefreshFlowsThenCheck(tapA, tapB);
        writeEvents(eventsA[0], eventsB[0]);
        forTap(tapA).verifyEvents(eventsA[0]);
        forTap(tapB).verifyEvents(eventsB[0]);

        updateThenRefreshFlowsThenCheck();
        writeEvents(eventsA[1], eventsB[1]);
        forTap(tapA).verifyEvents(eventsA[0]);
        forTap(tapB).verifyEvents(eventsB[0]);
    }

    @Test
    public void testRefreshFlowsCreateNonQosNonQosSameTaps()
    {
        // [] -> tapA1, tapA2 -> []
        testRefreshFlowsCreateTwoSameTaps(tapA1, tapA2);
    }

    @Test
    public void testRefreshFlowsCreateQosQosSameTaps()
    {
        // [] -> qtapA1, qtapA2 -> []
        testRefreshFlowsCreateTwoSameTaps(qtapA1, qtapA2);
    }

    private void testRefreshFlowsCreateTwoSameTaps(ServiceDescriptor tapA1, ServiceDescriptor tapA2)
    {
        updateThenRefreshFlowsThenCheck(tapA1, tapA2);
        writeEvents(eventsA[0], eventsB[0]);
        forTap(tapA1).verifyEvents(eventsA[0]);
        forTap(tapA2).verifyEvents(eventsA[0]);

        updateThenRefreshFlowsThenCheck();
        writeEvents(eventsA[1], eventsB[1]);
        forTap(tapA1).verifyEvents(eventsA[0]);
        forTap(tapA2).verifyEvents(eventsA[0]);
    }

    @Test
    public void testRefreshFlowsCreateNonQosNonQosTapsWithExistingTap()
    {
        // tapA -> tapA, tapB, tapC -> tapA
        testRefreshFlowsCreateTwoTapsWithExistingTap(tapA, tapB, tapC);
    }

    @Test
    public void testRefreshFlowsCreateQosQosTapsWithExistingTap()
    {
        // tapA -> tapA, qtapB, qtapC -> tapA
        testRefreshFlowsCreateTwoTapsWithExistingTap(tapA, qtapB, qtapC);
    }

    @Test
    public void testRefreshFlowsCreateQosNonQosTapsWithExistingTap()
    {
        // tapA -> tapA, qtapB, tapC -> tapA
        testRefreshFlowsCreateTwoTapsWithExistingTap(tapA, qtapB, tapC);
    }

    @Test
    public void testRefreshFlowsCreateNonQosNonQosTapsWithExistingQosTap()
    {
        // qtapA -> tapA, tapB, tapC -> qtapA
        testRefreshFlowsCreateTwoTapsWithExistingTap(qtapA, tapB, tapC);
    }

    @Test
    public void testRefreshFlowsCreateQosQosTapsWithExistingQosTap()
    {
        // qtapA -> tapA, qtapB, qtapC -> qtapA
        testRefreshFlowsCreateTwoTapsWithExistingTap(qtapA, qtapB, qtapC);
    }

    @Test
    public void testRefreshFlowsCreateQosNonQosTapsWithExistingQosTap()
    {
        // qtapA -> tapA, qtapB, tapC -> qtapA
        testRefreshFlowsCreateTwoTapsWithExistingTap(qtapA, qtapB, tapC);
    }

    private void testRefreshFlowsCreateTwoTapsWithExistingTap(ServiceDescriptor tapA, ServiceDescriptor tapB, ServiceDescriptor tapC)
    {
        // [q]tapA -> [q]tapX, [q]tapY, -> [q]tapA
        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[0], eventsB[0], eventsC[0]);
        forTap(tapA).verifyEvents(eventsA[0]);
        forTap(tapB).verifyNoFlow();
        forTap(tapC).verifyNoFlow();

        updateThenRefreshFlowsThenCheck(tapA, tapB, tapC);
        writeEvents(eventsA[1], eventsB[1], eventsC[1]);
        forTap(tapA).verifyEvents(eventsA[0], eventsA[1]);
        forTap(tapB).verifyEvents(eventsB[1]);
        forTap(tapC).verifyEvents(eventsC[1]);

        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[2], eventsB[2], eventsC[2]);
        forTap(tapA).verifyEvents(eventsA[0], eventsA[1], eventsA[2]);
        forTap(tapB).verifyEvents(eventsB[1]);
        forTap(tapC).verifyEvents(eventsC[1]);
    }

    @Test
    public void testRefreshFlowsCreateNonQosNonQosSameTapsWithExistingTap()
    {
        // tapA -> tapA, tapB1, tapB2 -> tapA
        testRefreshFlowsCreateTwoSameTapsWithExistingTap(tapA, tapB1, tapB2);
    }

    @Test
    public void testRefreshFlowsCreateQosQosSameTapsWithExistingTap()
    {
        // tapA -> qtapB1, qtapB2 -> tapA
        testRefreshFlowsCreateTwoSameTapsWithExistingTap(tapA, qtapB1, qtapB2);
    }

    @Test
    public void testRefreshFlowsCreateNonQosNonQosSameTapsWithExistingQosTap()
    {
        // qtapA -> tapA, tapB1, tapB2 -> qtapA
        testRefreshFlowsCreateTwoSameTapsWithExistingTap(qtapA, tapB1, tapB2);
    }

    @Test
    public void testRefreshFlowsCreateQosQosSameTapsWithExistingQosTap()
    {
        // qtapA -> qtapB1, qtapB2 -> qtapA
        testRefreshFlowsCreateTwoSameTapsWithExistingTap(qtapA, qtapB1, qtapB2);
    }

    private void testRefreshFlowsCreateTwoSameTapsWithExistingTap(ServiceDescriptor tapA, ServiceDescriptor tapB1, ServiceDescriptor tapB2)
    {
        // tapA -> tapA, tapB, tapC -> tapA
        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[0], eventsB[0]);
        forTap(tapA).verifyEvents(eventsA[0]);
        forTap(tapB1).verifyNoFlow();
        forTap(tapB2).verifyNoFlow();

        updateThenRefreshFlowsThenCheck(tapA, tapB1, tapB2);
        writeEvents(eventsA[1], eventsB[1]);
        forTap(tapA).verifyEvents(eventsA[0], eventsA[1]);
        forTap(tapB1).verifyEvents(eventsB[1]);
        forTap(tapB2).verifyEvents(eventsB[1]);

        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[2], eventsB[2]);
        forTap(tapA).verifyEvents(eventsA[0], eventsA[1], eventsA[2]);
        forTap(tapB1).verifyEvents(eventsB[1]);
        forTap(tapB2).verifyEvents(eventsB[1]);
    }

    @Test
    public void testRefreshFlowsCreateNonQosNonQosQos()
    {
        // [] -> tapA1, tapA2, qtapA3 -> []
        testRefreshFlowsCreateThreeSameTaps(tapA1, tapA2, qtapA3);
    }

    public void testRefreshFlowsCreateThreeSameTaps(ServiceDescriptor tapA1, ServiceDescriptor tapA2, ServiceDescriptor tapA3)
    {
        updateThenRefreshFlowsThenCheck(tapA1, tapA2, tapA3);
        writeEvents(eventsA[0]);
        forTap(tapA1).verifyEvents(eventsA[0]);
        forTap(tapA2).verifyEvents(eventsA[0]);
        forTap(tapA3).verifyEvents(eventsA[0]);

        updateThenRefreshFlowsThenCheck();
        writeEvents(eventsA[1]);
        forTap(tapA1).verifyEvents(eventsA[0]);
        forTap(tapA2).verifyEvents(eventsA[0]);
        forTap(tapA3).verifyEvents(eventsA[0]);
    }

    @Test
    public void testRefreshFlowsCreateNonQosTwoSameNonQos()
    {
        // [] -> tapA1, tapA2a, tapA2b -> []
        testRefreshFlowsCreateThreeSameTapsWithTwoShared(tapA1, tapA2a, tapA2b);
    }

    @Test
    public void testRefreshFlowsCreateNonQosWithPromotedQos()
    {
        // [] -> tapA1, tapA2a, qtapA2b -> []
        testRefreshFlowsCreateThreeSameTapsWithTwoShared(tapA1, tapA2a, qtapA2b);

    }

    @Test
    public void testRefreshFlowsCreateNonQosWithPromotedQosSecond()
    {
        // [] -> tapA1, qtapA2a, tapA2b -> []
        testRefreshFlowsCreateThreeSameTapsWithTwoShared(tapA1, qtapA2a, tapA2b);
    }

    public void testRefreshFlowsCreateThreeSameTapsWithTwoShared(ServiceDescriptor tapA1, ServiceDescriptor tapA2a, ServiceDescriptor tapA2b)
    {
        updateThenRefreshFlowsThenCheck(tapA1, tapA2a, tapA2b);
        writeEvents(eventsA[0]);
        forTap(tapA1).verifyEvents(eventsA[0]);
        forSharedTaps(tapA2a, tapA2b).verifyEvents(eventsA[0]);

        updateThenRefreshFlowsThenCheck();
        writeEvents(eventsA[1]);
        forTap(tapA1).verifyEvents(eventsA[0]);
        forSharedTaps(tapA2a, tapA2b).verifyEvents(eventsA[0]);
    }

    @Test
    public void testRefreshFlowsCreateNonQosNonQosQosWithExistingTap()
    {
        // tapA -> tapB1, tapB2, qtapB3 -> tapA
        testRefreshFlowsCreateThreeSameTapsWithExistingTap(tapA, tapB1, tapB2, qtapB3);
    }

    @Test
    public void testRefreshFlowsCreateNonQosNonQosQosWithExistingQosTap()
    {
        // tapA -> tapB1, tapB2, qtapB3 -> tapA
        testRefreshFlowsCreateThreeSameTapsWithExistingTap(qtapA, tapB1, tapB2, qtapB3);
    }

    private void testRefreshFlowsCreateThreeSameTapsWithExistingTap(ServiceDescriptor tapA, ServiceDescriptor tapB1, ServiceDescriptor tapB2, ServiceDescriptor tapB3)
    {
        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[0], eventsB[0]);
        forTap(tapA).verifyEvents(eventsA[0]);
        forTap(tapB1).verifyNoFlow();
        forTap(tapB2).verifyNoFlow();
        forTap(tapB3).verifyNoFlow();

        updateThenRefreshFlowsThenCheck(tapA, tapB1, tapB2, tapB3);
        writeEvents(eventsA[1], eventsB[1]);
        forTap(tapA).verifyEvents(eventsA[0], eventsA[1]);
        forTap(tapB1).verifyEvents(eventsB[1]);
        forTap(tapB2).verifyEvents(eventsB[1]);
        forTap(tapB3).verifyEvents(eventsB[1]);

        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[2], eventsB[2]);
        forTap(tapA).verifyEvents(eventsA[0], eventsA[1], eventsA[2]);
        forTap(tapB1).verifyEvents(eventsB[1]);
        forTap(tapB2).verifyEvents(eventsB[1]);
        forTap(tapB3).verifyEvents(eventsB[1]);
    }

    @Test
    public void testRefreshFlowsCreateNonQosWithSharedNonQosNonQosWithExistingTap()
    {
        // tapA -> tapA, tapB1, tapB2a, tapB2b -> tapA
        testRefreshFlowsCreateThreeSameTapsWithTwoSharedWithExistingTap(tapA, tapB1, tapB2a, tapB2b);
    }

    @Test
    public void testRefreshFlowsCreateNonQosWithSharedNonQosQosWithExistingTap()
    {
        // tapA -> tapA, tapB1, tapB2a, qtapB2b -> tapA
        testRefreshFlowsCreateThreeSameTapsWithTwoSharedWithExistingTap(tapA, tapB1, tapB2a, qtapB2b);
    }

    @Test
    public void testRefreshFlowsCreateNonQosWithSharedQosNonQosWithExistingTap()
    {
        // tapA -> tapA, tapB1, qtapB2a, tapB2b -> tapA
        testRefreshFlowsCreateThreeSameTapsWithTwoSharedWithExistingTap(tapA, tapB1, qtapB2a, tapB2b);
    }

    @Test
    public void testRefreshFlowsCreateQosWithPromotedQosWithExistingTap()
    {
        // tapA -> tapA, tapB1, qtapB2a, qtapB2b -> tapA
        testRefreshFlowsCreateThreeSameTapsWithTwoSharedWithExistingTap(tapA, tapB1, qtapB2a, qtapB2b);
    }

    @Test
    public void testRefreshFlowsCreateNonQosWithSharedNonQosNonQosWithExistingQosTap()
    {
        // qtapA -> qtapA, tapB1, tapB2a, tapB2b -> qtapA
        testRefreshFlowsCreateThreeSameTapsWithTwoSharedWithExistingTap(qtapA, tapB1, tapB2a, tapB2b);
    }

    @Test
    public void testRefreshFlowsCreateNonQosWithSharedNonQosQosWithExistingQosTap()
    {
        // qtapA -> qtapA, tapB1, tapB2a, qtapB2b -> qtapA
        testRefreshFlowsCreateThreeSameTapsWithTwoSharedWithExistingTap(qtapA, tapB1, tapB2a, qtapB2b);
    }

    @Test
    public void testRefreshFlowsCreateNonQosWithSharedQosNonQosWithExistingQosTap()
    {
        // qtapA -> qtapA, tapB1, qtapB2a, tapB2b -> qtapA
        testRefreshFlowsCreateThreeSameTapsWithTwoSharedWithExistingTap(qtapA, tapB1, qtapB2a, tapB2b);
    }

    @Test
    public void testRefreshFlowsCreateQosWithSharedNonQosQosWithExistingQosTap()
    {
        // qtapA -> qtapA, tapB1, qtapB2a, qtapB2b -> qtapA
        testRefreshFlowsCreateThreeSameTapsWithTwoSharedWithExistingTap(qtapA, tapB1, qtapB2a, qtapB2b);
    }

    private void testRefreshFlowsCreateThreeSameTapsWithTwoSharedWithExistingTap(ServiceDescriptor tapA, ServiceDescriptor tapB1, ServiceDescriptor tapB2a, ServiceDescriptor tapB2b)
    {
        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[0], eventsB[0]);
        forTap(tapA).verifyEvents(eventsA[0]);
        forTap(tapB1).verifyNoFlow();
        forSharedTaps(tapB2a, tapB2b).verifyNoFlow();

        updateThenRefreshFlowsThenCheck(tapA, tapB1, tapB2a, tapB2b);
        writeEvents(eventsA[1], eventsB[1]);
        forTap(tapA).verifyEvents(eventsA[0], eventsA[1]);
        forTap(tapB1).verifyEvents(eventsB[1]);
        forSharedTaps(tapB2a, tapB2b).verifyEvents(eventsB[1]);

        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[2], eventsB[2]);
        forTap(tapA).verifyEvents(eventsA[0], eventsA[1], eventsA[2]);
        forTap(tapB1).verifyEvents(eventsB[1]);
        forSharedTaps(tapB2a, tapB2b).verifyEvents(eventsB[1]);
    }

    @Test
    public void testRefreshFlowsSwapSameTaps()
    {
        // tapA1 -> tapA2
        testRefreshFlowsSwapSameTaps(tapA1, tapA2);
    }

    @Test
    public void testRefreshFlowsNonQosToSameQos()
    {
        // tapA1 -> qtapA2
        testRefreshFlowsSwapSameTaps(tapA1, qtapA2);
    }

    @Test
    public void testRefreshFlowsQosToIdenticalNonQos()
    {
        // qtapA1 -> tapA2
        testRefreshFlowsSwapSameTaps(qtapA1, tapA2);
    }

    @Test
    void testRefreshFlowsQosToSameNonQos()
    {
        // qtapA1 -> tapA2
        testRefreshFlowsSwapSameTaps(qtapA1, tapA2);
    }

    private void testRefreshFlowsSwapSameTaps(ServiceDescriptor tapA1, ServiceDescriptor tapA2)
    {
        updateThenRefreshFlowsThenCheck(tapA1);
        writeEvents(eventsA[0]);
        forTap(tapA1).verifyEvents(eventsA[0]);
        forTap(tapA2).verifyNoFlow();

        updateThenRefreshFlowsThenCheck(tapA2);
        writeEvents(eventsA[1]);
        forTap(tapA1).verifyEvents(eventsA[0]);
        forTap(tapA2).verifyEvents(eventsA[1]);
    }

    @Test
    public void testRefreshFlowsNonQosToIdenticalQos()
    {
        // tapA1 -> qtapA1
        updateThenRefreshFlowsThenCheck(tapA1);
        writeEvents(eventsA[0]);
        forTap(tapA1).verifyEvents(eventsA[0]);
        forTap(qtapA1).verifyNoFlow();

        updateThenRefreshFlowsThenCheck(qtapA1);
        writeEvents(eventsA[1]);
        forTap(tapA1).verifyEvents(eventsA[0]);
        forTap(qtapA1).verifyEvents(eventsA[1]);
    }

    @Test
    public void testRefreshFlowsQosToIdenticalQos()
    {
        // tapA1a -> tapA1b
        updateThenRefreshFlowsThenCheck(tapA1a);
        writeEvents(eventsA[0]);
        forTap(tapA1a).verifyEvents(eventsA[0]);

        updateThenRefreshFlowsThenCheck(tapA1b);
        writeEvents(eventsA[1]);
        forTap(tapA1b).verifyEvents(eventsA[0], eventsA[1]);
    }

    @Test
    public void testRefreshFlowsRemovesOldEntries()
    {
        updateThenRefreshFlowsThenCheck(tapA, tapB);
        writeEvents(eventsA[0], eventsB[0]);
        forTap(tapA).verifyEvents(eventsA[0]);
        forTap(tapB).verifyEvents(eventsB[0]);

        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[1], eventsB[1]);
        forTap(tapA).verifyEvents(eventsA[0], eventsA[1]);
        forTap(tapB).verifyEvents(eventsB[0]);
    }

    @Test
    public void testRefreshFlowsUpdatesExistingProcessor()
    {
        // If the taps for a given event type changes, don't create the processor
        updateThenRefreshFlowsThenCheck(tapA1);
        writeEvents(eventsA[0]);
        forTap(tapA1).verifyEvents(eventsA[0]);
        forTap(tapA2).verifyNoFlow();

        updateThenRefreshFlowsThenCheck(tapA2);
        writeEvents(eventsA[1]);
        forTap(tapA1).verifyEvents(eventsA[0]);
        forTap(tapA2).verifyEvents(eventsA[1]);
    }

    @Test
    public void testRefreshFlowsIsCalledPeriodically()
    {
        String batchProcessorNameA = extractProcessorName(tapA);
        String batchProcessorNameB = extractProcessorName(tapB);
        String batchProcessorNameC = extractProcessorName(tapC);
        updateTaps(tapA);
        executorService.elapseTime(
                (long) eventTapConfig.getEventTapRefreshDuration().toMillis() - 1,
                TimeUnit.MILLISECONDS);
        assertFalse(batchProcessors.containsKey(batchProcessorNameA));
        executorService.elapseTime(1, TimeUnit.MILLISECONDS);
        assertTrue(batchProcessors.containsKey(batchProcessorNameA));
        assertEquals(batchProcessors.get(batchProcessorNameA).size(), 1);

        // If the refreshFlows() is called after the period, tapB should be
        // created to handle the new tap after one period.
        updateTaps(tapB);
        executorService.elapseTime(
                (long) eventTapConfig.getEventTapRefreshDuration().toMillis() - 1,
                TimeUnit.MILLISECONDS);
        assertFalse(batchProcessors.containsKey(batchProcessorNameB));
        executorService.elapseTime(1, TimeUnit.MILLISECONDS);
        assertTrue(batchProcessors.containsKey(batchProcessorNameB));
        assertEquals(batchProcessors.get(batchProcessorNameB).size(), 1);

        // Same is true after the second period, but with tapC.
        updateTaps(tapC);
        executorService.elapseTime(
                (long) eventTapConfig.getEventTapRefreshDuration().toMillis() - 1,
                TimeUnit.MILLISECONDS);
        assertFalse(batchProcessors.containsKey(batchProcessorNameC));
        executorService.elapseTime(1, TimeUnit.MILLISECONDS);
        assertTrue(batchProcessors.containsKey(batchProcessorNameC));
        assertEquals(batchProcessors.get(batchProcessorNameC).size(), 1);
    }

    @Test
    public void testRefreshFlowsStillHappensAfterException()
    {
        String batchProcessorName = extractProcessorName(tapA);

        // Cause exception, which we expect to be handled
        updateTaps(new RuntimeException("Thrown deliberately"));
        executorService.elapseTime(
                (long) eventTapConfig.getEventTapRefreshDuration().toMillis(),
                TimeUnit.MILLISECONDS);
        verify(serviceSelector, atLeastOnce()).selectAllServices();

        // If the refreshFlows() is rescheduled after the exception, tap should be
        // created to handle the new tap after one period.
        updateTaps(tapA);
        executorService.elapseTime(
                (long) eventTapConfig.getEventTapRefreshDuration().toMillis() - 1,
                TimeUnit.MILLISECONDS);
        assertFalse(batchProcessors.containsKey(batchProcessorName));
        executorService.elapseTime(1, TimeUnit.MILLISECONDS);
        assertTrue(batchProcessors.containsKey(batchProcessorName));
        assertEquals(batchProcessors.get(batchProcessorName).size(), 1);
    }

    @Test
    public void testWritePartitionsByType()
    {
        updateThenRefreshFlowsThenCheck(tapA, tapB);
        writeEvents(eventsA[0], eventsB[0], eventsC[0]);
        forTap(tapA).verifyEvents(eventsA[0]);
        forTap(tapB).verifyEvents(eventsB[0]);
    }

    @Test
    public void testWriteSendsToNewFlows()
    {
        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[0], eventsB[0]);
        forTap(tapA).verifyEvents(eventsA[0]);

        updateThenRefreshFlowsThenCheck(tapA, tapB);
        writeEvents(eventsA[1], eventsB[1]);
        forTap(tapA).verifyEvents(eventsA[0], eventsA[1]);
        forTap(tapB).verifyEvents(eventsB[1]);
    }

    @Test
    public void testWriteDoesntSendToOldFlows()
    {
        updateThenRefreshFlowsThenCheck(tapA, tapB);
        writeEvents(eventsA[0], eventsB[0]);
        forTap(tapA).verifyEvents(eventsA[0]);
        forTap(tapB).verifyEvents(eventsB[0]);

        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[1], eventsB[1]);
        forTap(tapA).verifyEvents(eventsA[0], eventsA[1]);
        forTap(tapB).verifyEvents(eventsB[0]);
    }

    @Test
    public void testMetricForDroppedEvents()
    {
        updateThenRefreshFlowsThenCheck(tapA);
        String batchProcessorName = extractProcessorName(tapA);
        String eventType = extractEventType(tapA);
        String flowId = extractFlowId(tapA);
        String uri = extractUri(tapA);

        MockBatchProcessor<Event> processor = batchProcessors.get(batchProcessorName).iterator().next();

        writeEvents(eventsA[0]);
        verify(eventCollectorStats).outboundEvents(eventType, flowId, Status.DELIVERED);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, Status.DROPPED);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, Status.LOST);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, uri, Status.REJECTED);

        processor.succeed = false;

        writeEvents(eventsA[1]);
        verify(eventCollectorStats).outboundEvents(eventType, flowId, Status.DROPPED);
        verify(eventCollectorStats).outboundEvents(eventType, flowId, Status.DELIVERED);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, Status.LOST);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, uri, Status.REJECTED);
    }

    @Test
    public void testMetricForLostEvents()
    {
        updateThenRefreshFlowsThenCheck(tapA);
        String eventType = extractEventType(tapA);
        String flowId = extractFlowId(tapA);
        String uri = extractUri(tapA);

        MockEventTapFlow eventTapFlow = nonQosEventTapFlows.get(ImmutableList.of(eventType, flowId)).iterator().next();

        writeEvents(eventsA[0]);
        verify(eventCollectorStats).outboundEvents(eventType, flowId, Status.DELIVERED);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, Status.DROPPED);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, Status.LOST);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, uri, Status.REJECTED);

        eventTapFlow.setToLoseEvent();
        writeEvents(eventsA[1]);

        verify(eventCollectorStats).outboundEvents(eventType, flowId, Status.LOST);
        verify(eventCollectorStats).outboundEvents(eventType, flowId, Status.DELIVERED);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, Status.DROPPED);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, uri, Status.REJECTED);
    }

    @Test
    public void testMetricForRejectedEvents()
    {
        updateThenRefreshFlowsThenCheck(tapA);
        String eventType = extractEventType(tapA);
        String flowId = extractFlowId(tapA);
        String uri = extractUri(tapA);

        MockEventTapFlow eventTapFlow = nonQosEventTapFlows.get(ImmutableList.of(eventType, flowId)).iterator().next();

        writeEvents(eventsA[0]);
        verify(eventCollectorStats).outboundEvents(eventType, flowId, Status.DELIVERED);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, Status.DROPPED);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, Status.LOST);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, uri, Status.REJECTED);

        eventTapFlow.setToRejectEvent();
        writeEvents(eventsA[1]);

        verify(eventCollectorStats).outboundEvents(eventType, flowId, uri, Status.REJECTED);
        verify(eventCollectorStats).outboundEvents(eventType, flowId, Status.DELIVERED);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, Status.DROPPED);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, Status.LOST);

        assertEquals(2, counterForDelivered.getTotalCount() + counterForRejected.getTotalCount() + counterForLost.getTotalCount());
    }

    @Test
    public void testMetricForDeliveredEvents()
    {
        updateThenRefreshFlowsThenCheck(tapA);
        String eventType = extractEventType(tapA);
        String flowId = extractFlowId(tapA);
        String uri = extractUri(tapA);

        writeEvents(eventsA[0]);
        verify(eventCollectorStats).outboundEvents(eventType, flowId, Status.DELIVERED);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, Status.DROPPED);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, Status.LOST);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, uri, Status.REJECTED);

        writeEvents(eventsA[1]);

        verify(eventCollectorStats, times(2)).outboundEvents(eventType, flowId, Status.DELIVERED);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, Status.DROPPED);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, Status.LOST);
        verify(eventCollectorStats, never()).outboundEvents(eventType, flowId, uri, Status.REJECTED);
    }

    private void updateThenRefreshFlowsThenCheck(ServiceDescriptor... taps)
    {
        // Figure out which of the processors should have been destroyed.
        // This happens if: (a) The flow disappears, or (b) the flow switches
        // between QoS and non-QoS.
        Map<String, Boolean> newProcessors = createProcessorsForTaps(taps);
        for (Map.Entry<String, Boolean> entry : currentProcessors.entrySet()) {
            String processorName = entry.getKey();
            boolean processorQos = entry.getValue();
            Boolean currentProcessor = newProcessors.get(processorName);
            if (currentProcessor == null || currentProcessor != processorQos) {
                recordExpectedProcessorDiscards(processorName);
            }
        }
        currentProcessors = newProcessors;

        updateTaps(taps);
        eventTapWriter.refreshFlows();
        checkActiveProcessors(taps);
    }

    private Map<String, Boolean> createProcessorsForTaps(ServiceDescriptor[] taps)
    {
        HashMap<String, Boolean> result = new HashMap<>();
        for (ServiceDescriptor tap : taps) {
            String processorName = extractProcessorName(tap);
            boolean qos = nullToEmpty(tap.getProperties().get("qos.delivery")).equalsIgnoreCase("retry");
            boolean existingQos = firstNonNull(result.get(processorName), Boolean.valueOf(false));
            result.put(processorName, existingQos | qos);
        }
        return ImmutableMap.copyOf(result);
    }

    private void recordExpectedProcessorDiscards(String processorName)
    {
        int current = firstNonNull(expectedBatchProcessorDiscards.get(processorName), Integer.valueOf(0));
        expectedBatchProcessorDiscards.put(processorName, current + 1);
    }

    private void updateTaps(ServiceDescriptor... taps)
    {
        doReturn(ImmutableList.copyOf(taps)).when(serviceSelector).selectAllServices();
    }

    private void updateTaps(Exception e)
    {
        doThrow(e).when(serviceSelector).selectAllServices();
    }

    private void writeEvents(Event... events)
    {
        for (Event event : events) {
            eventTapWriter.write(event);
        }
    }

    private static Event createEvent(String type)
    {
        return new Event(type, randomUUID().toString(), "host", DateTime.now(), ImmutableMap.<String, Object>of());
    }

    private static Event[] createEvents(String type, int count)
    {
        Event[] results = new Event[count];
        for (int i = 0; i < count; ++i) {
            results[i] = createEvent(type);
        }
        return results;
    }

    private void checkActiveProcessors(ServiceDescriptor... taps)
    {
        List<ServiceDescriptor> tapsAsList = ImmutableList.copyOf(taps);

        for (ServiceDescriptor tap : tapsAsList) {
            String processorName = extractProcessorName(tap);
            int expectedDiscards = firstNonNull(expectedBatchProcessorDiscards.get(processorName), Integer.valueOf(0));
            assertTrue(batchProcessors.containsKey(processorName), format("no processor created for %s", processorName));

            List<MockBatchProcessor<Event>> processors = ImmutableList.copyOf(batchProcessors.get(processorName));
            assertEquals(processors.size(), expectedDiscards + 1, format("wrong number of processors for %s", processorName));
            for (int i = 0; i < expectedDiscards; ++i) {
                MockBatchProcessor<Event> processor = processors.get(i);
                assertEquals(processor.startCount, 1, format("invalid start count for discarded processor %s[%d]", processorName, i));
                assertEquals(processor.stopCount, 1, format("invalid stop count for discarded processor %s[%d]", processorName, i));
            }

            // The batch processor should have been started, but not stopped
            // if it is still active.
            MockBatchProcessor<Event> processor = processors.get(expectedDiscards);
            assertEquals(processor.startCount, 1, format("invalid start count for processor %s[%d]", processorName, expectedDiscards));
            assertEquals(processor.stopCount, 0, format("invalid stop count for processor %s[%d]", processorName, expectedDiscards));
        }

        // For all non-active processors, make sure they have been stopped.
        for (Entry<String, Collection<MockBatchProcessor<Event>>> entry : batchProcessors.asMap().entrySet()) {
            String processorName = entry.getKey();
            int expectedDiscards = firstNonNull(expectedBatchProcessorDiscards.get(processorName), Integer.valueOf(0));
            ServiceDescriptor tap = null;
            for (ServiceDescriptor t : tapsAsList) {
                if (processorName.equals(extractProcessorName(t))) {
                    tap = t;
                    break;
                }
            }
            if (tap != null) {
                continue;           // Handled in loop above
            }

            List<MockBatchProcessor<Event>> processors = ImmutableList.copyOf(entry.getValue());
            assertEquals(processors.size(), expectedDiscards, format("wrong number of processors for %s", processorName));

            // The batch processor should have been started and stopped.
            for (int i = 0; i < expectedDiscards; ++i) {
                MockBatchProcessor<Event> processor = processors.get(i);
                assertEquals(processor.startCount, 1, format("invalid start count for processor %s[%d]", processorName, i));
                assertEquals(processor.stopCount, 1, format("invalid stop count for processor %s[%d]", processorName, i));
            }
        }
    }

    private EventTapFlowVerifier forTap(ServiceDescriptor tap)
    {
        return forSharedTaps(tap);
    }

    private EventTapFlowVerifier forSharedTaps(ServiceDescriptor... taps)
    {
        ImmutableSet.Builder<URI> urisBuilder = ImmutableSet.builder();
        String eventType = null;
        String flowId = null;
        boolean qosEnabled = false;

        for (ServiceDescriptor tap : taps) {
            String thisEventType = tap.getProperties().get("eventType");
            String thisFlowId = tap.getProperties().get(EventTapWriter.FLOW_ID_PROPERTY_NAME);
            String thisUri = tap.getProperties().get("http");
            boolean thisQosEnabled = nullToEmpty(tap.getProperties().get("qos.delivery")).equalsIgnoreCase("retry");

            assertNotNull(thisEventType);
            assertNotNull(thisFlowId);
            assertNotNull(thisUri);
            if (eventType != null) {
                assertEquals(thisEventType, eventType, "multiple taps must have the same EventType");
                assertEquals(thisFlowId, flowId, "multiple taps must have the same flowId");
            }
            else {
                eventType = thisEventType;
                flowId = thisFlowId;
            }
            urisBuilder.add(URI.create(thisUri));
            if (thisQosEnabled) {
                qosEnabled = true;
            }
        }

        assertNotNull(eventType, "No taps specified?");
        assertNotNull(flowId, "No taps specified?");

        return new EventTapFlowVerifier(urisBuilder.build(), eventType, flowId, qosEnabled);
    }

    private static String extractProcessorName(ServiceDescriptor tap)
    {
        return format("%s{%s}", tap.getProperties().get("eventType"),
                tap.getProperties().get(EventTapWriter.FLOW_ID_PROPERTY_NAME));
    }

    private static String extractEventType(ServiceDescriptor tap)
    {
        return tap.getProperties().get("eventType");
    }

    private static String extractFlowId(ServiceDescriptor tap)
    {
        return tap.getProperties().get(EventTapWriter.FLOW_ID_PROPERTY_NAME);
    }

    private static String extractUri(ServiceDescriptor tap)
    {
        return tap.getProperties().get("http");
    }

    private static ServiceDescriptor createServiceDescriptor(String eventType, Map<String, String> properties)
    {
        String nodeId = randomUUID().toString();
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        builder.putAll(properties);
        builder.put("eventType", eventType);
        if (!properties.containsKey(EventTapWriter.FLOW_ID_PROPERTY_NAME)) {
            builder.put(EventTapWriter.FLOW_ID_PROPERTY_NAME, "1");
        }
        if (!properties.containsKey("tapId")) {
            builder.put("tapId", randomUUID().toString());
        }
        if (!properties.containsKey("http")) {
            builder.put("http", format("http://%s.event.tap", eventType));
        }
        return new ServiceDescriptor(
                randomUUID(),
                nodeId,
                "EventTap",
                "global",
                "/" + nodeId,
                ServiceState.RUNNING,
                builder.build());
    }

    private static ServiceDescriptor createServiceDescriptor(String eventType, String flowId, String instanceId)
    {
        return createServiceDescriptor(eventType,
                ImmutableMap.of(EventTapWriter.FLOW_ID_PROPERTY_NAME, flowId, "http", format("http://%s-%s.event.tap", eventType, instanceId)));
    }

    private static ServiceDescriptor createQosServiceDescriptor(String eventType, String flowId, String instanceId)
    {
        return createServiceDescriptor(eventType,
                ImmutableMap.of("qos.delivery", "retry",
                        EventTapWriter.FLOW_ID_PROPERTY_NAME, flowId,
                        "http", format("http://%s-%s.event.tap", eventType, instanceId)));
    }

    private class MockBatchProcessorFactory implements BatchProcessorFactory
    {
        @Override
        @SuppressWarnings("unchecked")
        public <T> BatchProcessor<T> createBatchProcessor(String name, BatchHandler<T> batchHandler, BatchProcessor.Observer observer)
        {
            Logger.get(EventTapWriter.class).error("Create Batch Processor %s", name);
            MockBatchProcessor batchProcessor = new MockBatchProcessor(name, batchHandler, observer);
            batchProcessors.put(name, batchProcessor);
            return batchProcessor;
        }

        @Override
        public <T> BatchProcessor<T> createBatchProcessor(String name, BatchHandler<T> batchHandler)
        {
            return createBatchProcessor(name, batchHandler, BatchProcessor.NULL_OBSERVER);
        }
    }

    private static class MockBatchProcessor<T> implements BatchProcessor<T>
    {
        public int startCount = 0;
        public int stopCount = 0;
        public boolean succeed = true;
        public List<T> entries = new LinkedList<>();
        public final String name;

        private final BatchHandler<T> handler;
        private final Observer observer;

        public MockBatchProcessor(String name, BatchHandler<T> batchHandler, Observer observer)
        {
            this.name = name;
            this.handler = batchHandler;
            this.observer = observer;
        }

        @Override
        public void start()
        {
            startCount += 1;
        }

        @Override
        public void stop()
        {
            stopCount += 1;
        }

        @Override
        public void put(T entry)
        {
            entries.add(entry);
            if (succeed) {
                handler.processBatch(ImmutableList.of(entry));
            }
            else {
                observer.onRecordsDropped(1);
            }
        }
    }

    private class MockEventTapFlowFactory implements EventTapFlowFactory
    {
        @Override
        public EventTapFlow createEventTapFlow(String eventType, String flowId, Set<URI> taps, Observer observer)
        {
            return createEventTapFlow(nonQosEventTapFlows, eventType, flowId, taps, observer);
        }

        @Override
        public EventTapFlow createEventTapFlow(String eventType, String flowId, Set<URI> taps)
        {
            return createEventTapFlow(eventType, flowId, taps, EventTapFlow.NULL_OBSERVER);
        }

        @Override
        public EventTapFlow createQosEventTapFlow(String eventType, String flowId, Set<URI> taps, Observer observer)
        {
            return createEventTapFlow(qosEventTapFlows, eventType, flowId, taps, observer);
        }

        @Override
        public EventTapFlow createQosEventTapFlow(String eventType, String flowId, Set<URI> taps)
        {
            return createQosEventTapFlow(eventType, flowId, taps, EventTapFlow.NULL_OBSERVER);
        }

        private EventTapFlow createEventTapFlow(Multimap<List<String>, MockEventTapFlow> eventTapFlows, String eventType, String flowId, Set<URI> taps, Observer observer)
        {
            List<String> key = ImmutableList.of(eventType, flowId);
            MockEventTapFlow eventTapFlow = new MockEventTapFlow(taps, observer);
            eventTapFlows.put(key, eventTapFlow);
            return eventTapFlow;
        }
    }

    private static class MockEventTapFlow implements EventTapFlow
    {
        private Set<URI> taps;
        private List<Event> events;
        private Status status;

        private final Observer observer;

        private enum Status
        {
            DELIVERED           // events successfully delivered to the consumer
                    {
                        @Override
                        public void observe(Observer observer, URI next, int count)
                        {
                            observer.onRecordsDelivered(count);
                        }
                    },
            LOST                 // events couldn't be delivered because all taps returned 5XX error
                    {
                        @Override
                        public void observe(Observer observer, URI next, int count)
                        {
                            observer.onRecordsLost(count);
                        }
                    },
            REJECTED             // events couldn't be delivered because a tap rejected with 4XX error
                    {
                        @Override
                        public void observe(Observer observer, URI uri, int count)
                        {
                            observer.onRecordsRejected(uri, count);
                        }
                    };

            @Override
            public String toString()
            {
                return name().toLowerCase();
            }

            public abstract void observe(Observer observer, URI next, int count);
        }

        public MockEventTapFlow(Set<URI> taps, Observer observer)
        {
            this.taps = taps;
            this.events = new LinkedList<>();
            this.observer = observer;
            this.status = Status.DELIVERED;
        }

        @Override
        public Set<URI> getTaps()
        {
            return taps;
        }

        @Override
        public void setTaps(Set<URI> taps)
        {
            this.taps = taps;
        }

        @Override
        public void processBatch(List<Event> entries)
        {
            events.addAll(entries);

            status.observe(observer, taps.iterator().next(), entries.size());
        }

        @Override
        public void notifyEntriesDropped(int count)
        {
        }

        public List<Event> getEvents()
        {
            return ImmutableList.copyOf(events);
        }

        public void setToLoseEvent()
        {
            status = Status.LOST;
        }

        public void setToRejectEvent()
        {
            status = Status.REJECTED;
        }
    }

    private class EventTapFlowVerifier
    {
        private final Set<URI> taps;
        private final String eventType;
        private final String flowId;
        private final List<String> key;
        private final boolean qosEnabled;

        public EventTapFlowVerifier(Set<URI> taps, String eventType, String flowId, boolean qosEnabled)
        {
            this.taps = taps;
            this.eventType = eventType;
            this.flowId = flowId;
            this.key = ImmutableList.of(eventType, flowId);
            this.qosEnabled = qosEnabled;
        }

        public EventTapFlowVerifier verifyEvents(Event... events)
        {
            Collection<MockEventTapFlow> eventTapFlows = getEventTapFlows().get(key);

            assertNotNull(eventTapFlows, context());
            assertEquals(eventTapFlows.size(), 1, context());

            MockEventTapFlow eventTapFlow = eventTapFlows.iterator().next();
            assertEquals(eventTapFlow.getTaps(), taps, context());
            assertEqualsNoOrder(eventTapFlow.getEvents().toArray(), ImmutableList.copyOf(events).toArray(), context());
            return this;
        }

        public EventTapFlowVerifier verifyNoFlow()
        {
            assertTrue(getEventTapFlows().get(key).isEmpty(), context());
            return this;
        }

        private Multimap<List<String>, MockEventTapFlow> getEventTapFlows()
        {
            if (qosEnabled) {
                return qosEventTapFlows;
            }
            else {
                return nonQosEventTapFlows;
            }
        }

        private String context()
        {
            return format("eventType=%s flowId=%s qos=%s uris=%s", eventType, flowId, qosEnabled ? "true" : "false", taps);
        }
    }
}
