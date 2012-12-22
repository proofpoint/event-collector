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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.inject.assistedinject.Assisted;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.discovery.client.ServiceState;
import com.proofpoint.discovery.client.testing.StaticServiceSelector;
import com.proofpoint.event.collector.BatchProcessor.BatchHandler;
import com.proofpoint.event.collector.EventCounters.CounterState;
import com.proofpoint.event.collector.EventTapFlow.Observer;
import com.proofpoint.log.Logger;
import org.joda.time.DateTime;
import org.logicalshift.concurrent.SerialScheduledExecutorService;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.concat;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertEqualsNoOrder;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

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
    private static final ServiceDescriptor tapB1a = tapB1;
    private static final ServiceDescriptor tapB1b = createServiceDescriptor(typeB, flowId1, instanceB);
    private static final ServiceDescriptor tapB2 = createServiceDescriptor(typeB, flowId2, instanceA);
    private static final ServiceDescriptor tapB2a = tapB2;
    private static final ServiceDescriptor tapB2b = createServiceDescriptor(typeB, flowId2, instanceB);
    private static final ServiceDescriptor tapC = createServiceDescriptor(typeC, flowId1, instanceA);
    private static final ServiceDescriptor tapC1 = tapC;
    private static final ServiceDescriptor tapC1a = tapC1;
    private static final ServiceDescriptor tapC1b = createServiceDescriptor(typeC, flowId1, instanceB);
    private static final ServiceDescriptor tapC2 = createServiceDescriptor(typeC, flowId2, instanceA);
    private static final ServiceDescriptor tapC2a = tapC2;
    private static final ServiceDescriptor tapC2b = createServiceDescriptor(typeC, flowId2, instanceB);
    private static final ServiceDescriptor qtapA = createQosServiceDescriptor(typeA, flowId1, instanceA);
    private static final ServiceDescriptor qtapA1 = tapA;
    private static final ServiceDescriptor qtapA1a = tapA1;
    private static final ServiceDescriptor qtapA1b = createQosServiceDescriptor(typeA, flowId1, instanceB);
    private static final ServiceDescriptor qtapA2 = createQosServiceDescriptor(typeA, flowId2, instanceA);
    private static final ServiceDescriptor qtapA2a = tapA2;
    private static final ServiceDescriptor qtapA2b = createQosServiceDescriptor(typeA, flowId2, instanceB);
    private static final ServiceDescriptor qtapA3 = createQosServiceDescriptor(typeA, flowId3, instanceA);
    private static final ServiceDescriptor qtapB = createQosServiceDescriptor(typeB, flowId1, instanceA);
    private static final ServiceDescriptor qtapB1 = tapB;
    private static final ServiceDescriptor qtapB1a = tapB1;
    private static final ServiceDescriptor qtapB1b = createQosServiceDescriptor(typeB, flowId1, instanceB);
    private static final ServiceDescriptor qtapB2 = createQosServiceDescriptor(typeB, flowId2, instanceA);
    private static final ServiceDescriptor qtapB3 = createQosServiceDescriptor(typeB, flowId3, instanceA);
    private static final ServiceDescriptor qtapB2a = tapB2;
    private static final ServiceDescriptor qtapB2b = createQosServiceDescriptor(typeB, flowId2, instanceB);
    private static final ServiceDescriptor qtapC = createQosServiceDescriptor(typeC, flowId1, instanceA);
    private static final ServiceDescriptor qtapC1 = tapC;
    private static final ServiceDescriptor qtapC1a = tapC1;
    private static final ServiceDescriptor qtapC1b = createQosServiceDescriptor(typeC, flowId1, instanceB);
    private static final ServiceDescriptor qtapC2 = createQosServiceDescriptor(typeC, flowId2, instanceA);
    private static final ServiceDescriptor qtapC2a = tapC2;
    private static final ServiceDescriptor qtapC2b = createQosServiceDescriptor(typeC, flowId2, instanceB);

    private ServiceSelector serviceSelector;
    private SerialScheduledExecutorService executorService;
    private BatchProcessorFactory batchProcessorFactory = new MockBatchProcessorFactory();
    private Multimap<String, MockBatchProcessor<Event>> batchProcessors;
    private EventTapFlowFactory eventTapFlowFactory = new MockEventTapFlowFactory();
    private Multimap<List<String>, EventTapFlow> eventTapFlows;
    private EventTapConfig eventTapConfig;
    private EventTapWriter eventTapWriter;

    @BeforeMethod
    public void setup()
    {
        serviceSelector = new StaticServiceSelector(ImmutableSet.<ServiceDescriptor>of());
        executorService = new SerialScheduledExecutorService();
        batchProcessors = HashMultimap.create();
        eventTapFlows = HashMultimap.create();
        eventTapConfig = new EventTapConfig();
        serviceSelector = mock(ServiceSelector.class);
        eventTapWriter = new EventTapWriter(
                serviceSelector, executorService,
                batchProcessorFactory, eventTapFlowFactory,
                eventTapConfig);
        eventTapWriter.start();
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "selector is null")
    public void testConstructorNullSelector()
    {
        new EventTapWriter(null, executorService, batchProcessorFactory, eventTapFlowFactory, new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "executorService is null")
    public void testConstructorNullExecutorService()
    {
        new EventTapWriter(serviceSelector, null, batchProcessorFactory, eventTapFlowFactory, new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "batchProcessorFactory is null")
    public void testConstructorNullBatchProcessorFactory()
    {
        new EventTapWriter(serviceSelector, executorService, null, eventTapFlowFactory, new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "eventTapFlowFactory is null")
    public void testConstructorNullEventTapFlowFactory()
    {
        new EventTapWriter(serviceSelector, executorService, batchProcessorFactory, null, new EventTapConfig());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "config is null")
    public void testConstructorNullConfig()
    {
        new EventTapWriter(serviceSelector, executorService, batchProcessorFactory, eventTapFlowFactory, null);
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
        checkTapEvents(tapA, eventsA[0]);
        checkTapEvents(tapB);

        updateThenRefreshFlowsThenCheck(tapA, tapB);
        writeEvents(eventsA[1], eventsB[1]);
        checkTapEvents(tapA, eventsA[0], eventsA[1]);
        checkTapEvents(tapB, eventsB[1]);

        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[2], eventsB[2]);
        checkTapEvents(tapA, eventsA[0], eventsA[1], eventsA[2]);
        checkTapEvents(tapB, eventsB[1]);

        updateThenRefreshFlowsThenCheck();
        writeEvents(eventsA[3], eventsB[3]);
        checkTapEvents(tapA, eventsA[0], eventsA[1], eventsA[2]);
        checkTapEvents(tapB, eventsB[1]);
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
        checkTapEvents(tapA, eventsA[0]);
        checkTapEvents(tapB, eventsB[0]);

        updateThenRefreshFlowsThenCheck();
        writeEvents(eventsA[1], eventsB[1]);
        checkTapEvents(tapA, eventsA[0]);
        checkTapEvents(tapB, eventsB[0]);
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
        checkTapEvents(tapA1, eventsA[0]);
        checkTapEvents(tapA2, eventsA[0]);

        updateThenRefreshFlowsThenCheck();
        writeEvents(eventsA[1], eventsB[1]);
        checkTapEvents(tapA1, eventsA[0]);
        checkTapEvents(tapA2, eventsA[0]);
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
        checkTapEvents(tapA, eventsA[0]);
        checkTapEvents(tapB);
        checkTapEvents(tapC);

        updateThenRefreshFlowsThenCheck(tapA, tapB, tapC);
        writeEvents(eventsA[1], eventsB[1], eventsC[1]);
        checkTapEvents(tapA, eventsA[0], eventsA[1]);
        checkTapEvents(tapB, eventsB[1]);
        checkTapEvents(tapC, eventsC[1]);

        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[2], eventsB[2], eventsC[2]);
        checkTapEvents(tapA, eventsA[0], eventsA[1], eventsA[2]);
        checkTapEvents(tapB, eventsB[1]);
        checkTapEvents(tapC, eventsC[1]);
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
        checkTapEvents(tapA, eventsA[0]);
        checkTapEvents(tapB1);
        checkTapEvents(tapB2);

        updateThenRefreshFlowsThenCheck(tapA, tapB1, tapB2);
        writeEvents(eventsA[1], eventsB[1]);
        checkTapEvents(tapA, eventsA[0], eventsA[1]);
        checkTapEvents(tapB1, eventsB[1]);
        checkTapEvents(tapB2, eventsB[1]);

        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[2], eventsB[2]);
        checkTapEvents(tapA, eventsA[0], eventsA[1], eventsA[2]);
        checkTapEvents(tapB1, eventsB[1]);
        checkTapEvents(tapB2, eventsB[1]);
    }

    @Test
    public void testRefreshFlowsCreateNonQosNonQosQos()
    {
        // [] -> tapA1, tapA2, qtapA3 -> []
        testRefreshFlowsCreateThreeSameTaps(tapA1, tapA2, qtapA3);
    }

    @Test
    public void testRefreshFlowsCreateNonQosTwoSameNonQos()
    {
        // [] -> tapA1, tapA2a, tapA2b -> []
        testRefreshFlowsCreateThreeSameTaps(tapA1, tapA2a, tapA2b);
    }

    @Test
    public void testRefreshFlowsCreateNonQosWithPromotedQos()
    {
        // [] -> tapA1, tapA2a, qtapA2b -> []
        testRefreshFlowsCreateThreeSameTaps(tapA1, tapA2a, qtapA2b);

    }

    @Test
    public void testRefreshFlowsCreateNonQosWithPromotedQosSecond()
    {
        // [] -> tapA1, qtapA2a, tapA2b -> []
        testRefreshFlowsCreateThreeSameTaps(tapA1, qtapA2a, tapA2b);
    }

    public void testRefreshFlowsCreateThreeSameTaps(ServiceDescriptor tapA1, ServiceDescriptor tapA2, ServiceDescriptor tapA3)
    {
        updateThenRefreshFlowsThenCheck(tapA1, tapA2, tapA3);
        writeEvents(eventsA[0]);
        checkTapEvents(tapA1, eventsA[0]);
        checkTapEvents(tapA2, eventsA[0]);
        checkTapEvents(tapA3, eventsA[0]);

        updateThenRefreshFlowsThenCheck();
        writeEvents(eventsA[1]);
        checkTapEvents(tapA1, eventsA[0]);
        checkTapEvents(tapA2, eventsA[0]);
        checkTapEvents(tapA3, eventsA[0]);
    }

    @Test
    public void testRefreshFlowsCreateNonQosNonQosQosWithExistingTap()
    {
        // [] -> tapA1, tapA2, qtapA3 -> []
        testRefreshFlowsCreateThreeSameTapsWithExistingTap(tapA, tapB1, tapB2, qtapB3);
    }

    @Test
    public void testRefreshFlowsCreateNonQosTwoSameNonQosWithExistingTap()
    {
        // [] -> tapA1, tapA2a, tapA2b -> []
        testRefreshFlowsCreateThreeSameTapsWithExistingTap(tapA, tapB1, tapB2a, tapB2b);
    }

    @Test
    public void testRefreshFlowsCreateNonQosWithPromotedQosWithExistingTap()
    {
        // [] -> tapA1, tapA2a, qtapA2b -> []
        testRefreshFlowsCreateThreeSameTapsWithExistingTap(tapA, tapB1, tapB2a, qtapB2b);

    }

    @Test
    public void testRefreshFlowsCreateNonQosWithPromotedQosSecondWithExistingTap()
    {
        // [] -> tapA1, qtapA2a, tapA2b -> []
        testRefreshFlowsCreateThreeSameTapsWithExistingTap(tapA, tapB1, qtapB2a, tapB2b);
    }

    @Test
    public void testRefreshFlowsCreateNonQosNonQosQosWithExistingQosTap()
    {
        // [] -> tapA1, tapA2, qtapA3 -> []
        testRefreshFlowsCreateThreeSameTapsWithExistingTap(qtapA, tapB1, tapB2, qtapB3);
    }

    @Test
    public void testRefreshFlowsCreateNonQosTwoSameNonQosWithExistingQosTap()
    {
        // [] -> tapA1, tapA2a, tapA2b -> []
        testRefreshFlowsCreateThreeSameTapsWithExistingTap(qtapA, tapB1, tapB2a, tapB2b);
    }

    @Test
    public void testRefreshFlowsCreateNonQosWithPromotedQosWithExistingQosTap()
    {
        // [] -> tapA1, tapA2a, qtapA2b -> []
        testRefreshFlowsCreateThreeSameTapsWithExistingTap(qtapA, tapB1, tapB2a, qtapB2b);

    }

    @Test
    public void testRefreshFlowsCreateNonQosWithPromotedQosSecondWithExistingQosTap()
    {
        // [] -> tapA1, qtapA2a, tapA2b -> []
        testRefreshFlowsCreateThreeSameTapsWithExistingTap(qtapA, tapB1, qtapB2a, tapB2b);
    }

    private void testRefreshFlowsCreateThreeSameTapsWithExistingTap(ServiceDescriptor tapA, ServiceDescriptor tapB1, ServiceDescriptor tapB2, ServiceDescriptor tapB3)
    {
        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[0], eventsB[0]);
        checkTapEvents(tapA, eventsA[0]);
        checkTapEvents(tapB1);
        checkTapEvents(tapB2);
        checkTapEvents(tapB3);

        updateThenRefreshFlowsThenCheck(tapA, tapB1, tapB2, tapB3);
        writeEvents(eventsA[1], eventsB[1]);
        checkTapEvents(tapA, eventsA[0], eventsA[1]);
        checkTapEvents(tapB1, eventsB[1]);
        checkTapEvents(tapB2, eventsB[1]);
        checkTapEvents(tapB3, eventsB[1]);

        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[2], eventsB[2]);
        checkTapEvents(tapA, eventsA[0], eventsA[1], eventsA[2]);
        checkTapEvents(tapB1, eventsB[1]);
        checkTapEvents(tapB2, eventsB[1]);
        checkTapEvents(tapB3, eventsB[1]);

    }


    @Test
    public void testRefreshFlowsCreateQosWithPromotedQosWithExistingTap()
    {
        // tapA -> tapA, tapB1, tapB2a, qtapB2b -> tapA
        testRefreshFlowsCreateQosWithPromotedQosWithExistingTap(tapA, tapB1, tapB2a, qtapB2b);
    }

    @Test
    public void testRefreshFlowsCreateQosWithPromotedQosWithExistingQosTap()
    {
        // qtapA -> qtapA, tapB1, tapB2a, qtapB2b -> qtapA
        testRefreshFlowsCreateQosWithPromotedQosWithExistingTap(qtapA, tapB1, tapB2a, qtapB2b);
    }

    private void testRefreshFlowsCreateQosWithPromotedQosWithExistingTap(ServiceDescriptor tapA, ServiceDescriptor tapB1, ServiceDescriptor tapB2a, ServiceDescriptor tapB2b)
    {
        // [q]tapA1 -> [q]tapA1, [q]tapB2a, [q]tapB2b -> [q]tapA1
        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[0], eventsB[0]);
        checkTapEvents(tapA, eventsA[0]);
        checkTapEvents(tapB1);
        checkTapEvents(tapB2a);
        checkTapEvents(tapB2b);

        updateThenRefreshFlowsThenCheck(tapA, tapB1, tapB2a, tapB2b);
        writeEvents(eventsA[1], eventsB[1]);
        checkTapEvents(tapA, eventsA[0], eventsA[1]);
        checkTapEvents(tapB1, eventsB[1]);
        checkTapEvents(tapB2a, eventsB[1]);
        checkTapEvents(tapB2b, eventsB[1]);

        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[2], eventsB[2]);
        checkTapEvents(tapA, eventsA[0], eventsA[1], eventsA[2]);
        checkTapEvents(tapB1, eventsB[1]);
        checkTapEvents(tapB2a, eventsB[1]);
        checkTapEvents(tapB2b, eventsB[1]);
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

    @Test void testRefreshFlowsQosToSameNonQos()
    {
        // qtapA1 -> tapA2
        testRefreshFlowsSwapSameTaps(qtapA1, tapA2);
    }

    private void testRefreshFlowsSwapSameTaps(ServiceDescriptor tapA1, ServiceDescriptor tapA2)
    {
        updateThenRefreshFlowsThenCheck(tapA1);
        writeEvents(eventsA[0]);
        checkTapEvents(tapA1, eventsA[0]);
        checkTapEvents(tapA2);

        updateThenRefreshFlowsThenCheck(tapA2);
        writeEvents(eventsA[1]);
        checkTapEvents(tapA1, eventsA[0]);
        checkTapEvents(tapA2, eventsA[1]);
    }

    @Test
    public void testRefreshFlowsNonQosToIdenticalQos()
    {
        // tapA1 -> qtapA1
        testRefreshFlowsSwapIdenticalTaps(tapA1, qtapA1);
    }

    @Test
    public void testRefreshFlowsQosToIdenticalQos()
    {
        // tapA1a -> tapA1b
        testRefreshFlowsSwapIdenticalTaps(tapA1a, tapA1b);
    }

    private void testRefreshFlowsSwapIdenticalTaps(ServiceDescriptor tapA1, ServiceDescriptor tapA2)
    {
        updateThenRefreshFlowsThenCheck(tapA1);
        writeEvents(eventsA[0]);
        checkTapEvents(tapA1, eventsA[0]);
        checkTapEvents(tapA2, eventsA[0]);  // The taps share the same flow ID

        updateThenRefreshFlowsThenCheck(tapA2);
        writeEvents(eventsA[1]);
        checkTapEvents(tapA1, eventsA[0], eventsA[1]);  // The taps share the same flow ID
        checkTapEvents(tapA2, eventsA[0], eventsA[1]);
    }

    @Test
    public void testRefreshFlowsRemovesOldEntries()
    {
        updateThenRefreshFlowsThenCheck(tapA, tapB);
        writeEvents(eventsA[0], eventsB[0]);
        checkTapEvents(tapA, eventsA[0]);
        checkTapEvents(tapB, eventsB[0]);

        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[1], eventsB[1]);
        checkTapEvents(tapA, eventsA[0], eventsA[1]);
        checkTapEvents(tapB, eventsB[0]);
    }

    @Test
    public void testRefreshFlowsUpdatesExistingProcessor()
    {
        // If the taps for a given event type changes, don't create the processor
        updateThenRefreshFlowsThenCheck(tapA1);
        writeEvents(eventsA[0]);
        checkTapEvents(tapA1, eventsA[0]);
        checkTapEvents(tapA2);

        updateThenRefreshFlowsThenCheck(tapA2);
        writeEvents(eventsA[1]);
        checkTapEvents(tapA1, eventsA[0]);
        checkTapEvents(tapA2, eventsA[1]);
    }

    @Test
    public void testRefreshFlowsIsCalledPeriodically()
    {
        updateTaps(tapA);
        executorService.elapseTime(
                (long) eventTapConfig.getEventTapRefreshDuration().toMillis() - 1,
                TimeUnit.MILLISECONDS);
        assertFalse(batchProcessors.containsKey(typeA));
        executorService.elapseTime(1, TimeUnit.MILLISECONDS);
        assertTrue(batchProcessors.containsKey(typeA));
        assertEquals(batchProcessors.get(typeA).size(), 1);

        // If the refreshFlows() is called after the period, tapB should be
        // created to handle the new tap after one period.
        updateTaps(tapB);
        executorService.elapseTime(
                (long) eventTapConfig.getEventTapRefreshDuration().toMillis() - 1,
                TimeUnit.MILLISECONDS);
        assertFalse(batchProcessors.containsKey(typeB));
        executorService.elapseTime(1, TimeUnit.MILLISECONDS);
        assertTrue(batchProcessors.containsKey(typeB));
        assertEquals(batchProcessors.get(typeB).size(), 1);

        // Same is true after the second period, but with tapC.
        updateTaps(tapC);
        executorService.elapseTime(
                (long) eventTapConfig.getEventTapRefreshDuration().toMillis() - 1,
                TimeUnit.MILLISECONDS);
        assertFalse(batchProcessors.containsKey(typeC));
        executorService.elapseTime(1, TimeUnit.MILLISECONDS);
        assertTrue(batchProcessors.containsKey(typeC));
        assertEquals(batchProcessors.get(typeC).size(), 1);
    }

    @Test
    public void testWritePartitionsByType()
    {
        updateThenRefreshFlowsThenCheck(tapA, tapB);
        writeEvents(eventsA[0], eventsB[0], eventsC[0]);
        checkTapEvents(tapA, eventsA[0]);
        checkTapEvents(tapB, eventsB[0]);
    }

    @Test
    public void testWriteSendsToNewFlows()
    {
        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[0], eventsB[0]);
        checkTapEvents(tapA, eventsA[0]);

        updateThenRefreshFlowsThenCheck(tapA, tapB);
        writeEvents(eventsA[1], eventsB[1]);
        checkTapEvents(tapA, eventsA[0], eventsA[1]);
        checkTapEvents(tapB, eventsB[1]);
    }

    @Test
    public void testWriteDoesntSendToOldFlows()
    {
        updateThenRefreshFlowsThenCheck(tapA, tapB);
        writeEvents(eventsA[0], eventsB[0]);
        checkTapEvents(tapA, eventsA[0]);
        checkTapEvents(tapB, eventsB[0]);

        updateThenRefreshFlowsThenCheck(tapA);
        writeEvents(eventsA[1], eventsB[1]);
        checkTapEvents(tapA, eventsA[0], eventsA[1]);
        checkTapEvents(tapB, eventsB[0]);
    }

    @Test
    public void testQueueCounters()
    {
        updateThenRefreshFlowsThenCheck(tapA);

        MockBatchProcessor<Event> processors = batchProcessors.get(typeA).iterator().next();
        writeEvents(eventsA[0]);
        checkCounters(eventTapWriter.getQueueCounters(), typeA, 1, 0);
        assertCountersOnlyExistWithTheseNames(eventTapWriter.getQueueCounters(), typeA);

        processors.succeed = false;
        writeEvents(eventsA[1]);
        checkCounters(eventTapWriter.getQueueCounters(), typeA, 2, 1);
        assertCountersOnlyExistWithTheseNames(eventTapWriter.getQueueCounters(), typeA);
    }

    private void updateThenRefreshFlowsThenCheck(ServiceDescriptor... taps)
    {
        updateTaps(taps);
        eventTapWriter.refreshFlows();
        checkActiveProcessors(taps);
    }

    private void updateTaps(ServiceDescriptor... taps)
    {
        when(serviceSelector.selectAllServices()).thenReturn(ImmutableList.copyOf(taps));
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
            assertTrue(batchProcessors.containsKey(processorName), format("no processor created for %s", processorName));

            List<MockBatchProcessor<Event>> processors = ImmutableList.copyOf(batchProcessors.get(processorName));
            assertEquals(processors.size(), 1, format("wrong number of processors for %s", processorName));

            // The batch processor should have been started, but not stopped
            // if it is still active.
            MockBatchProcessor<Event> processor = processors.get(0);
            assertEquals(processor.startCount, 1, format("invalid start count for processor %s", processorName));
            assertEquals(processor.stopCount, 0, format("invalid stop count for processor %s", processorName));
        }

        // For all non-active processors, make sure they have been stopped.
        for (Entry<String, Collection<MockBatchProcessor<Event>>> entry : batchProcessors.asMap().entrySet()) {
            String processorName = entry.getKey();
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
            assertEquals(processors.size(), 1, format("wrong number of processors for %s", processorName));

            // The batch processor should have been started and stopped.
            MockBatchProcessor<Event> processor = processors.get(0);
            assertEquals(processor.startCount, 1, format("invalid start count for processor %s", processorName));
            assertEquals(processor.stopCount, 1, format("invalid stop count for processor %s", processorName));
        }
    }

    private void checkTapEvents(ServiceDescriptor tap, Event... events)
    {
        String eventType = nullToEmpty(tap.getProperties().get("eventType"));
        String flowId = nullToEmpty(tap.getProperties().get("tapId"));
        Collection<EventTapFlow> eventTapFlows = this.eventTapFlows.get(ImmutableList.of(eventType, flowId));
        EventTapFlow eventTapFlow = null;
        @SuppressWarnings("deprecated")
        ArgumentCaptor<List<Event>> eventArgumentCaptor = new ArgumentCaptor<List<Event>>();

        if (eventTapFlows != null && !eventTapFlows.isEmpty()) {
            assertEquals(eventTapFlows.size(), 1);
            eventTapFlow = eventTapFlows.iterator().next();
        }

        if (events.length == 0) {
            if (eventTapFlow != null) {
                verify(eventTapFlow, never()).processBatch(anyListOf(Event.class));
            }
        }
        else {
            assertNotNull(eventTapFlow);
            verify(eventTapFlow, atLeast(1)).processBatch(eventArgumentCaptor.capture());
            List<Event> actualEvents = ImmutableList.copyOf(concat(eventArgumentCaptor.getAllValues()));
            assertEqualsNoOrder(actualEvents.toArray(), ImmutableList.copyOf(events).toArray());
        }
    }

    private void verifyNoTapEvents(ServiceDescriptor... taps)
    {
        for (ServiceDescriptor tap : taps) {
            checkTapEvents(tap);
        }
    }

    private static void checkCounters(Map<String, CounterState> counters, String type, int received, int lost)
    {
        CounterState counterState = counters.get(type);
        assertEquals(counterState.getReceived(), received);
        assertEquals(counterState.getLost(), lost);
    }

    private static void assertCountersOnlyExistWithTheseNames(Map<String, CounterState> counters, String... types)
    {
        assertEquals(Sets.difference(counters.keySet(), ImmutableSet.copyOf(types)), ImmutableSet.of());
    }

    private static String extractProcessorName(ServiceDescriptor tap)
    {
        if (nullToEmpty(tap.getProperties().get("qos.delivery")).equalsIgnoreCase("retry")) {
            return format("%s{%s}", tap.getProperties().get("eventType"),
                    tap.getProperties().get("tapId"));
        }
        else {
            return tap.getProperties().get("eventType");
        }
    }

    private static ServiceDescriptor createServiceDescriptor(String eventType, Map<String, String> properties)
    {
        String nodeId = randomUUID().toString();
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        builder.putAll(properties);
        builder.put("eventType", eventType);
        if (!properties.containsKey("tapId")) {
            builder.put("tapId", "1");
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
                ImmutableMap.of("tapId", flowId, "http", format("http://%s-%s.event.tap", eventType, instanceId)));
    }

    private static ServiceDescriptor createQosServiceDescriptor(String eventType, String flowId, String instanceId)
    {
        return createServiceDescriptor(eventType,
                ImmutableMap.of("qos.delivery", "retry",
                        "tapId", flowId,
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

    private class MockBatchProcessor<T> implements BatchProcessor<T>
    {
        public int startCount = 0;
        public int stopCount = 0;
        public boolean succeed = true;
        public List<T> entries = new LinkedList<T>();
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
                observer.onRecordsLost(1);
            }
            observer.onRecordsReceived(1);
        }
    }

    private class MockEventTapFlowFactory implements EventTapFlowFactory
    {
        @Override
        public EventTapFlow createEventTapFlow(String eventType, String flowId, Set<URI> taps, Observer observer)
        {
            List<String> key = ImmutableList.of(eventType, flowId);
            EventTapFlow eventTapFlow = mock(EventTapFlow.class);
            eventTapFlows.put(key, eventTapFlow);
            return eventTapFlow;
        }

        @Override
        public EventTapFlow createEventTapFlow(@Assisted("eventType") String eventType, @Assisted("flowId") String flowId, Set<URI> taps)
        {
            return createEventTapFlow(eventType, flowId, taps, EventTapFlow.NULL_OBSERVER);
        }
    }
}
