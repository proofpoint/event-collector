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
package com.proofpoint.event.collector;

import org.testng.annotations.Test;

import static com.proofpoint.event.collector.QosDelivery.BEST_EFFORT;
import static com.proofpoint.event.collector.QosDelivery.RETRY;
import static org.testng.Assert.assertEquals;

public class TestQosDelivery
{
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "No enum constant com.proofpoint.event.collector.QosDelivery.DUMMY")
    public void testFailureFromStringIllegalValue()
    {
        QosDelivery.fromString("dummy");
    }

    @Test
    public void testSuccessFromStringRetry()
    {
        assertEquals(QosDelivery.fromString("retry"), RETRY);
    }

    @Test
    public void tesSuccessFromStringBestEffort()
    {
        assertEquals(QosDelivery.fromString("bestEffort"), BEST_EFFORT);
    }

    @Test
    public void testSuccessToStringRetry()
    {
        assertEquals(RETRY.toString(), "retry");
    }

    @Test
    public void testSuccessToStringBestEffort()
    {
        assertEquals(BEST_EFFORT.toString(), "bestEffort");
    }

    @Test
    public void testFromStringNull()
    {
        assertEquals(BEST_EFFORT, QosDelivery.fromString(null));
    }
}
