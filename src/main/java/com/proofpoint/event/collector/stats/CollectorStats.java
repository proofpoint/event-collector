/*
 * Copyright 2011 Proofpoint, Inc.
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
package com.proofpoint.event.collector.stats;

import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class CollectorStats
{
    private final CounterStat uploadFailure = new CounterStat();
    private final CounterStat uploadSuccess = new CounterStat();
    private final CounterStat fileError = new CounterStat();

    public void uploadFailed()
    {
        uploadFailure.update(1);
    }

    @Managed
    @Nested
    public CounterStat getUploadFailureStats()
    {
        return uploadFailure;
    }

    public void uploadSucceeded()
    {
        uploadSuccess.update(1);
    }

    @Managed
    @Nested
    public CounterStat getUploadSuccessStats()
    {
        return uploadSuccess;
    }

    public void fileError()
    {
        fileError.update(1);
    }

    @Managed
    @Nested
    public CounterStat getFileErrorStats()
    {
        return fileError;
    }
}
