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

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;

public interface BatchProcessor<T>
{
    @PostConstruct
    void start();

    @PreDestroy
    void stop();

    void put(T entry);

    public static interface BatchHandler<T>
    {
        void processBatch(List<T> entries);
    }

    public static interface Observer
    {
        void onRecordsLost(int count);

        void onRecordsReceived(int count);
    }
}
