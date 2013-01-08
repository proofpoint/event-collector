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

import java.util.List;

public interface BatchProcessor<T>
{
    Observer NULL_OBSERVER = new Observer()
    {
        @Override
        public void onRecordsLost(int count)
        {
        }

        @Override
        public void onRecordsReceived(int count)
        {
        }
    };

    void start();

    void stop();

    void put(T entry);

    interface BatchHandler<T>
    {
        void processBatch(List<T> entries);

        void notifyEntriesDropped(int count);
    }

    interface Observer
    {
        void onRecordsLost(int count);

        void onRecordsReceived(int count);
    }
}
