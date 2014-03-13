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

import com.proofpoint.event.collector.BatchProcessor.BatchHandler;

import java.net.URI;
import java.util.Set;

public interface EventTapFlow extends BatchHandler<Event>
{
    Observer NULL_OBSERVER = new Observer()
    {
        @Override
        public void onRecordsDelivered(int count)
        {
        }

        @Override
        public void onRecordsLost(int count)
        {
        }

        @Override
        public void onRecordsRejected(URI uri, int count)
        {

        }
    };

    Set<URI> getTaps();

    void setTaps(Set<URI> taps);

    interface Observer
    {
        void onRecordsDelivered(int count);

        void onRecordsLost(int count);

        void onRecordsRejected(URI uri, int count);
    }
}
