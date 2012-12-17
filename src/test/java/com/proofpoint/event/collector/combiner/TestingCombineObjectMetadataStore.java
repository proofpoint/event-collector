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
package com.proofpoint.event.collector.combiner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.proofpoint.event.collector.EventPartition;

import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TestingCombineObjectMetadataStore
        implements CombineObjectMetadataStore
{
    private final ConcurrentMap<Key, CombinedGroup> metadata = Maps.newConcurrentMap();

    @Override
    public CombinedGroup getCombinedGroupManifest(EventPartition eventPartition, String sizeName)
    {
        Preconditions.checkNotNull(eventPartition, "eventPartition is null");
        Preconditions.checkNotNull(sizeName, "sizeName is null");

        return metadata.get(new Key(eventPartition, sizeName));
    }

    @Override
    public boolean replaceCombinedGroupManifest(EventPartition eventPartition, String sizeName, CombinedGroup currentGroup, CombinedGroup newGroup)
    {
        checkNotNull(currentGroup, "currentGroup is null");
        checkNotNull(newGroup, "newGroup is null");
        checkArgument(currentGroup.getLocationPrefix().equals(newGroup.getLocationPrefix()), "currentGroup location is different from newGroup location");

        Key key = new Key(eventPartition, sizeName);
        CombinedGroup persistentGroup = metadata.get(key);
        if (persistentGroup != null) {
            if (persistentGroup.getVersion() != currentGroup.getVersion()) {
                return false;
            }
        }
        else if (currentGroup.getVersion() != 0) {
            return false;
        }

        if (currentGroup.getVersion() == 0) {
            return metadata.putIfAbsent(key, newGroup) == null;
        }
        else {
            return metadata.replace(key, currentGroup, newGroup);
        }
    }

    private static class Key {
        private final EventPartition eventPartition;
        private final String sizeName;

        private Key(EventPartition eventPartition, String sizeName)
        {
            this.eventPartition = eventPartition;
            this.sizeName = sizeName;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Key key = (Key) o;

            if (!eventPartition.equals(key.eventPartition)) {
                return false;
            }
            if (!sizeName.equals(key.sizeName)) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = eventPartition.hashCode();
            result = 31 * result + sizeName.hashCode();
            return result;
        }
    }
}
