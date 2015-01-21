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

import com.proofpoint.reporting.Key;
import com.proofpoint.stats.SparseCounterStat;
import com.proofpoint.stats.SparseTimeStat;

public interface S3UploaderStats
{
    SparseCounterStat processedFiles(@Key("eventType") String eventType, @Key("status") FileProcessedStatus status);

    SparseTimeStat processedTime(@Key("eventType") String eventType);

    SparseCounterStat uploadAttempts(@Key("eventType") String eventType, @Key("status") FileUploadStatus status);

    public enum FileProcessedStatus
    {
        UPLOADED,             // file successfully uploaded to S3
        CORRUPT;              // error reading or verifying file before upload, will not be uploaded

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }

    public enum FileUploadStatus
    {
        SUCCESS,              // upload attempt succeeded
        FAILURE;              // upload attempt failed, file will be retried for upload

        @Override
        public String toString()
        {
            return name().toLowerCase();
        }
    }
}