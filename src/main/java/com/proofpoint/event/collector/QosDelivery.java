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

import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.UPPER_UNDERSCORE;

public enum QosDelivery
{
    RETRY,
    BEST_EFFORT;   // default behavior

    public static QosDelivery fromString(String rawValue)
    {
        return Enum.valueOf(QosDelivery.class, LOWER_CAMEL.to(UPPER_UNDERSCORE, rawValue));
    }

    @Override
    public String toString()
    {
        return UPPER_UNDERSCORE.to(LOWER_CAMEL, name());
    }
}
