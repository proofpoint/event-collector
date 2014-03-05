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

import com.google.inject.Inject;
import com.proofpoint.http.server.HttpServer;
import com.proofpoint.log.Logger;

import java.util.concurrent.atomic.AtomicBoolean;

public class HttpServerController
{
    private static final Logger log = Logger.get(HttpServerController.class);
    private final HttpServer httpServer;
    private final AtomicBoolean stopped = new AtomicBoolean(false);

    @Inject
    public HttpServerController(HttpServer httpServer)
    {
        this.httpServer = httpServer;
    }

    public void stop()
    {
        if (!stopped.getAndSet(true)) {
            log.info("Disabling external interfaces.");
            try {
                httpServer.stop();
            }
            catch (Exception ex) {
                log.error(ex, "Error stopping HttpServer");
            }
        }
    }
}
