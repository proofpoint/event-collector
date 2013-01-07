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

import com.google.common.collect.ImmutableList;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.Request;
import com.proofpoint.http.client.RequestStats;
import com.proofpoint.http.client.Response;
import com.proofpoint.http.client.ResponseHandler;

import javax.ws.rs.core.Response.Status;
import java.util.LinkedList;
import java.util.List;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MockHttpClient implements HttpClient
{
    private Exception exception;
    private int exceptionCount;
    private Response response = createOkResponse();
    private List<Request> requests = new LinkedList<Request>();

    @Override
    public RequestStats getStats()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler)
            throws E
    {
        requests.add(request);
        if (exception != null) {
            Exception thisException = exception;
            if (exceptionCount > 0) {
                exceptionCount -= 1;
                if (exceptionCount == 0) {
                    exception = null;
                }
            }

            E newException = responseHandler.handleException(request, thisException);
            if (newException != null) {
                throw newException;
            }
            return null;
        }

        return responseHandler.handle(request, response);
    }

    public List<Request> getRequests()
    {
        return ImmutableList.copyOf(requests);
    }

    public void clearRequests()
    {
        requests.clear();
    }

    public void respondWithException()
    {
        respondWithException(new Exception(), 0);
    }

    public void respondWithException(Exception exception, int exceptionCount)
    {
        this.exception = exception;
        this.exceptionCount = exceptionCount;
    }

    public void respondWithOk()
    {
        respondWith(createOkResponse());
    }

    public void respondWithServerError()
    {
        respondWith(createServerErrorResponse());
    }

    public void respondWithClientError()
    {
        respondWith(createClientErrorResponse());
    }

    public void respondWith(Response response)
    {
        this.exception = null;
        this.response = response;
    }

    private static Response createOkResponse()
    {
        Response response = mock(Response.class);
        when(response.getStatusCode()).thenReturn(200);
        when(response.getStatusMessage()).thenReturn("OK");
        return response;
    }

    private static Response createClientErrorResponse()
    {
        Response response = mock(Response.class);
        when(response.getStatusCode()).thenReturn(BAD_REQUEST.getStatusCode());
        when(response.getStatusMessage()).thenReturn(BAD_REQUEST.getReasonPhrase());
        return response;
    }

    private static Response createServerErrorResponse()
    {
        Response response = mock(Response.class);
        when(response.getStatusCode()).thenReturn(SERVICE_UNAVAILABLE.getStatusCode());
        when(response.getStatusMessage()).thenReturn(SERVICE_UNAVAILABLE.getReasonPhrase());
        return response;
    }
}
