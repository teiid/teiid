/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.olingo.web;

import static org.junit.Assert.*;

import javax.servlet.http.HttpServletRequest;

import org.junit.Test;
import org.mockito.Mockito;

@SuppressWarnings("nls")
public class TestProxyHttpServletRequest {

    @Test public void testProxyUrl() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        assertSame(request, ProxyHttpServletRequest.handleProxiedRequest(request));

        Mockito.stub(request.getHeader("X-Forwarded-Host")).toReturn("host:8080");
        Mockito.stub(request.getHeader("X-Forwarded-Port")).toReturn("8080");
        assertEquals("http://host:8080", ProxyHttpServletRequest.getProxyUrl(request));

        Mockito.stub(request.getHeader("X-Forwarded-Host")).toReturn("host");
        Mockito.stub(request.getHeader("X-Forwarded-Port")).toReturn("443");
        Mockito.stub(request.getHeader("X-Forwarded-Proto")).toReturn("https");
        assertEquals("https://host:443", ProxyHttpServletRequest.getProxyUrl(request));

        Mockito.stub(request.getHeader("X-Forwarded-Host")).toReturn(null);
        Mockito.stub(request.getHeader("Forwarded")).toReturn("Forwarded for=192.168.42.1;host=hostname:80;proto=http;proto-version=");
        assertEquals("http://hostname:80", ProxyHttpServletRequest.getProxyUrl(request));
    }

}
