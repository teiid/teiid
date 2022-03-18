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

import org.junit.Test;
import org.mockito.Mockito;

import javax.servlet.http.HttpServletRequest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

@SuppressWarnings("nls")
public class TestProxyHttpServletRequest {

    @Test public void testProxyUrl() {
        HttpServletRequest request = Mockito.mock(HttpServletRequest.class);
        assertSame(request, ProxyHttpServletRequest.handleProxiedRequest(request));

        Mockito.when(request.getHeader("X-Forwarded-Host")).thenReturn("host:8080");
        Mockito.when(request.getHeader("X-Forwarded-Port")).thenReturn("8080");
        assertEquals("http://host:8080", ProxyHttpServletRequest.getProxyUrl(request));

        Mockito.when(request.getHeader("X-Forwarded-Host")).thenReturn("host");
        Mockito.when(request.getHeader("X-Forwarded-Port")).thenReturn("443");
        Mockito.when(request.getHeader("X-Forwarded-Proto")).thenReturn("https");
        assertEquals("https://host:443", ProxyHttpServletRequest.getProxyUrl(request));

        Mockito.when(request.getHeader("X-Forwarded-Host")).thenReturn(null);
        Mockito.when(request.getHeader("Forwarded")).thenReturn("Forwarded for=192.168.42.1;host=hostname:80;proto=http;proto-version=");
        assertEquals("http://hostname:80", ProxyHttpServletRequest.getProxyUrl(request));
    }

}
