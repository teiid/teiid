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

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

public class ProxyHttpServletRequest extends HttpServletRequestWrapper {

    public static HttpServletRequest handleProxiedRequest(
            HttpServletRequest httpRequest) {
        String proxyUrl = getProxyUrl(httpRequest);
        if (proxyUrl != null) {
            httpRequest = new ProxyHttpServletRequest(httpRequest, proxyUrl);
        }
        return httpRequest;
    }

    /**
     *
     * @param httpRequest
     * @return the url or null if not proxied
     */
    static String getProxyUrl(HttpServletRequest httpRequest) {
        /*
        Forwarded for=192.168.42.1;host=hostname:80;proto=http;proto-version=
        X-Forwarded-Proto http
        X-Forwarded-Host hostname
        X-Forwarded-Port 80
        */
        String host = httpRequest.getHeader("X-Forwarded-Host"); //$NON-NLS-1$
        String protocol = httpRequest.getHeader("X-Forwarded-Proto"); //$NON-NLS-1$
        String port = httpRequest.getHeader("X-Forwarded-Port"); //$NON-NLS-1$
        if (host == null) {
            String forwarded = httpRequest.getHeader("Forwarded"); //$NON-NLS-1$
            if (forwarded != null) {
                String[] parts = forwarded.split(";"); //$NON-NLS-1$
                for (String part : parts) {
                    String[] keyValue = part.split("=", 2); //$NON-NLS-1$
                    if (keyValue.length != 2) {
                        continue;
                    }
                    if (keyValue[0].equals("host")) { //$NON-NLS-1$
                        host = keyValue[1];
                        port = null;
                    } else if (keyValue[0].equals("proto")) { //$NON-NLS-1$
                        protocol = keyValue[1];
                    }
                }
            }
        }
        String proxyUrl = null;
        if (host != null) {
            StringBuffer buf = new StringBuffer();
            buf.append(protocol==null?"http":protocol); //$NON-NLS-1$
            buf.append("://").append(host); //$NON-NLS-1$
            if (port != null) {
                String portSuffix = ":" + port; //$NON-NLS-1$
                if (!host.endsWith(portSuffix)) {
                    buf.append(portSuffix);
                }
            }
            proxyUrl = buf.toString();
        }
        return proxyUrl;
    }

    private URI encodedURI;
    private URL encodedURL;

    public ProxyHttpServletRequest(HttpServletRequest delegate, String proxyBaseURI) {
        super(delegate);

        URL url = null;
        try {
            url = new URL(delegate.getRequestURL().toString());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

        String buf = url.toExternalForm();
        try {
            buf = extractURI(url, proxyBaseURI);
            encodedURI = (new URL(buf)).toURI();
        } catch (Exception e) {
            throw new RuntimeException("Unable to create URI: " + buf, e); //$NON-NLS-1$
        }

        try {
            encodedURL = new URL(encodedURI.toString());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private static String extractURI(URL url, String baseURI) {
        StringBuffer buffer = new StringBuffer();

        buffer.append(baseURI);
        if (url.getPath() != null) {
            buffer.append(url.getPath());
        }
        if (url.getQuery() != null) {
            buffer.append("?").append(url.getQuery()); //$NON-NLS-1$
        }
        if (url.getRef() != null) {
            buffer.append("#").append(url.getRef()); //$NON-NLS-1$
        }
        String buf = buffer.toString();
        return buf;
    }


    @Override
    public String getRequestURI() {
        return encodedURI.getRawPath();
    }

    @Override
    public StringBuffer getRequestURL() {
        return new StringBuffer(encodedURL.toString());
    }
}
