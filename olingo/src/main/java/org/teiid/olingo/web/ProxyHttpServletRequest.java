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

import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

public class ProxyHttpServletRequest extends HttpServletRequestWrapper {
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
            encodedURI = URI.create(buf);
        } catch (Exception e) {
            throw new RuntimeException("Unable to create URI: " + buf, e); //$NON-NLS-1$
        }
        
        try {
            encodedURL = new URL(encodedURI.toString());
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }

    private static String extractURI(URL url, String baseURI) throws UnsupportedEncodingException {
        StringBuffer buffer = new StringBuffer();

        buffer.append(baseURI);
        if (url.getPath() != null) {
            buffer.append(URLEncoder.encode(url.getPath(), "UTF-8")); //$NON-NLS-1$
        }
        if (url.getQuery() != null) {
            buffer.append("?").append(url.getQuery()); //$NON-NLS-1$
        }
        if (url.getRef() != null) {
            buffer.append("#").append(URLEncoder.encode(url.getRef(), "UTF-8")); //$NON-NLS-1$ //$NON-NLS-2$
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
