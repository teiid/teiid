/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
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
