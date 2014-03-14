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
package org.teiid.odata;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import javax.servlet.http.HttpServletRequest;

import org.jboss.resteasy.util.Encode;
import org.jboss.resteasy.util.HttpServletRequestDelegate;

public class ProxyHttpServletRequest extends HttpServletRequestDelegate {
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
		
		String buf = extractURI(url, proxyBaseURI);
		try {
			encodedURI = URI.create(buf);
		} catch (Exception e) {
			throw new RuntimeException("Unable to create URI: " + buf, e);
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
			buffer.append(Encode.encodePath(url.getPath()));
		}
		if (url.getQuery() != null) {
			buffer.append("?").append(url.getQuery());
		}
		if (url.getRef() != null) {
			buffer.append("#").append(Encode.encodeFragment(url.getRef()));
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
