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

package org.teiid.translator;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;

import javax.resource.cci.Connection;
import javax.xml.namespace.QName;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service;

/**
 * Simple {@link Connection} interface for web services
 */
public interface WSConnection extends Connection {

	<T> Dispatch<T> createDispatch(String binding, String endpoint, Class<T> type, Service.Mode mode);
	
	<T> Dispatch<T> createDispatch(Class<T> type, Service.Mode mode) throws IOException;
	
	URL getWsdl();
	
	QName getServiceQName();
	
	QName getPortQName();

	public static class Util {
		
		public static String appendQueryString(String endpoint, String param) {
			return endpoint + (endpoint.indexOf('?') >= 0?'&':'?') + param;
		}
		
	    public static String httpURLEncode(String s) {
	        try {
	            return URLEncoder.encode(s, "UTF-8").replaceAll("\\+", "%20"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
	        } catch (UnsupportedEncodingException e) {
	        	throw new RuntimeException(e);
	        }
	    }

		public static void closeSource(final Source source) {
			if (!(source instanceof StreamSource)) {
				return;
			}
			
			StreamSource stream = (StreamSource)source;
			try {
				if (stream.getInputStream() != null) {
					stream.getInputStream().close();
				}
			} catch (IOException e) {
			}
			try {
				if (stream.getReader() != null) {
					stream.getReader().close();
				}
			} catch (IOException e) {
			}
		}
		
	}
	
}
