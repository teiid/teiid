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
package com.metamatrix.uddi.util;

import java.net.MalformedURLException;
import java.util.Properties;
import org.uddi4j.client.UDDIProxy;
import org.uddi4j.transport.TransportFactory;

/**
 * This class provides utility methods to the UDDI
 */
public class MMUddiUtil {

	static final String TRANSPORT = "org.uddi4j.transport.ApacheAxisTransport"; //$NON-NLS-1$

	/**
	 * Creates an instance of UDDIProxy for a given inquiry/publish URL.
	 * 
	 * @param inquiryUrl
	 * @param publishUrl
	 * @return
	 * @throws MalformedURLException
	 */
	public static UDDIProxy getUddiProxy( final String inquiryUrl,
	                                      final String publishUrl ) throws MalformedURLException {
		Properties props = new Properties();
		props.put(TransportFactory.PROPERTY_NAME, TRANSPORT);
		// Construct a UDDIProxy object
		UDDIProxy proxy = null;
		proxy = new UDDIProxy(props);
		// Select the desired UDDI server node
		proxy.setInquiryURL(inquiryUrl);
		proxy.setPublishURL(publishUrl);
		return proxy;
	}
}
