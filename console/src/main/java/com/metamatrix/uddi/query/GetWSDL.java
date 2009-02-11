/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package com.metamatrix.uddi.query;

import java.net.MalformedURLException;
import java.util.Vector;
import org.uddi4j.UDDIException;
import org.uddi4j.client.UDDIProxy;
import org.uddi4j.datatype.Name;
import org.uddi4j.response.ServiceList;
import org.uddi4j.transport.TransportException;
import org.uddi4j.util.FindQualifier;
import org.uddi4j.util.FindQualifiers;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.uddi.exception.MMUddiException;
import com.metamatrix.uddi.util.MMUddiUtil;

/**
 * Uses UDDI4j's UDDI v2 API to call get_wsdlServiceInfo WSDL. Used to determine if a given WSDL is published or not.
 */
public class GetWSDL {

	public String uddiUrl = StringUtil.Constants.EMPTY_STRING;

	/**
	 * @param user
	 * @param password
	 * @param businessKey
	 * @param url
	 * @param inquiryUrl
	 * @param publishUrl
	 * @return
	 * @throws SOAPException
	 * @since 5.6
	 */
	public boolean isPublished( String user,
	                            String password,
	                            String businessKey,
	                            String url,
	                            String inquiryUrl,
	                            String publishUrl )
	    throws TransportException, MMUddiException, UDDIException, MalformedURLException {

		UDDIProxy proxy = MMUddiUtil.getUddiProxy(inquiryUrl, publishUrl);
		boolean isPublished = false;

		// creating vector of Name Object
		Vector names = new Vector();
		names.add(new Name(url));

		// Setting FindQualifiers to 'exactNameMatch'
		FindQualifiers findQualifiers = new FindQualifiers();
		Vector qualifier = new Vector();
		qualifier.add(new FindQualifier(FindQualifier.exactNameMatch));
		findQualifiers.setFindQualifierVector(qualifier);

		// **** Find the Business Service saved.
		// And setting the maximum rows to be returned as 1.
		ServiceList serviceList = proxy.find_service(businessKey, names, null, null, findQualifiers, 1);

		// Process the returned ServiceList object
		Vector serviceInfoVector = serviceList.getServiceInfos().getServiceInfoVector();
		if (serviceInfoVector.size() > 0) {
			isPublished = true;			
		}
		return isPublished;
	}
}
