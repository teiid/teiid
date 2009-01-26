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
import java.security.InvalidParameterException;
import java.util.Vector;
import javax.xml.soap.SOAPException;
import org.uddi4j.UDDIException;
import org.uddi4j.client.UDDIProxy;
import org.uddi4j.datatype.Name;
import org.uddi4j.response.BusinessList;
import org.uddi4j.transport.TransportException;
import org.uddi4j.util.FindQualifier;
import org.uddi4j.util.FindQualifiers;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.uddi.util.MMUddiUtil;

/**
 * Uses UDDI4j's API to UDDI v2 call find_business.
 */
public class FindBusiness {

	// The number of values that will be listed in the results of find calls
	public static int max_rows = 0;

	public static String host = StringUtil.Constants.EMPTY_STRING;
	public static String port = StringUtil.Constants.EMPTY_STRING;

	public FindBusiness() {
	}

	/**
	 * Find all businesses in a UDDI Registry via UDDI4J's API.
	 * 
	 * @param inquiryUrl inquiry URL of the UDDI Registry
	 * @param publishUrl publish URL of the UDDI Registry
	 * @param searchString Business name search string.
	 * @param maxRows Maximum rows to be returned.
	 * @return List List of matching <@link BusinessEntity>s
	 * @throws Exception If the UDDI call fails with error.
	 * @since 5.6
	 */
	public BusinessList findBusiness( String inquiryUrl,
	                                  String publishUrl,
	                                  String searchString,
	                                  int maxRows )
	    throws UDDIException, InvalidParameterException, SOAPException, TransportException, MalformedURLException {

		UDDIProxy proxy = MMUddiUtil.getUddiProxy(inquiryUrl, publishUrl);

		BusinessList businessList = null;

		// creating vector of Name Object
		Vector names = new Vector();
		names.add(new Name(searchString));

		// Setting FindQualifiers to 'caseSensitiveMatch'
		FindQualifiers findQualifiers = new FindQualifiers();
		Vector qualifier = new Vector();
		qualifier.add(new FindQualifier(FindQualifier.caseSensitiveMatch));
		findQualifiers.setFindQualifierVector(qualifier);

		// Find businesses by name
		businessList = proxy.find_business(names, null, null, null, null, findQualifiers, maxRows);

		return businessList;
	}
}
