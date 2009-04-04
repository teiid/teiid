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

package com.metamatrix.uddi.publish;

import java.net.MalformedURLException;
import java.util.Properties;
import java.util.Vector;

import org.uddi4j.UDDIException;
import org.uddi4j.client.UDDIProxy;
import org.uddi4j.datatype.Name;
import org.uddi4j.datatype.business.BusinessEntity;
import org.uddi4j.response.AuthToken;
import org.uddi4j.response.BusinessInfo;
import org.uddi4j.response.BusinessList;
import org.uddi4j.response.DispositionReport;
import org.uddi4j.response.Result;
import org.uddi4j.transport.TransportFactory;
import org.uddi4j.util.FindQualifier;
import org.uddi4j.util.FindQualifiers;

/**
 * Attempts to save a businessEntity. This class is strictly for building test data.
 * <OL>
 * <LI>Sets up a UDDIProxy object
 * <LI>Requests an authorization token
 * <LI>Saves a businessEntity
 * <LI>Lists businesses starting with the the first letter of the business's name. The new business should be in the list.
 * </OL>
 * 
 * @since 5.6
 */
public class SaveBusiness {

	Properties config = null;

	public void addBusiness( String user,
	                         String password,
	                         String businessName,
	                         String publishUrl,
	                         String inquiryUrl ) {
		Properties props = new Properties();
		props.put(TransportFactory.PROPERTY_NAME, "org.uddi4j.transport.ApacheAxisTransport");  //$NON-NLS-1$
		// Construct a UDDIProxy object
		UDDIProxy proxy = null;
		try {
			proxy = new UDDIProxy(props);
		} catch (MalformedURLException err) {

		}

		try {

			// Select the desired UDDI server node
			proxy.setInquiryURL(inquiryUrl);
			proxy.setPublishURL(publishUrl);

			// Pass in userid and password registered at the UDDI site
			AuthToken token = proxy.get_authToken(user, password);

			// Create minimum required data objects
			Vector entities = new Vector();

			// Create a new business entity using required elements constructor
			// Name is the business name. BusinessKey must be "" to save a new
			// business
			BusinessEntity be = new BusinessEntity("", businessName); //$NON-NLS-1$
			be.setDefaultDescriptionString("This company rocks!!"); //$NON-NLS-1$
			entities.addElement(be);

			// Save business
			proxy.save_business(token.getAuthInfo().getText(), entities);

			// Find all businesses that start with that particular letter e.g. 'S' for 'Sample Business'.
			String businessNameLeadingSubstring = (businessName.substring(0, 1));

			// creating vector of Name Object
			Vector names = new Vector();
			names.add(new Name(businessNameLeadingSubstring));

			// Setting FindQualifiers to 'caseSensitiveMatch'
			FindQualifiers findQualifiers = new FindQualifiers();
			Vector qualifier = new Vector();
			qualifier.add(new FindQualifier("caseSensitiveMatch")); //$NON-NLS-1$
			findQualifiers.setFindQualifierVector(qualifier);

			// Find businesses by name
			// And setting the maximum rows to be returned as 0 or all.
			BusinessList businessList = proxy.find_business(names, null, null, null, null, findQualifiers, 0);

			Vector businessInfoVector = businessList.getBusinessInfos().getBusinessInfoVector();
			for (int i = 0; i < businessInfoVector.size(); i++) {
				BusinessInfo businessInfo = (BusinessInfo)businessInfoVector.elementAt(i);
				System.out.println(businessInfo.getDefaultNameString());
			}
		}
		// Handle possible errors
		catch (UDDIException e) {
			DispositionReport dr = e.getDispositionReport();
			if (dr != null) {
				System.out.println("UDDIException faultCode:" + e.getFaultCode() + "\n operator:" + dr.getOperator() //$NON-NLS-1$ //$NON-NLS-2$
				                   + "\n generic:" + dr.getGeneric()); //$NON-NLS-1$

				Vector results = dr.getResultVector();
				for (int i = 0; i < results.size(); i++) {
					Result r = (Result)results.elementAt(i);
					System.out.println("\n errno:" + r.getErrno()); //$NON-NLS-1$
					if (r.getErrInfo() != null) {
						System.out.println("\n errCode:" + r.getErrInfo().getErrCode() + "\n errInfoText:" //$NON-NLS-1$ //$NON-NLS-2$
						                   + r.getErrInfo().getText());
					}
				}
			}

			e.printStackTrace();
		}
		// Catch any other exception that may occur
		catch (Exception e) {
			e.printStackTrace();
		}
	}
}
