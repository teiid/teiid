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
package com.metamatrix.uddi.util;

import java.net.MalformedURLException;
import java.security.InvalidParameterException;

import javax.xml.soap.SOAPException;

import org.uddi4j.UDDIException;
import org.uddi4j.response.BusinessList;
import org.uddi4j.transport.TransportException;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.uddi.UddiPlugin;
import com.metamatrix.uddi.exception.MMUddiException;
import com.metamatrix.uddi.publish.PublishWSDL;
import com.metamatrix.uddi.publish.UnPublishWSDL;
import com.metamatrix.uddi.query.FindBusiness;
import com.metamatrix.uddi.query.GetWSDL;

/**
 * Concrete implementation of UddiHelper.
 * 
 * @since 5.6
 */
public class MMUddiHelper implements UddiHelper {

	/**
	 * {@inheritDoc}
	 * 
	 * @see com.metamatrix.uddi.util.UddiHelper#publish(java.lang.String, java.lang.String, java.lang.String, java.lang.String,
	 *      java.lang.String, java.lang.String)
	 * @since 5.6
	 */
	public void publish( String uddiUserName,
	                     String uddiPassword,
	                     String busKey,
	                     String wsdlUrl,
	                     String inquiryUrl,
	                     String publishUrl ) throws MMUddiException {
		PublishWSDL pubWSDL = new PublishWSDL();
		try {
			pubWSDL.publish(uddiUserName, uddiPassword, busKey, wsdlUrl, inquiryUrl, publishUrl);
		} catch (TransportException err) {
			processException(err, "PublishWSDL.TransportException_in_WSDL_publish"); //$NON-NLS-1$
		} catch (MalformedURLException err) {
			processException(err, "MMUddiHelper.MalformedURLException_in_WSDL_publish"); //$NON-NLS-1$
		} catch (UDDIException err) {
			processException(err, "MMUddiHelper.UDDIException_in_WSDL_publish"); //$NON-NLS-1$			
		} catch (InvalidParameterException err) {
			processException(err, "MMUddiHelper.InvalidParameterException_in_WSDL_publish"); //$NON-NLS-1$
		} catch (SOAPException err) {
			processException(err, "MMUddiHelper.SOAPException_in_WSDL_publish"); //$NON-NLS-1$
		}

	}

	/**
	 * @param err
	 * @throws MMUddiException
	 */
	private void processException( final Exception err,
	                               final String msgKey ) throws MMUddiException {
		Object[] params = new Object[] {err};
		String msg = UddiPlugin.Util.getString(msgKey, params);
		LogManager.logError(this.getClass().getName(), msg);
		throw new MMUddiException(err, msg);
	}

	/**
	 * @see com.metamatrix.uddi.util.UddiHelper#unPublish(java.lang.String, java.lang.String, java.lang.String, java.lang.String,
	 *      java.lang.String)
	 * @since 5.6
	 */
	public void unPublish( String UddiUserName,
	                       String UddiPassword,
	                       String busKey,
	                       String wsdlUrl,
	                       String inquiryUrl,
	                       String publishUrl ) throws MMUddiException {
		UnPublishWSDL pubWSDL = new UnPublishWSDL();
		try {
			pubWSDL.unpublish(UddiUserName, UddiPassword, busKey, wsdlUrl, inquiryUrl, publishUrl);
		} catch (TransportException err) {
			processException(err, "MMUddiHelper.TransportException_in_WSDL_unpublish"); //$NON-NLS-1$
		} catch (MalformedURLException err) {
			processException(err, "MMUddiHelper.MalformedURLException_in_WSDL_unpublish"); //$NON-NLS-1$
		} catch (UDDIException err) {
			processException(err, "MMUddiHelper.UDDIException_in_WSDL_unpublish"); //$NON-NLS-1$			
		} catch (InvalidParameterException err) {
			processException(err, "MMUddiHelper.InvalidParameterException_in_WSDL_unpublish"); //$NON-NLS-1$
		} catch (SOAPException err) {
			processException(err, "MMUddiHelper.SOAPException_in_WSDL_publish"); //$NON-NLS-1$
		}

	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see com.metamatrix.uddi.util.UddiHelper#getAllBusinesses(java.lang.String, java.lang.String, int)
	 * @since 5.6
	 */
	public BusinessList getAllBusinesses( String inquiryUrl,
	                                      String publishUrl,
	                                      int maxRows ) throws MMUddiException {
		FindBusiness findBusiness = new FindBusiness();
		BusinessList list = null;
		try {
			list = findBusiness.findBusiness(inquiryUrl, publishUrl, "%", maxRows); //$NON-NLS-1$
		} catch (TransportException err) {
			processException(err, "MMUddiHelper.TransportException_finding_businesses"); //$NON-NLS-1$
		} catch (MalformedURLException err) {
			processException(err, "MMUddiHelper.MalformedURLException_all_businesses"); //$NON-NLS-1$
		} catch (UDDIException err) {
			processException(err, "MMUddiHelper.UDDIException_finding_all_businesses"); //$NON-NLS-1$			
		} catch (InvalidParameterException err) {
			processException(err, "MMUddiHelper.InvalidParameterException_finding_all_businesses"); //$NON-NLS-1$
		} catch (SOAPException err) {
			processException(err, "MMUddiHelper.SOAPException_finding_all_businesses"); //$NON-NLS-1$
		}

		return list;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @see com.metamatrix.uddi.util.UddiHelper#getBusinessByName(java.lang.String, java.lang.String, int)
	 * @since 5.6
	 */
	public BusinessList getBusinessByName( String inquiryUrl,
	                                       String publishUrl,
	                                       String searchString,
	                                       int maxRows ) throws MMUddiException {
		FindBusiness findBusiness = new FindBusiness();
		BusinessList list = null;
		try {
			list = findBusiness.findBusiness(inquiryUrl, publishUrl, searchString, maxRows);
		} catch (TransportException err) {
			processException(err, "MMUddiHelper.TransportException_finding_businesses_by_name"); //$NON-NLS-1$
		} catch (MalformedURLException err) {
			processException(err, "MMUddiHelper.MalformedURLException_finding_businesses_by_name"); //$NON-NLS-1$
		} catch (UDDIException err) {
			processException(err, "MMUddiHelper.UDDIException_finding_businesses_by_name"); //$NON-NLS-1$			
		} catch (InvalidParameterException err) {
			processException(err, "MMUddiHelper.InvalidParameterException_finding_businesses_by_name"); //$NON-NLS-1$
		} catch (SOAPException err) {
			processException(err, "MMUddiHelper.SOAPException_finding_businesses_by_name"); //$NON-NLS-1$
		}

		return list;
	}

	/**
	 * @see com.metamatrix.uddi.util.UddiHelper#isPublished(java.lang.String, java.lang.String, java.lang.String,
	 *      java.lang.String, java.lang.String, java.lang.String)
	 * @since 5.6
	 */
	public boolean isPublished( String UddiUserName,
	                            String UddiPassword,
	                            String busKey,
	                            String wsdlUrl,
	                            String inquiryUrl,
	                            String publishUrl ) throws MMUddiException {
		GetWSDL wsdl = new GetWSDL();
		boolean published = false;
		try {
			published = wsdl.isPublished(UddiUserName, UddiPassword, busKey, wsdlUrl, inquiryUrl, publishUrl);
		} catch (MalformedURLException err) {
			processException(err, "MMUddiHelper.MalformedURLException_isPublished"); //$NON-NLS-1$
		} catch (TransportException err) {
			processException(err, "MMUddiHelper.TransportException_isPublished"); //$NON-NLS-1$			
		} catch (UDDIException err) {
			processException(err, "MMUddiHelper.UDDIException_isPublished"); //$NON-NLS-1$
		}

		return published;
	}

}
