/*
 * (c) 2008 Varsity Gateway LLC. All rights reserved.
 */
package com.metamatrix.uddi.publish;

import java.net.MalformedURLException;
import java.security.InvalidParameterException;
import java.util.Vector;

import javax.xml.soap.SOAPException;

import org.uddi4j.UDDIException;
import org.uddi4j.client.UDDIProxy;
import org.uddi4j.datatype.service.BusinessService;
import org.uddi4j.response.AuthToken;
import org.uddi4j.transport.TransportException;

import com.metamatrix.uddi.util.MMUddiUtil;

/**
 * This class uses UDDI4j's API to publish WSDL.
 */
public class PublishWSDL {

	public PublishWSDL() {
	}

	/**
	 * Publish WSDL via UDDI4j java API.
	 * 
	 * @param user The user name for the UDDI Registry.
	 * @param password The password for the UDDI Registry
	 * @param businessKey The business key of the <@link BusinessEntity> for which to publish the WSDL
	 * @param url The url of the WSDL to publish
	 * @param inquiryUrl The inquiry url of the UDDI Registry
	 * @param publishUrl The publish url of the UDDI Registry
	 * @throws InvalidParameterException If the value is invalid.
	 * @throws UDDIException If the UDDI call fails with error.
	 * @throws SOAPException If the UDDI call fails with error.
	 * @since 5.6
	 */
	public void publish( String user,
	                     String password,
	                     String businessKey,
	                     String url,
	                     String inquiryUrl,
	                     String publishUrl )
	    throws UDDIException, InvalidParameterException, SOAPException, TransportException, MalformedURLException {

		UDDIProxy proxy = MMUddiUtil.getUddiProxy(inquiryUrl, publishUrl);

		// Pass in userid and password registered at the UDDI site
		AuthToken token = proxy.get_authToken(user, password);

		// Create a new business service using BindingTemplates default constructor.
		// DefaultName is the service name. ServiceKey must be "" to save a new service
		BusinessService businessService = new BusinessService("");  //$NON-NLS-1$
		businessService.setDefaultNameString(url, null);
		businessService.setBusinessKey(businessKey);
		Vector services = new Vector();
		services.addElement(businessService);

		// Save a Business Service
		proxy.save_service(token.getAuthInfoString(), services);	
	}
}
