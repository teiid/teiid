/*
 * (c) 2008 Varsity Gateway LLC. All rights reserved.
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
