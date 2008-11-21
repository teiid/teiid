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
import org.uddi4j.datatype.Name;
import org.uddi4j.response.AuthToken;
import org.uddi4j.response.DispositionReport;
import org.uddi4j.response.ServiceInfo;
import org.uddi4j.response.ServiceList;
import org.uddi4j.transport.TransportException;
import org.uddi4j.util.FindQualifier;
import org.uddi4j.util.FindQualifiers;

import com.metamatrix.uddi.UddiPlugin;
import com.metamatrix.uddi.exception.MMUddiException;
import com.metamatrix.uddi.util.MMUddiUtil;

/**
 * Use UDDI4j's API to call unpublish WSDL. *
 */
public class UnPublishWSDL {

	/**
	 * Creates and fills the Unpublish structure.
	 * 
	 * @param user Username for UDDI Registry
	 * @param password Password for UDDI Registry
	 * @param businessKey Key for business entity
	 * @param inquiryUrl Inquiry URL for UDDI Registry
	 * @param publishUrl Publish URL for UDDI Registry
	 * @throws InvalidParameterException If the value is invalid.
	 * @since 5.6
	 */
	public void unpublish( String user,
	                       String password,
	                       String businessKey,
	                       String wsdlUrl,
	                       String inquiryUrl,
	                       String publishUrl )
	    throws MMUddiException, UDDIException, InvalidParameterException, SOAPException, TransportException, MalformedURLException {

		UDDIProxy proxy = MMUddiUtil.getUddiProxy(inquiryUrl, publishUrl);

		// Pass in userid and password registered at the UDDI site
		AuthToken token = proxy.get_authToken(user, password);

		// creating vector of Name Object
		Vector names = new Vector();
		names.add(new Name(wsdlUrl));

		// Setting FindQualifiers to 'caseSensitiveMatch'
		FindQualifiers findQualifiers = new FindQualifiers();
		Vector qualifier = new Vector();
		qualifier.add(new FindQualifier(FindQualifier.exactNameMatch));
		findQualifiers.setFindQualifierVector(qualifier);

		// **** Get the first instance of this service for the given business key
		ServiceList serviceList = proxy.find_service(businessKey, names, null, null, findQualifiers, 1);

		// Process the returned ServiceList object
		Vector serviceInfoVector = serviceList.getServiceInfos().getServiceInfoVector();
		ServiceInfo serviceInfo = new ServiceInfo();
		for (int i = 0; i < serviceInfoVector.size(); i++) {
			serviceInfo = (ServiceInfo)serviceInfoVector.elementAt(i);
		}

		// Try to delete saved Business Service. Delete will fail for services not created by this id
		// **** Having service key, delete using the authToken
		DispositionReport dr = proxy.delete_service(token.getAuthInfoString(), serviceInfo.getServiceKey());

		if (!dr.success()) {
			Object[] parms = new Object[] {dr.getOperator(), dr.getGeneric()};
			String msg = UddiPlugin.Util.getString("UnPublishWSDL.UnpublishWsdl.ErrorDeletingService", parms); //$NON-NLS-1$
			throw new MMUddiException(msg);			
		}
	}
}
