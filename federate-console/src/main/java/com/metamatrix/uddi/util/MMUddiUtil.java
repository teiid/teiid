/*
 *
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
