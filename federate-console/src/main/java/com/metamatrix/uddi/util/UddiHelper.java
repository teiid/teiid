/*
 * (c) 2008 Varsity Gateway LLC. All rights reserved.
 */
package com.metamatrix.uddi.util;

import org.uddi4j.response.BusinessList;
import com.metamatrix.uddi.exception.MMUddiException;

/**
 * Interface to UDDI4j's API's for UDDI Registry.
 */
public interface UddiHelper {

	/**
	 * Publish WSDL to the UDDI Registry.
	 * 
	 * @param UddiUserName The username used to logon to the UDDI registry
	 * @param UddiPassword The password used to logon to the UDDI registry
	 * @param businessKey The key for the bsusiness entity this WSDL is associated with
	 * @param wsdlUrl The URL of the WSDL to publish
	 * @param inquiryUrl The inquiry url of the UDDI registry
	 * @param publishUrl The publish url of the UDDI registry
	 * @since 5.6
	 */
	public void publish( String UddiUserName,
	                     String UddiPassword,
	                     String businessKey,
	                     String wsdlUrl,
	                     String inquiryUrl,
	                     String publishUrl ) throws MMUddiException;

	/**
	 * Un-Publish WSDL from the UDDI Registry.
	 * 
	 * @param UddiUserName The username used to logon to the UDDI registry
	 * @param UddiPassword The password used to logon to the UDDI registry
	 * @param businessKey The key for the bsusiness entity this WSDL is associated with
	 * @param wsdlUrl The url for the WSDL to be un-published
	 * @param inquiryUrl The inquiry url of the UDDI registry
	 * @param publishUrl The publish url of the UDDI registry
	 * @since 5.6
	 */
	public void unPublish( String UddiUserName,
	                       String UddiPassword,
	                       String businessKey,
	                       String wsdlUrl,
	                       String inquiryUrl,
	                       String publishUrl ) throws MMUddiException;

	/**
	 * Get all businesses from the UDDI Registry.
	 * 
	 * @param inquiryUrl The inquiry url of the UDDI registry
	 * @param publishUrl The publish url of the UDDI registry
	 * @param maxRows Maximum rows to be returned.
	 * @return List List of matching <@link BusinessEntity>s
	 * @throws Exception If the UDDI call fails with error.
	 * @since 5.6
	 */
	public BusinessList getAllBusinesses( String inquiryUrl,
	                                      String publishUrl,
	                                      int maxrows ) throws MMUddiException;

	/**
	 * Get business by name from the UDDI Registry.
	 * 
	 * @param inquiryUrl The inquiry url of the UDDI registry
	 * @param publishUrl The publish url of the UDDI registry
	 * @param searchString Business name search string.
	 * @param maxRows Maximum rows to be returned.
	 * @return List List of matching <@link BusinessEntity>s
	 * @throws Exception If the UDDI call fails with error.
	 * @since 5.6
	 */
	public BusinessList getBusinessByName( String inquiryUrl,
	                                       String publishUrl,
	                                       String searchString,
	                                       int maxRows ) throws MMUddiException;

	/**
	 * Determine if passed WSDL is published.
	 * 
	 * @param user The user name for the UDDI Registry.
	 * @param password The password for the UDDI Registry
	 * @param wsdlUrl The url for the WSDL to search for
	 * @param inquiryUrl Inquiry URL of the UDDI Registry
	 * @param publishUrl The publish url of the UDDI registry
	 * @return boolean true (published) or false (not published)
	 * @throws Exception If the UDDI call fails with error.
	 * @since 5.6
	 */
	public boolean isPublished( String UddiUserName,
	                            String UddiPassword,
	                            String busKey,
	                            String wsdlUrl,
	                            String inquiryUrl,
	                            String publishUrl ) throws MMUddiException;

}
