/*
 * (c) 2008 Varsity Gateway LLC. All rights reserved.
 */
package com.metamatrix.uddi.util;

import java.security.InvalidParameterException;
import java.util.Vector;
import junit.framework.TestCase;
import org.uddi4j.UDDIException;
import org.uddi4j.client.UDDIProxy;
import org.uddi4j.response.AuthToken;
import org.uddi4j.response.BusinessInfo;
import org.uddi4j.response.BusinessInfos;
import org.uddi4j.response.BusinessList;
import org.uddi4j.response.DispositionReport;
import org.uddi4j.response.Result;
import com.metamatrix.uddi.exception.MMUddiException;
import com.metamatrix.uddi.publish.SaveBusiness;

/**
 * Concrete example of UddiHelper.
 * 
 * @since 5.5.3
 */
public class TestMMUddiHelper extends TestCase {
	private final static String user = "juddi"; //$NON-NLS-1$

	private final static String password = "password"; //$NON-NLS-1$

	private final static String businessName = "MetaMatrix"; //$NON-NLS-1$

	private final static String url = "http://slntdb07.mm.atl2.redhat.com:8080/juddi/SqlQueryWebService.wsdl"; //$NON-NLS-1$

	private final static String uddiHost = "slntdb07.mm.atl2.redhat.com"; //$NON-NLS-1$

	private final static String uddiPort = "8080"; //$NON-NLS-1$

	private final static String scheme = "http://"; //$NON-NLS-1$

	private final static String colon = ":"; //$NON-NLS-1$

	private final static String inquiryUrl = scheme + uddiHost + colon
			+ uddiPort + "/juddi/inquiry"; //$NON-NLS-1$

	private final static String publishUrl = scheme + uddiHost + colon
			+ uddiPort + "/juddi/publish"; //$NON-NLS-1$

	private MMUddiHelper helper = new MMUddiHelper();

	/*
	 * @see TestCase#setUp()
	 */
	public void setUp() throws Exception {
		// cleanRegistry();
	}

	/*
	 * @see TestCase#tearDown()
	 */
	public void tearDown() throws Exception {
		super.tearDown();
	}

	/*
	 * Delete all BusinessEntities from the test UDDI registry
	 */
	public void testCleanRegistry() throws Exception {

		System.out.println("Cleaning registry..."); //$NON-NLS-1$

		UDDIProxy proxy = MMUddiUtil.getUddiProxy(inquiryUrl, publishUrl);

		try {

			// Pass in userid and password registered at the UDDI site
			AuthToken token = proxy.get_authToken(user, password);

			// Find businesses by Business name
			// And setting the maximum rows to be returned as 5.
			BusinessList businessList = helper.getAllBusinesses(inquiryUrl,
					publishUrl, 100);

			Vector businessInfoVector = businessList.getBusinessInfos()
					.getBusinessInfoVector();

			// Try to delete any businesses with this name. Multiple businesses
			// with the same
			// name may have been created by different userids. Delete will fail
			// for businesses
			// not created by this id
			for (int i = 0; i < businessInfoVector.size(); i++) {
				BusinessInfo bi = (BusinessInfo) businessInfoVector
						.elementAt(i);
				System.out.println("Found business key:" + bi.getBusinessKey()); //$NON-NLS-1$

				// Have found the matching business key, delete using the
				// authToken
				DispositionReport dr = proxy.delete_business(token
						.getAuthInfoString(), bi.getBusinessKey());

				if (dr.success()) {
					System.out.println("Business successfully deleted"); //$NON-NLS-1$
				} else {
					System.out
							.println(" Error during deletion of Business\n" + "\n operator:" + dr.getOperator() + "\n generic:" //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
									+ dr.getGeneric());

					Vector results = dr.getResultVector();
					for (int j = 0; j < results.size(); j++) {
						Result r = (Result) results.elementAt(j);
						System.out.println("\n errno:" + r.getErrno()); //$NON-NLS-1$
						if (r.getErrInfo() != null) {
							System.out
									.println("\n errCode:" + r.getErrInfo().getErrCode() + "\n errInfoText:" //$NON-NLS-1$ //$NON-NLS-2$
											+ r.getErrInfo().getText());
						}
					}
				}
			}
		}
		// Handle possible errors
		catch (UDDIException e) {
			DispositionReport dr = e.getDispositionReport();
			if (dr != null) {
				System.out
						.println("UDDIException faultCode:" + e.getFaultCode() + "\n operator:" + dr.getOperator() //$NON-NLS-1$ //$NON-NLS-2$
								+ "\n generic:" + dr.getGeneric()); //$NON-NLS-1$

				Vector results = dr.getResultVector();
				for (int i = 0; i < results.size(); i++) {
					Result r = (Result) results.elementAt(i);
					System.out.println("\n errno:" + r.getErrno()); //$NON-NLS-1$
					if (r.getErrInfo() != null) {
						System.out
								.println("\n errCode:" + r.getErrInfo().getErrCode() + "\n errInfoText:" //$NON-NLS-1$ //$NON-NLS-2$
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

		System.out.println("Finished cleaning registry!"); //$NON-NLS-1$
	}

	/**
	 * Test the addBusiness() method of MMUddiHelper
	 * 
	 * @since 5.6
	 */
	public void testAddBusiness() {

		try {
			addBusiness(user, password, businessName, inquiryUrl, publishUrl);
		} catch (MMUddiException err) {
			err.printStackTrace();
		}
	}

	/**
	 * Test the publish() method of MMUddiHelper
	 * 
	 * @since 5.6
	 */
	public void testPublish() {

		try {

			helper.publish(user, password, getBusinessKeyHelper(), url,
					inquiryUrl, publishUrl);

		} catch (MMUddiException err) {
			err.printStackTrace();
			fail(err.getMessage());
		}
	}

	/**
	 * @see com.metamatrix.uddi.util.UddiHelper#getAllBusinesses(java.lang.String,
	 *      java.lang.String, java.lang.String, java.lang.String)
	 * @since 5.6
	 */
	public void testIsPublished() {
		boolean published = false;

		try {
			published = helper.isPublished(user, password,
					getBusinessKeyHelper(), url, inquiryUrl, publishUrl);
		} catch (MMUddiException err) {
			err.printStackTrace();
			fail(err.getMessage());
		}

		assertEquals(true, published);
	}

	/**
	 * @return
	 * @throws MMUddiException
	 */
	private String getBusinessKeyHelper() throws MMUddiException {
		String businessKey = ""; //$NON-NLS-1$
		BusinessList businessList = helper.getBusinessByName(inquiryUrl,
				publishUrl, businessName, 1);

		BusinessInfos businessInfos = businessList.getBusinessInfos();
		// Take first business key we find
		for (int i = 0; i < businessInfos.size(); i++) {
			BusinessInfo entity = businessInfos.get(i);
			businessKey = entity.getBusinessKey();
			break;
		}
		return businessKey;
	}

	/**
	 * @see com.metamatrix.uddi.util.UddiHelper#getAllBusinesses(java.lang.String,
	 *      java.lang.String, java.lang.String, java.lang.String)
	 * @since 5.6
	 */
	public void testGetAllBusinesses() {
		BusinessList businessList = new BusinessList();
		try {
			businessList = helper.getAllBusinesses(inquiryUrl, publishUrl, 10);
		} catch (MMUddiException err) {
			err.printStackTrace();
			fail(err.getMessage());
		}
		BusinessInfos businessInfos = businessList.getBusinessInfos();
		for (int i = 0; i < businessInfos.size(); i++) {
			BusinessInfo entity = businessInfos.get(i);
			System.out.println(entity.getDefaultNameString());
		}
	}

	/**
	 * @see com.metamatrix.uddi.util.UddiHelper#getBusinessByName(java.lang.String,
	 *      java.lang.String, java.lang.String, java.lang.String,
	 *      java.lang.String)
	 * @since 5.6
	 */
	public void testGetBusinessByName() {

		String searchString = "%" + businessName.substring(2, 3) + "%"; //$NON-NLS-1$ //$NON-NLS-2$
		BusinessList businessList = new BusinessList();
		try {
			businessList = helper.getBusinessByName(inquiryUrl, publishUrl,
					searchString, 10); //$NON-NLS-1$
		} catch (MMUddiException err) {
			err.printStackTrace();
		}
		BusinessInfos businessInfos = businessList.getBusinessInfos();
		for (int i = 0; i < businessInfos.size(); i++) {
			BusinessInfo entity = businessInfos.get(i);
			System.out.println(entity.getDefaultNameString());
		}
	}

	/**
	 * Test the unPublish() method of MMUddiHelper
	 * 
	 * @since 5.6
	 */
	public void testUnPublish() {
		try {
			String businessKey = getBusinessKeyHelper();
			helper.unPublish(user, password, businessKey, url, inquiryUrl,
					publishUrl);
		} catch (MMUddiException err) {
			err.printStackTrace();
		}
	}

	/**
	 * Add a business to the UDDI Registry.
	 * 
	 * @param UddiUserName
	 *            The username used to logon to the UDDI registry
	 * @param UddiPassword
	 *            The password used to logon to the UDDI registry
	 * @param businessName
	 *            The name for the bussiness entity to add
	 * @param inquiryUrl
	 *            The inquiry url of the UDDI registry
	 * @param publishUrl
	 *            The publish url of the UDDI registry
	 * @since 5.6
	 */
	public void addBusiness(String uddiUserName, String uddiPassword,
			String businessName, String inquiryUrl, String publishUrl)
			throws MMUddiException {

		SaveBusiness saveBusiness = new SaveBusiness();
		try {
			saveBusiness.addBusiness(uddiUserName, uddiPassword, businessName,
					publishUrl, inquiryUrl);
		} catch (InvalidParameterException err) {
			err.printStackTrace();
		}
	}

}
