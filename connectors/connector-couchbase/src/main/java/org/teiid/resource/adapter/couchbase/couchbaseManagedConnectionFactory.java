/*
 * ${license}
 */
package org.teiid.resource.adapter.couchbase;

import javax.resource.ResourceException;
import javax.resource.spi.InvalidPropertyException;

import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

import org.teiid.core.BundleUtil;

public class couchbaseManagedConnectionFactory extends BasicManagedConnectionFactory {

	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(couchbaseManagedConnectionFactory.class);

	//private String sampleProperty = null;
	private String serverAddress = null;
	private String bucketName = null;
	private String bucketPassword = null;


	@Override
	public BasicConnectionFactory<couchbaseConnectionImpl> createConnectionFactory() throws ResourceException {

		if (serverAddress == null) {
	 		throw new InvalidPropertyException(UTIL.getString("couchbaseManagedConnectionFactory.serveraddress_not_set")); //$NON-NLS-1$
		}

		if (bucketName == null) {
			throw new InvalidPropertyException(UTIL.getString("couchbaseManagedConnectionFactory.bucketName_not_set")); //$NON-NLS-1$
		}

		if (bucketPassword == null) {
	 		throw new InvalidPropertyException(UTIL.getString("couchbaseManagedConnectionFactory.bucketPassword_not_set")); //$NON-NLS-1$
		}
		return new BasicConnectionFactory<couchbaseConnectionImpl>() {
			@Override
			public couchbaseConnectionImpl getConnection() throws ResourceException {
				return new couchbaseConnectionImpl(couchbaseManagedConnectionFactory.this);
			}
		};
	}

	// get couchbase server address
		public String getServerAddress() {
		return this.serverAddress;
	}
	// get couchbase server address
	public void setServerAddress(String server) {
		return this.serverAddress = server;
	}

	// get couchbase bucket name
	public String getBucketName() {
		return this.bucketName;
	}
	// set couchbase bucket name
	public void setBucketName(String bucketName) {
		return this.bucketName = bucketName;
	}

	// get couchbase bucket passowrd
	public String getBucketPassword() {
		return this.bucketPassword;
	}
	// set couchbase bucket name
	public void setBucketPassword(String bucketPassword) {
		return this.bucketPassword = bucketPassword;
	}


	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime
				* result
				+ ((sampleProperty == null) ? 0 : sampleProperty.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;

		couchbaseManagedConnectionFactory other = (couchbaseManagedConnectionFactory) obj;

		if (!checkEquals(this.getSampleProperty(), other.getSampleProperty())) {
			return false;
		}

		return true;

	}

}
