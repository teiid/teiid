package org.teiid.resource.adapter.simpledb;

import javax.resource.ResourceException;

import org.teiid.resource.spi.BasicConnectionFactory;
import org.teiid.resource.spi.BasicManagedConnectionFactory;

public class SimpleDBManagedConnectionFactory extends BasicManagedConnectionFactory {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1346340853914009086L;
	
	private String accessKey;
	private String secretAccessKey;

	@Override
	@SuppressWarnings("serial")
	public BasicConnectionFactory<SimpleDBConnectionImpl> createConnectionFactory()
			throws ResourceException {
		return new BasicConnectionFactory<SimpleDBConnectionImpl>() {

			@Override
			public SimpleDBConnectionImpl getConnection()
					throws ResourceException {
				return new SimpleDBConnectionImpl(accessKey, secretAccessKey);
			}
			
		};
	}
	
	@Override
	public boolean equals(Object obj) {
		return super.equals(obj);
	}
	
	@Override
	public int hashCode() {
		return super.hashCode();
	}

	public String getAccessKey() {
		return accessKey;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public String getSecretAccessKey() {
		return secretAccessKey;
	}

	public void setSecretAccessKey(String secretAccessKey) {
		this.secretAccessKey = secretAccessKey;
	}

}
