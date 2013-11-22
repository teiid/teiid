package org.teiid.resource.adapter.simpledb;

import javax.resource.ResourceException;

import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.resource.adpter.simpledb.SimpleDbAPIClass;
import org.teiid.resource.spi.BasicConnection;

public class SimpleDBConnectionImpl extends BasicConnection implements
		SimpleDBConnection {

	private SimpleDbAPIClass apiClass;

	public SimpleDBConnectionImpl(String accessKey, String secretAccessKey) {
		apiClass = new SimpleDbAPIClass(accessKey, secretAccessKey);
	}

	public void close() throws ResourceException {

	}

	public SimpleDbAPIClass getAPIClass() {
		return apiClass;
	}
}
