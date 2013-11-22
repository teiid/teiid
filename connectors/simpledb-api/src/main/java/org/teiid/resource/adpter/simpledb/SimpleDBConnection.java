package org.teiid.resource.adpter.simpledb;

import javax.resource.cci.Connection;

public interface SimpleDBConnection extends Connection{
	
	public SimpleDbAPIClass getAPIClass();

}
