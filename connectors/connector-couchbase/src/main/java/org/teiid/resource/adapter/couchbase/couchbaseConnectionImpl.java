/*
 * ${license}
 */
package org.teiid.resource.adapter.couchbase;


import javax.resource.ResourceException;

import org.teiid.resource.spi.BasicConnection;
import org.teiid.logging.LogManager;
import org.teiid.core.BundleUtil;
import org.teiid.logging.LogConstants;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;

import org.teiid.translator.couchbase.couchbaseConnection;

/**
 * Connection to the resource. You must define couchbaseConnection interface, that
 * extends the "javax.resource.cci.Connection"
 */
public class couchbaseConnectionImpl extends BasicConnection implements couchbaseConnection {

	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(couchbaseConnectionImpl.class);

    private couchbaseManagedConnectionFactory config;
    private Cluster cluster;

	public couchbaseConnectionImpl(couchbaseManagedConnectionFactory env) {
        this.config = env;
        this.cluster = CouchbaseCluster.create();
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "couchbase Connection has been created."); //$NON-NLS-1$

    }

    @Override
    public void close() {
    	if (this.cluster != null)
			this.cluster.disconnect();
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "couchbase Connection has been closed."); //$NON-NLS-1$

    }
	
	@Override
	public void someMethod() throws ResourceException {
		// TODO Auto-generated method stub
		
	}
}
