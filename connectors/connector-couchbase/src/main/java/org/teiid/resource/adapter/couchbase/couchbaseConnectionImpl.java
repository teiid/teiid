/*
 * ${license}
 */
package org.teiid.resource.adapter.couchbase;


import javax.resource.ResourceException;

import org.teiid.resource.spi.BasicConnection;
import org.teiid.logging.LogManager;
import org.teiid.core.BundleUtil;
import org.teiid.logging.LogConstants;

/**
 * Connection to the resource. You must define couchbaseConnection interface, that 
 * extends the "javax.resource.cci.Connection"
 */
public class couchbaseConnectionImpl extends BasicConnection implements couchbaseConnection {

	public static final BundleUtil UTIL = BundleUtil.getBundleUtil(couchbaseConnectionImpl.class);


    private couchbaseManagedConnectionFactory config;

    public couchbaseConnectionImpl(couchbaseManagedConnectionFactory env) {
        this.config = env;
        // todo: connect to your source here
        
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "couchbase Connection has been created."); //$NON-NLS-1$

    }
    
    @Override
    public void close() {
        LogManager.logDetail(LogConstants.CTX_CONNECTOR, "couchbase Connection has been closed."); //$NON-NLS-1$
    	
    }
}
