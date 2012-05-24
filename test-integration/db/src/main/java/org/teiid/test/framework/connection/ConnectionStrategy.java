/*
 * Copyright (c) 2000-2007 MetaMatrix, Inc.
 * All rights reserved.
 */
package org.teiid.test.framework.connection;

import java.sql.Connection;
import java.util.Properties;

import javax.sql.XAConnection;

import org.teiid.core.util.PropertiesUtils;
import org.teiid.test.framework.ConfigPropertyLoader;
import org.teiid.test.framework.datasource.DataSource;
import org.teiid.test.framework.datasource.DataSourceMgr;
import org.teiid.test.framework.datasource.DataStore;
import org.teiid.test.framework.exception.QueryTestFailedException;
import org.teiid.test.framework.exception.TransactionRuntimeException;

@SuppressWarnings("nls")
public abstract class ConnectionStrategy {

    private Properties env = null;
    // private DataSourceFactory dsFactory;
    // the useProxy is used for non-teiid connections so that the sources are
    // closed and reconnected all the time
    private boolean useProxy = false;

    public ConnectionStrategy(Properties props) {
	this.env = PropertiesUtils.clone(props);

    }

    /*
     * Lifecycle methods for managing the connection
     */

    /**
     * Returns a connection
     * 
     * @return Connection
     * @throws QueryTestFailedException 
     */
    public abstract Connection getConnection() throws QueryTestFailedException;

    public boolean useProxy() {
	return this.useProxy;
    }

    void setUseProxy(boolean useproxy) {
	this.useProxy = useproxy;
    }

    /**
     * @since
     */
    public void shutdown() {

    }

    /**
     * @throws QueryTestFailedException  
     */
    public Connection getAdminConnection() throws QueryTestFailedException {
	return null;
    }

    private boolean autoCommit;

    public boolean getAutocommit() {
	return autoCommit;
    }

    /**
     * @throws QueryTestFailedException  
     */
    public XAConnection getXAConnection() throws QueryTestFailedException {
	return null;
    }

    /**
     * In certain testcases, the data that being provided is already
     * preconfigured and should not be touched by the {@link DataStore}
     * processing.
     * 
     * @return
     */
    public boolean isDataStoreDisabled() {
	return ConfigPropertyLoader.getInstance().isDataStoreDisabled();
    }

    public Properties getEnvironment() {
	return env;
    }

    public void setEnvironmentProperty(String key, String value) {
	this.env.setProperty(key, value);
    }

    /**
     * @throws QueryTestFailedException  
     */
    void configure() throws QueryTestFailedException {
//
//	if (this.isDataStoreDisabled()) {
//	    return;
//	} else {
//	    
//	 // commenting out until embedded testing is made available and its required to configure
//	 // the vdb and bindings in this mannder
//
//	    if (true)
//		return;
//	}
//
//	try {
//	    // the the driver strategy is going to be used to connection
//	    // directly to the connector binding
//	    // source, then no administration can be done
//	    Admin admin = AdminFactory.getInstance().createAdmin(
//		    this.env.getProperty("admin.user"),
//		    this.env.getProperty("admin.password").toCharArray(),
//		    this.env.getProperty("admin.url"));
//
//	    java.sql.Connection conn = getConnection();
//
//	    if (!(conn instanceof ConnectionImpl)) {
//		TestLogger
//			.log("ConnectionStrategy configuration:  connection is not of type MMConnection and therefore no vdb setup will be performed");
//		return;
//	    }
//	    
//	   	   
//	    // setupVDBConnectorBindings(admin);
//
//	    // admin.restart();
//
//	    int sleep = 5;
//
//	    TestLogger.log("Bouncing the system..(wait " + sleep + " seconds)"); //$NON-NLS-1$
//	    Thread.sleep(1000 * sleep);
//	    TestLogger.log("done."); //$NON-NLS-1$
//
//	} catch (Throwable e) {
//	    e.printStackTrace();
//
//	    throw new TransactionRuntimeException(e.getMessage());
//	} finally {
//	    // need to close and flush the connection after restarting
//	    // this.shutdown();
//
//	}
    }

    // protected void setupVDBConnectorBindings(Admin api) throws
    // QueryTestFailedException {
    //         
    // try {
    //    	    
    // VDB vdb = null;
    // Set<VDB> vdbs = api.getVDBs();
    // if (vdbs == null || vdbs.isEmpty()) {
    // throw new QueryTestFailedException(
    // "AdminApi.GetVDBS returned no vdbs available");
    // }
    //
    // String urlString = this.env.getProperty(DriverConnection.DS_URL);
    // JDBCURL url = new JDBCURL(urlString);
    // TestLogger.logDebug("Trying to match VDB : " + url.getVDBName());
    //
    // for (Iterator<VDB> iterator = vdbs.iterator(); iterator.hasNext();) {
    // VDB v = (VDB) iterator.next();
    // if (v.getName().equalsIgnoreCase(url.getVDBName())) {
    // vdb = v;
    // }
    //
    // }
    // if (vdbs == null) {
    // throw new QueryTestFailedException(
    // "GetVDBS did not return a vdb that matched "
    // + url.getVDBName());
    // }
    //	    	    
    // List<Model> models = vdb.getModels();
    // Iterator<Model> modelIt = models.iterator();
    // while (modelIt.hasNext()) {
    // Model m = modelIt.next();
    //
    // if (!m.isSource())
    // continue;
    //
    // // get the mapping, if defined
    // String mappedName = this.env.getProperty(m.getName());
    //
    // String useName = m.getName();
    // if (mappedName != null) {
    // useName = mappedName;
    // }
    //
    // org.teiid.test.framework.datasource.DataSource ds =
    // this.dsFactory.getDatasource(useName, m.getName());
    //
    // if (ds != null) {
    //
    //		    TestLogger.logInfo("Set up Connector Binding (model:mapping:type): " + m.getName() + ":" + useName + ":" + ds.getConnectorType()); //$NON-NLS-1$
    //
    // api.addConnectorBinding(ds.getName(),ds.getConnectorType(),
    // ds.getProperties());
    //
    // api.assignBindingToModel(vdb.getName(), vdb.getVersion(), m.getName(),
    // ds.getName(), ds.getProperty("jndi-name"));
    //
    // api.startConnectorBinding(api.getConnectorBinding(ds.getName()));
    // } else {
    // throw new QueryTestFailedException(
    // "Error: Unable to create binding to map to model : "
    // + m.getName() + ", the mapped name "
    // + useName
    // + " had no datasource properties defined");
    // }
    //
    // }
    //    		
    // } catch (QueryTestFailedException qt) {
    // throw qt;
    // } catch (Exception t) {
    // t.printStackTrace();
    // throw new QueryTestFailedException(t);
    // }
    //
    //    	
    // }

    public synchronized Connection createDriverConnection(String identifier)
	    throws QueryTestFailedException {

	DataSource ds = null;
	if (identifier != null) {
	    ds = DataSourceMgr.getInstance().getDataSource(identifier);
	}
	if (ds == null) {
	    throw new TransactionRuntimeException(
		    "Program Error: DataSource is not mapped to Identifier "
			    + identifier);
	}

	Connection conn = ds.getConnection();

	if (conn != null)
	    return conn;

	ConnectionStrategy cs = null;
	if (identifier == null) {
	    cs = new DriverConnection(ds.getProperties());

	} else {
	    cs = new DriverConnection(ds.getProperties());
	}

//	conn = cs.getConnection();
//
//	conn = (Connection) Proxy.newProxyInstance(Thread.currentThread()
//		.getContextClassLoader(),
//		new Class[] { java.sql.Connection.class },
//		new CloseInterceptor(conn));

	ds.setConnection(cs.getConnection());

	return ds.getConnection();

    }

    public synchronized XAConnection createDataSourceConnection(
	    String identifier) throws QueryTestFailedException {

	DataSource ds = null;
	if (identifier != null) {
	    ds = DataSourceMgr.getInstance().getDataSource(identifier);
	}
	if (ds == null) {
	    throw new TransactionRuntimeException(
		    "Program Error: DataSource is not mapped to Identifier "
			    + identifier);
	}

	XAConnection conn = ds.getXAConnection();

	if (conn != null)
	    return conn;

	ConnectionStrategy cs = null;
	if (identifier == null) {
	    cs = new DataSourceConnection(ds.getProperties());
	} else {
	    cs = new DataSourceConnection(ds.getProperties());
	}

//	conn = cs.getXAConnection();
//
//	conn = (XAConnection) Proxy.newProxyInstance(Thread.currentThread()
//		.getContextClassLoader(),
//		new Class[] { javax.sql.XAConnection.class },
//		new CloseInterceptor(conn));

	ds.setXAConnection(cs.getXAConnection());

	return ds.getXAConnection();

    }

//    class CloseInterceptor implements InvocationHandler {
//
//	Connection conn;
//	XAConnection xaconn;
//
//	CloseInterceptor(Object conn) {
//	    if (conn instanceof Connection) {
//		this.conn = (Connection) conn;
//	    } else {
//		this.xaconn = (XAConnection) conn;
//	    }
//	}
//
//	public Object invoke(Object proxy, Method method, Object[] args)
//		throws Throwable {
//	    if (method.getName().equals("close")) { //$NON-NLS-1$
//		return null;
//	    }
//	    try {
//		return method.invoke(this.conn, args);
//	    } catch (InvocationTargetException e) {
//		throw e.getTargetException();
//	    }
//	}
//    }

}
