package org.teiid.test.framework.datasource;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.XAConnection;

import org.teiid.test.framework.exception.QueryTestFailedException;

/**
 * DataSource represents a single database that was configured by a connection.properties file.
 * @author vanhalbert
 *
 */
@SuppressWarnings("nls")
public class DataSource {
	public static final String CONNECTOR_TYPE="db.connectortype";
	public static final String DB_TYPE="db.type";
	
	private Properties props;

	private String name;
	private String group;
	private String dbtype;
	
	// The connections are stored in the datasource and are reused
	// for the duration of all tests so thats there's not
	// disconnect/connect being performed over and over
	private Connection conn=null;
	private Connection proxyconn = null;
	private XAConnection xaconn=null;
	private XAConnection proxyxaconn=null;
	

	public DataSource(String name, String group, Properties properties) {
		this.name = name;
		this.group = group;
		this.props = properties;
		this.dbtype = this.props.getProperty(DB_TYPE);
	}

	
	public String getName() {
		return name;
	}
	
	public String getGroup() {
		return group;
	}
	
	public String getConnectorType() {
		return props.getProperty(CONNECTOR_TYPE);
	}
	
	public String getProperty(String propName) {
		return props.getProperty(propName);
	}
	
	public Properties getProperties() {
		return this.props;
	}
	
	public String getDBType() {
		return this.dbtype;
	}
	
	
	public Connection getConnection() throws QueryTestFailedException {
	    if (this.conn == null) return null;
   
	    try {
		if (this.conn.isClosed()) {
		    this.conn = null;
		    this.proxyconn = null;
		}
	    } catch (SQLException e) {
		this.conn = null;
		this.proxyconn = null;
	    }
	    
	    return this.proxyconn;

	}
	
	public void setConnection(Connection c) {
	    this.conn = c;
	    
	    this.proxyconn = (Connection) Proxy.newProxyInstance(Thread.currentThread()
			.getContextClassLoader(),
			new Class[] { java.sql.Connection.class },
			new CloseInterceptor(conn));
	}
	
	public XAConnection getXAConnection() throws QueryTestFailedException {	    
	    return this.proxyxaconn;

	}
	
	public void setXAConnection(XAConnection xaconn) {
	    this.xaconn = xaconn;
	    
	    this.proxyxaconn =  (XAConnection) Proxy.newProxyInstance(Thread.currentThread()
 		.getContextClassLoader(),
 		new Class[] { XAConnection.class },
 		new CloseInterceptor(xaconn));
	}
	
	public void shutdown() {

		if (this.conn != null) {
		    try {
			this.conn.close();
		    } catch (Exception e) {
			// ignore
		    } 
		}

		this.conn = null;


		if (this.xaconn != null) {
		    try {			
			    this.xaconn.close();
		    } catch (Exception e) {
			    // ignore..
		    }
	}


		this.xaconn = null;	    

	}
	

	 class CloseInterceptor implements InvocationHandler {

		Connection conn;
		XAConnection xaconn;

		CloseInterceptor(Object conn) {
		    if (conn instanceof Connection) {
			this.conn = (Connection) conn;
		    } else {
			this.xaconn = (XAConnection) conn;
		    }
		}

		public Object invoke(Object proxy, Method method, Object[] args)
			throws Throwable {
		    if (method.getName().equals("close")) { //$NON-NLS-1$
			return null;
		    }
		    try {
			return method.invoke(this.conn, args);
		    } catch (InvocationTargetException e) {
			throw e.getTargetException();
		    }
		}
	    }	

}
