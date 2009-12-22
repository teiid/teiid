package org.teiid.test.framework.datasource;

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
	private XAConnection xaconn=null;
	

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
		}
	    } catch (SQLException e) {
		this.conn = null;
	    }
	    
	    return this.conn;

	}
	
	public void setConnection(Connection c) {
	    this.conn = c;
	}
	
	public XAConnection getXAConnection() throws QueryTestFailedException {	    
	    return this.xaconn;

	}
	
	public void setXAConnection(XAConnection xaconn) {
	    this.xaconn = xaconn;
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

		try {

		    if (this.xaconn != null) {
			this.xaconn.close();
		    }
		} catch (SQLException e) {
		    // ignore..
		}

		this.xaconn = null;	    

	}

		

}
