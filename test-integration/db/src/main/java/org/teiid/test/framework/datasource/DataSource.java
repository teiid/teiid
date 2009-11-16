package org.teiid.test.framework.datasource;

import java.sql.Connection;
import java.util.Properties;

import javax.sql.XAConnection;

import org.teiid.test.framework.connection.ConnectionStrategy;
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
	private ConnectionStrategy conn;
	private ConnectionStrategy xaconn;
	
	public DataSource() {
	    this.name = "notassigned";
	}
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
	    
	    return this.conn.getConnection();
	}
	
	public void setConnection(ConnectionStrategy c) {
	    this.conn = c;
	}
	
	public XAConnection getXAConnection() throws QueryTestFailedException {
	    if (this.xaconn == null) return null;
	    
	    return this.xaconn.getXAConnection();
	}
	
	public void setXAConnection(ConnectionStrategy xaconn) {
	    this.xaconn = xaconn;
	}

		

}
