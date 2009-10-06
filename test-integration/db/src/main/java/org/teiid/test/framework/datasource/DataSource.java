package org.teiid.test.framework.datasource;

import java.util.Properties;

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
	

		

}
