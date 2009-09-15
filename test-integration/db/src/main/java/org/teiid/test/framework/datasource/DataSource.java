package org.teiid.test.framework.datasource;

import java.util.Properties;

public class DataSource {
	public static final String DIRECTORY="dir";
	public static final String CONNECTOR_TYPE="connectortype";
	
	private Properties props;

	private String name;
	private String group;
	
	public DataSource(String name, String group, Properties properties) {
		this.name = name;
		this.group = group;
		this.props = properties;
	}

	
	public String getName() {
		return name;
	}
	
	public String getGroup() {
		return group;
	}
	
	public String getType() {
		return props.getProperty(CONNECTOR_TYPE);
	}
	
	public String getProperty(String propName) {
		return props.getProperty(propName);
	}
	
	public Properties getProperties() {
		return this.props;
	}
	
	

}
