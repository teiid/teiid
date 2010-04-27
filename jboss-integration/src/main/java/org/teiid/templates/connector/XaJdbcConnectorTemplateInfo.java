/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
package org.teiid.templates.connector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.DefaultFieldsImpl;
import org.jboss.managed.plugins.ManagedPropertyImpl;
import org.jboss.metatype.api.types.MapCompositeMetaType;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.MapCompositeValueSupport;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.jboss.resource.deployers.management.DsDataSourceTemplateInfo;
import org.teiid.adminapi.jboss.ExtendedPropertyInfo;
import org.teiid.adminapi.jboss.ManagedUtil;

/**
 * This template is specific to XA data source combined with Teiid's JDBC connector.
 */
public class XaJdbcConnectorTemplateInfo extends DsDataSourceTemplateInfo implements ExtendedPropertyInfo {
	
	private static final long serialVersionUID = 9066758787789280783L;
	static final String SERVER_NAME = "ServerName";//$NON-NLS-1$	
	static final String PORT_NUMBER = "PortNumber";//$NON-NLS-1$	
	static final String DATABASE_NAME = "DatabaseName";//$NON-NLS-1$
	static final String ADDITIONAL_DS_PROPS = "addtional-ds-properties";//$NON-NLS-1$
	
	static final String[] EXTENDED_DS_PROPERTIES = {SERVER_NAME, PORT_NUMBER,DATABASE_NAME};
	
	private String rarName;
	
	public XaJdbcConnectorTemplateInfo(String name, String description, Map<String, ManagedProperty> properties) {
		super(name, description, properties);
	}

	public void start() {
		populate();
	}

	@Override
	public XaJdbcConnectorTemplateInfo copy() {
		XaJdbcConnectorTemplateInfo copy = new XaJdbcConnectorTemplateInfo(getName(), getDescription(), getProperties());
		copy.setRarName(getRarName());
		super.copy(copy);
		copy.populate();
		return copy;
	}
	
	private void populate() {
		super.start();
		List<ManagedProperty> props = RaXmlPropertyConverter.getAsManagedProperties(getRarName());
		for (ManagedProperty p:props) {
			addProperty(p);
		}

		ManagedProperty mp = this.getProperties().get("connection-definition");//$NON-NLS-1$	
		mp.setValue(ManagedUtil.wrap(SimpleMetaType.STRING, "javax.sql.DataSource"));//$NON-NLS-1$	

		mp = this.getProperties().get("dsType");//$NON-NLS-1$	
		mp.setValue(ManagedUtil.wrap(SimpleMetaType.STRING, "xa-datasource"));//$NON-NLS-1$	
		
		
		ManagedPropertyImpl dsTypeMP = buildConfigProperty();
		addProperty(dsTypeMP);
		
		addProperty(ConnectorTemplateInfo.buildTemplateProperty(getName()));
		
		ConnectorTemplateInfo.markAsTeiidProperty(this.getProperties().get("user-name")); //$NON-NLS-1$
		ConnectorTemplateInfo.markAsTeiidProperty(this.getProperties().get("password"));//$NON-NLS-1$
		addProperty(ConnectorTemplateInfo.buildProperty(DATABASE_NAME, SimpleMetaType.STRING,"Database Name","Database Name", false, null));//$NON-NLS-1$ //$NON-NLS-2$
		addProperty(ConnectorTemplateInfo.buildProperty(PORT_NUMBER, SimpleMetaType.INTEGER,"Database Port", "Database Port",false, null));//$NON-NLS-1$ //$NON-NLS-2$	
		addProperty(ConnectorTemplateInfo.buildProperty(SERVER_NAME, SimpleMetaType.STRING,"Database Server Name", "Database Server Name", false, null));//$NON-NLS-1$ //$NON-NLS-2$
		addProperty(ConnectorTemplateInfo.buildProperty(ADDITIONAL_DS_PROPS, SimpleMetaType.STRING,"Addtional Data Source Properties", "Addtional Data source properties. (comma separated name value pairs)", false, null));//$NON-NLS-1$ //$NON-NLS-2$
	}

	static ManagedPropertyImpl buildConfigProperty() {
		DefaultFieldsImpl fields = new DefaultFieldsImpl("config-property");//$NON-NLS-1$	
		fields.setDescription("The config-property type"); //$NON-NLS-1$	
		fields.setMetaType(new MapCompositeMetaType (SimpleMetaType.STRING));
		ManagedPropertyImpl dsTypeMP = new ManagedPropertyImpl(fields);
		return dsTypeMP;
	}
	
	
	public String getRarName() {
		return rarName;
	}

	public void setRarName(String rarName) {
		this.rarName = rarName;
	}

	@Override
	public void updateProperty(String name, String value, ManagedComponent main) {
		List<String> connectorNames = RaXmlPropertyConverter.getPropertyNames(getRarName());
		if (connectorNames.contains(name)) {
			ConnectorTemplateInfo.updateManagedConnectionFactory(name, value, main);
			//ConnectorTemplateInfo.updateConnectionFactory(name, value, cf);			
		}
		else if (name.equals(DATABASE_NAME)||name.equals(PORT_NUMBER)||name.equals(SERVER_NAME)||name.equals(ADDITIONAL_DS_PROPS)) {
			Map<String, String> map = new HashMap<String, String>();
			
			if (name.equals(ADDITIONAL_DS_PROPS)) {
				parseProperties(value, map);
			}
			else {
				map.put(name, value);
			}
			
			// update the container managed object.
			MapCompositeValueSupport previousValues = (MapCompositeValueSupport)main.getProperty("xa-datasource-properties").getValue(); //$NON-NLS-1$
			if (previousValues != null) {
				for (String key:map.keySet()) {
					previousValues.put(key, SimpleValueSupport.wrap(map.get(key)));
				}
			}
		}
	}
	
	static void parseProperties(String str, Map<String, String> props) {
		str = str.trim();
		StringTokenizer st = new StringTokenizer(str, ",");  //$NON-NLS-1$
		while (st.hasMoreTokens()) {
			String property = st.nextToken();
			int index = property.indexOf('=');
			if (index != -1 && property.length() > (index+1)) {
				props.put(property.substring(0, index).trim(), property.substring(index+1).trim());
			}
		}
	}	
}
