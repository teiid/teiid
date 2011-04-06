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
import org.teiid.deployers.ManagedPropertyUtil;

/**
 * This template is to create a simplified XA JDBC datasource  
 */
public class XaJdbcConnectorTemplateInfo extends DsDataSourceTemplateInfo implements ExtendedPropertyInfo {
	
	private static final long serialVersionUID = 9066758787789280783L;
	static final String SERVER_NAME = "ServerName";//$NON-NLS-1$	
	static final String PORT_NUMBER = "PortNumber";//$NON-NLS-1$	
	static final String DATABASE_NAME = "DatabaseName";//$NON-NLS-1$
	static final String ADDITIONAL_DS_PROPS = "additional-ds-properties";//$NON-NLS-1$
	
	static final String[] EXTENDED_DS_PROPERTIES = {SERVER_NAME, PORT_NUMBER,DATABASE_NAME};
	
	public XaJdbcConnectorTemplateInfo(String name, String description, Map<String, ManagedProperty> properties) {
		super(name, description, properties);
	}

	public void start() {
		populate();
	}

	@Override
	public XaJdbcConnectorTemplateInfo copy() {
		XaJdbcConnectorTemplateInfo copy = new XaJdbcConnectorTemplateInfo(getName(), getDescription(), getProperties());
		super.copy(copy);
		copy.populate();
		return copy;
	}
	
	private void populate() {
		super.start();

		ManagedProperty mp = this.getProperties().get("connection-definition");//$NON-NLS-1$	
		mp.setValue(ManagedUtil.wrap(SimpleMetaType.STRING, "javax.sql.DataSource"));//$NON-NLS-1$	

		mp = this.getProperties().get("dsType");//$NON-NLS-1$	
		mp.setValue(ManagedUtil.wrap(SimpleMetaType.STRING, "xa-datasource"));//$NON-NLS-1$	
		
		ManagedPropertyImpl dsTypeMP = buildConfigProperty();
		addProperty(dsTypeMP);
		
		addProperty(ConnectorTemplateInfo.buildTemplateProperty(getName()));
		
		ManagedPropertyUtil.markAsTeiidProperty(this.getProperties().get("user-name")); //$NON-NLS-1$
		ManagedPropertyUtil.markAsTeiidProperty(this.getProperties().get("password"));//$NON-NLS-1$
		addProperty(ManagedPropertyUtil.createProperty(DATABASE_NAME, SimpleMetaType.STRING,"Database Name","Database Name", false, false, null));//$NON-NLS-1$ //$NON-NLS-2$
		addProperty(ManagedPropertyUtil.createProperty(PORT_NUMBER, SimpleMetaType.INTEGER,"Database Port", "Database Port",false, false, null));//$NON-NLS-1$ //$NON-NLS-2$	
		addProperty(ManagedPropertyUtil.createProperty(SERVER_NAME, SimpleMetaType.STRING,"Database Server Name", "Database Server Name", false, false, null));//$NON-NLS-1$ //$NON-NLS-2$
		addProperty(ManagedPropertyUtil.createProperty(ADDITIONAL_DS_PROPS, SimpleMetaType.STRING,"Addtional Data Source Properties", "Addtional Data source properties. (comma separated name value pairs)", false, false, null));//$NON-NLS-1$ //$NON-NLS-2$
	}

	static ManagedPropertyImpl buildConfigProperty() {
		DefaultFieldsImpl fields = new DefaultFieldsImpl("config-property");//$NON-NLS-1$	
		fields.setDescription("The config-property type"); //$NON-NLS-1$	
		fields.setMetaType(new MapCompositeMetaType (SimpleMetaType.STRING));
		ManagedPropertyImpl dsTypeMP = new ManagedPropertyImpl(fields);
		return dsTypeMP;
	}
	
	
	@Override
	/**
	 * This is for updating a single property.
	 */
	public void updateProperty(String name, String value, ManagedComponent main) {
		if (name.equals(DATABASE_NAME)||name.equals(PORT_NUMBER)||name.equals(SERVER_NAME)||name.equals(ADDITIONAL_DS_PROPS)) {
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
