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
 * This template is to create a simplified local JDBC datasource  
 */
public class LocalJdbcConnectorTemplateInfo extends DsDataSourceTemplateInfo implements ExtendedPropertyInfo {
	private static final long serialVersionUID = 7618814758011974868L;
	static final String ADDITIONAL_CONNECTION_PROPS = "addtional-connection-properties";//$NON-NLS-1$
	
	public LocalJdbcConnectorTemplateInfo(String name, String description, Map<String, ManagedProperty> properties) {
		super(name, description, properties);
	}

	public void start() {
		populate();
	}

	@Override
	public LocalJdbcConnectorTemplateInfo copy() {
		LocalJdbcConnectorTemplateInfo copy = new LocalJdbcConnectorTemplateInfo(getName(), getDescription(), getProperties());
		super.copy(copy);
		copy.populate();
		return copy;
	}
	
	private void populate() {
		super.start();

		ManagedProperty mp = this.getProperties().get("connection-definition");//$NON-NLS-1$	
		mp.setValue(ManagedUtil.wrap(SimpleMetaType.STRING, "javax.sql.DataSource"));//$NON-NLS-1$	

		mp = this.getProperties().get("dsType");//$NON-NLS-1$	
		mp.setValue(ManagedUtil.wrap(SimpleMetaType.STRING, "local-tx-datasource"));//$NON-NLS-1$	
		
		ManagedPropertyImpl dsTypeMP = buildConfigProperty();
		addProperty(dsTypeMP);
		
		addProperty(ConnectorTemplateInfo.buildTemplateProperty(getName()));
		
		ManagedPropertyUtil.markAsTeiidProperty(this.getProperties().get("user-name")); //$NON-NLS-1$
		ManagedPropertyUtil.markAsTeiidProperty(this.getProperties().get("password"));//$NON-NLS-1$
		ManagedPropertyUtil.markAsTeiidProperty(this.getProperties().get("driver-class")); //$NON-NLS-1$
		ManagedPropertyUtil.markAsTeiidProperty(this.getProperties().get("connection-url"));//$NON-NLS-1$
		
		addProperty(ManagedPropertyUtil.createProperty(ADDITIONAL_CONNECTION_PROPS, SimpleMetaType.STRING,"Addtional Connection Properties", "Addtional Connection properties. (comma separated name value pairs)", false, false, null));//$NON-NLS-1$ //$NON-NLS-2$
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
		if (name.equals(ADDITIONAL_CONNECTION_PROPS)) {
			Map<String, String> map = new HashMap<String, String>();
			parseProperties(value, map);
			
			// update the container managed object.
			MapCompositeValueSupport previousValues = (MapCompositeValueSupport)main.getProperty("connection-properties").getValue(); //$NON-NLS-1$
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
