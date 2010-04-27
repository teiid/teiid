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

import java.util.List;
import java.util.Map;

import org.jboss.managed.api.DeploymentTemplateInfo;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.virtual.VirtualFile;
import org.teiid.adminapi.jboss.ManagedUtil;



public class XaJdbcConnectorTemplate extends org.jboss.resource.deployers.management.DsDataSourceTemplate {

	private static final long serialVersionUID = 1607932913015949359L;

	@Override
	public VirtualFile applyTemplate(DeploymentTemplateInfo values) throws Exception {
		
		XaJdbcConnectorTemplateInfo info = (XaJdbcConnectorTemplateInfo)getInfo();
		
		// combine connection factory specific properties.
		List<String> connectorPropNames = RaXmlPropertyConverter.getPropertyNames(info.getRarName());
		Map<String, String> props = ConnectorDeploymentTemplate.propertiesAsMap(values, connectorPropNames.toArray(new String[connectorPropNames.size()]), info.getName());
		props.put(ConnectorTemplateInfo.TEMPLATE_NAME, getInfo().getName());
		values.getProperties().get("config-property").setValue(ManagedUtil.compositeValueMap(props));//$NON-NLS-1$	
		
		// combine data source specific properties
		props = ConnectorDeploymentTemplate.propertiesAsMap(values, XaJdbcConnectorTemplateInfo.EXTENDED_DS_PROPERTIES, info.getName());
		
		ManagedProperty mp = values.getProperties().remove(XaJdbcConnectorTemplateInfo.ADDITIONAL_DS_PROPS);
		if (mp != null && mp.getValue() != null) {
			XaJdbcConnectorTemplateInfo.parseProperties(ManagedUtil.stringValue(mp.getValue()), props);
		}
		
		values.getProperties().get("xa-datasource-properties").setValue(ManagedUtil.compositeValueMap(props));//$NON-NLS-1$
	      
		return super.applyTemplate(values);
	}
	

}
