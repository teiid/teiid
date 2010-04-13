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

import java.io.File;
import java.util.Map;

import org.jboss.managed.api.DeploymentTemplateInfo;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.profileservice.management.FilteredDeploymentTemplateInfo;
import org.jboss.resource.deployers.management.DsDataSourceTemplate;
import org.jboss.resource.deployers.management.DsDataSourceTemplateInfo;

public class ExportConnectorTypeTemplateInfo extends DsDataSourceTemplateInfo{
	private static final long serialVersionUID = 7725742249912578496L;
	
	public ExportConnectorTypeTemplateInfo(String name, String description, String datasourceType) {
		super(name, description, datasourceType);
	}

	public ExportConnectorTypeTemplateInfo(String arg0, String arg1, Map<String, ManagedProperty> arg2) {
		super(arg0, arg1, arg2);
	}

	public static void writeTemplate(File dsXml, DeploymentTemplateInfo info) throws Exception {
		ExportConnectionFactoryTemplate template = new ExportConnectionFactoryTemplate();
		template.writeTemplate(dsXml, info);
	}
	
	public static class ExportConnectionFactoryTemplate extends DsDataSourceTemplate {
		@Override
		public void writeTemplate(File dsXml, DeploymentTemplateInfo values) throws Exception {
			FilteredDeploymentTemplateInfo filterInfo = new FilteredDeploymentTemplateInfo(values);
			super.writeTemplate(dsXml, filterInfo);
		}
	}		

}
