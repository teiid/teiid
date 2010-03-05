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

import org.jboss.deployers.spi.management.DeploymentTemplate;
import org.jboss.managed.api.DeploymentTemplateInfo;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.jboss.profileservice.spi.NoSuchDeploymentException;
import org.jboss.virtual.VirtualFile;
import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.AdminProcessingException;
import org.teiid.adminapi.jboss.ManagedUtil;

/**
 * The connection factory template implementation. Here the idea is "targetTemplate" is the actual template we store 
 * the information under, which is the "connection factory" that container generates. However, we have got data 
 * passed under Teiid owned template. Now this classe'ss JOB is to transfer the properties from the source template 
 * into target template and write the target template.  
 * 
 * When the properties are written to target template, and source has a new property that is not defined in target, that
 * property will be written as "config-property" 
 */
public class ConnectorTypeTemplate implements DeploymentTemplate {

	/** The deployment template info. */
	private DeploymentTemplateInfo info;
	private DeploymentTemplate targetTemplate;
  
	/** The file suffix. */
   private static final String FILE_SUFFIX = "-ds.xml";

    
	public String getDeploymentName(String deploymentBaseName) {
		if (deploymentBaseName == null)
			throw new IllegalArgumentException("Null base name.");
		
	    if(deploymentBaseName.endsWith(FILE_SUFFIX) == false)
	        deploymentBaseName = deploymentBaseName + FILE_SUFFIX;
	    
		return deploymentBaseName;
	}

	public VirtualFile applyTemplate(DeploymentTemplateInfo sourceInfo) throws Exception {
		try {
			
			ManagedProperty rar = sourceInfo.getProperties().get("rar-name");
			String rarName = ManagedUtil.stringValue(rar.getValue());
			if (!isValidRar(rarName)) {
				throw new AdminProcessingException("Invalid RAR specified; please supply correct RAR file. "+rarName);
			}
			
			DeploymentTemplateInfo targetInfo = this.targetTemplate.getInfo();

			// override these properties always. 
			targetInfo.getProperties().get("connection-definition").setValue(SimpleValueSupport.wrap("org.teiid.connector.api.Connector"));
			
			//config-properties list
			Map<String, String> configProps = new HashMap<String, String>();
			
			// template properties specific to the template
			Map<String, ManagedProperty> propertyMap = targetInfo.getProperties();
			
			// walk through the supplied properties and assign properly to either template
			// or config-properties.
			for (String key:sourceInfo.getProperties().keySet()) {
				ManagedProperty mp = propertyMap.get(key);
								
				if (mp != null) {
					// property found in target, so just add as value
					MetaValue value = sourceInfo.getProperties().get(key).getValue();
					if (ManagedUtil.sameValue(mp.getDefaultValue(), value)) {
						continue;
					}		
					
					if (value != null) {
						mp.setValue(value);
					}
				}
				else {
					// property not found in the target; add as "config-property"
					mp = sourceInfo.getProperties().get(key);
					if (ManagedUtil.sameValue(mp.getDefaultValue(), mp.getValue())) {
						continue;
					}	
					
					if (mp.getValue() != null) {
						configProps.put(key, ManagedUtil.stringValue(mp.getValue()));
						configProps.put(key+".type", mp.getValue().getMetaType().getClassName());
					}
				}
			}
			
			if (configProps.size() > 0) {
				MetaValue metaValue = ManagedUtil.compositeValueMap(configProps);
				targetInfo.getProperties().get("config-property").setValue(metaValue);				
			}
			return this.targetTemplate.applyTemplate(targetInfo);

		} catch (NoSuchDeploymentException e) {
			throw new AdminComponentException(e.getMessage(), e);
		} catch(Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}		
	}

	private boolean isValidRar(String rarName) {
		return rarName != null;
	}
		
	@Override
	public DeploymentTemplateInfo getInfo() {
		return info;
	}

	public void setInfo(DeploymentTemplateInfo info) {
		this.info = info;
	}
	
	public void setTargetTemplate(DeploymentTemplate target) {
		this.targetTemplate = target;
	}
}
