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

import org.jboss.deployers.spi.management.DeploymentTemplate;
import org.jboss.managed.api.DeploymentTemplateInfo;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.jboss.profileservice.spi.NoSuchDeploymentException;
import org.jboss.virtual.VirtualFile;
import org.teiid.adminapi.AdminComponentException;
import org.teiid.adminapi.jboss.ManagedUtil;
import org.teiid.connector.api.Connector;
import org.teiid.jboss.IntegrationPlugin;

/**
 * The connection factory template implementation. Here the idea is "targetTemplate" is the actual template we store 
 * the information under, which is the "connection factory" that container generates. However, we have got data 
 * passed under Teiid owned template. Now this classe'ss JOB is to transfer the properties from the source template 
 * into target template and write the target template.  
 * 
 * When the properties are written to target template, and source has a new property that is not defined in target, that
 * property will be written as "config-property" 
 */
public class ConnectorDeploymentTemplate implements DeploymentTemplate {

	/** The deployment template info. */
	private DeploymentTemplateInfo info;
	private DeploymentTemplate targetTemplate;
  
	/** The file suffix. */
   private static final String FILE_SUFFIX = "-ds.xml";//$NON-NLS-1$	

    
	public String getDeploymentName(String deploymentBaseName) {
		if (deploymentBaseName == null)
			throw new IllegalArgumentException("Null base name.");//$NON-NLS-1$	
		
	    if(deploymentBaseName.endsWith(FILE_SUFFIX) == false)
	        deploymentBaseName = deploymentBaseName + FILE_SUFFIX;
	    
		return deploymentBaseName;
	}

	public VirtualFile applyTemplate(DeploymentTemplateInfo sourceInfo) throws Exception {
		try {
			
			DeploymentTemplateInfo targetInfo = this.targetTemplate.getInfo();

			// override these properties always. 
			targetInfo.getProperties().get("connection-definition").setValue(SimpleValueSupport.wrap(Connector.class.getName()));//$NON-NLS-1$	
			targetInfo.getProperties().get("rar-name").setValue(SimpleValueSupport.wrap(((ConnectorTemplateInfo)getInfo()).getRarName()));//$NON-NLS-1$
			
			
			//config-properties list
			List<String> connectorPropNames = RaXmlPropertyConverter.getPropertyNames(((ConnectorTemplateInfo)getInfo()).getRarName());
			Map<String, String> configProps = ConnectorDeploymentTemplate.propertiesAsMap(sourceInfo, connectorPropNames.toArray(new String[connectorPropNames.size()]), info.getName());		
			configProps.put(ConnectorTemplateInfo.TEMPLATE_NAME, getInfo().getName());
			
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
						configProps.put(key+".type", mp.getValue().getMetaType().getClassName());//$NON-NLS-1$	
					}
				}
			}
			
			if (configProps.size() > 0) {
				MetaValue metaValue = ManagedUtil.compositeValueMap(configProps);
				targetInfo.getProperties().get("config-property").setValue(metaValue);//$NON-NLS-1$					
			}
			return this.targetTemplate.applyTemplate(targetInfo);

		} catch (NoSuchDeploymentException e) {
			throw new AdminComponentException(e.getMessage(), e);
		} catch(Exception e) {
			throw new AdminComponentException(e.getMessage(), e);
		}		
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
	
	/**
	 * Check to make sure supplied names are extracted from original list. If a mandatory property is missing fail
	 * @param values
	 * @param names
	 * @param templateName
	 * @return
	 * @throws Exception
	 */
	static Map<String, String> propertiesAsMap(DeploymentTemplateInfo values, String[] names, String templateName) throws Exception {
		Map<String, String> props = new HashMap<String, String>();
		
		Map<String, ManagedProperty> sourceProperties = values.getProperties();
		
		for (String name:names) {
			ManagedProperty mp = sourceProperties.remove(name);
			if (mp != null) {
				if (mp.getValue() != null) {
					props.put(name, ManagedUtil.stringValue(mp.getValue()));
				}
				else {
					if (mp.isMandatory()) {
						if( mp.getDefaultValue() != null) {
							props.put(name, ManagedUtil.stringValue(mp.getDefaultValue()));
						}
						else {
							throw new AdminComponentException(IntegrationPlugin.Util.getString("property_required_not_found", mp.getName(), templateName));//$NON-NLS-1$	
						}
					}
				}
			}
		}
		return props;
	}	
}
