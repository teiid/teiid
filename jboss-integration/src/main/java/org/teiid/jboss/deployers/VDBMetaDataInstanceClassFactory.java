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
package org.teiid.jboss.deployers;

import java.util.List;

import org.jboss.beans.info.spi.BeanInfo;
import org.jboss.managed.api.ManagedObject;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.api.factory.ManagedObjectFactory;
import org.jboss.managed.plugins.factory.AbstractInstanceClassFactory;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.MetaValueFactory;
import org.teiid.adminapi.impl.DataPolicyMetadata;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.adminapi.jboss.ManagedUtil;

public class VDBMetaDataInstanceClassFactory extends AbstractInstanceClassFactory<VDBMetaData> {
	
	public VDBMetaDataInstanceClassFactory() {
	}

	public VDBMetaDataInstanceClassFactory(ManagedObjectFactory mof) {
		super(mof);
	}
	
	@Override
	public Class<VDBMetaData> getType() {
		return VDBMetaData.class;
	}

	@Override
	public void setValue(BeanInfo beanInfo, ManagedProperty property, VDBMetaData vdb, MetaValue value) {
		
		if (property.getName().equals("models")) { //$NON-NLS-1$
			List<ManagedObject> models = (List<ManagedObject>)MetaValueFactory.getInstance().unwrap(property.getValue());
			for(ManagedObject managedModel:models) {
				String modelName = ManagedUtil.getSimpleValue(managedModel, "name", String.class); //$NON-NLS-1$
				ModelMetaData model = vdb.getModel(modelName);
				
		        ManagedProperty sourceMappings = managedModel.getProperty("sourceMappings");//$NON-NLS-1$
		        if (sourceMappings != null){
		            List<ManagedObject> mappings = (List<ManagedObject>)MetaValueFactory.getInstance().unwrap(sourceMappings.getValue());
		            for (ManagedObject mo:mappings) {
		                String name = ManagedUtil.getSimpleValue(mo, "name", String.class);//$NON-NLS-1$
		                String jndiName = ManagedUtil.getSimpleValue(mo, "jndiName", String.class);//$NON-NLS-1$
		                model.addSourceMapping(name, jndiName);
		            }
		        }				
			}						
		}
		else if (property.getName().equals("JAXBProperties")) { //$NON-NLS-1$
			List<ManagedObject> properties = (List<ManagedObject>)MetaValueFactory.getInstance().unwrap(property.getValue());
			for (ManagedObject managedProperty:properties) {
				vdb.addProperty(ManagedUtil.getSimpleValue(managedProperty, "name", String.class), ManagedUtil.getSimpleValue(managedProperty, "value", String.class)); //$NON-NLS-1$ //$NON-NLS-2$
			}
		}
		else if (property.getName().equals("dataPolicies")) { //$NON-NLS-1$
			List<ManagedObject> policies = (List<ManagedObject>)MetaValueFactory.getInstance().unwrap(property.getValue());
			for(ManagedObject managedPolicy:policies) {
				String policyName = ManagedUtil.getSimpleValue(managedPolicy, "name", String.class); //$NON-NLS-1$
				DataPolicyMetadata policy = vdb.getDataPolicy(policyName);
				
		        ManagedProperty mappedRoleNames = managedPolicy.getProperty("mappedRoleNames");//$NON-NLS-1$
		        if (mappedRoleNames != null){
		            List<String> roleNames = (List<String>)MetaValueFactory.getInstance().unwrap(mappedRoleNames.getValue());
		            policy.setMappedRoleNames(roleNames);
		        }				
			}
		}		
		else {
			super.setValue(beanInfo, property, vdb, value);
		}
	}

}
