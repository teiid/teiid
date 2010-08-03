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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.resource.cci.ConnectionFactory;

import org.jboss.managed.api.Fields;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.DefaultFieldsImpl;
import org.jboss.managed.plugins.ManagedPropertyImpl;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.resource.metadata.ConfigPropertyMetaData;
import org.jboss.resource.metadata.ConnectionDefinitionMetaData;
import org.jboss.resource.metadata.ConnectorMetaData;
import org.jboss.resource.metadata.DescriptionMetaData;
import org.teiid.adminapi.jboss.ManagedUtil;
import org.teiid.deployers.ExtendedPropertyMetadata;
import org.teiid.deployers.ManagedPropertyUtil;

public class RaXmlPropertyConverter {

	
	public static List<String> getPropertyNames(String rarName){
		ArrayList<String> names = new ArrayList<String>();
		Collection<ConfigPropertyMetaData> props = getRarProperties(rarName);
		if (props != null) {
			for (ConfigPropertyMetaData p:props) {
				names.add(p.getName());
			}
		}
		return names;
	}
	
	public static List<ManagedProperty> getAsManagedProperties(String rarName){
		ArrayList<ManagedProperty> managedProperties = new ArrayList<ManagedProperty>();
		Collection<ConfigPropertyMetaData> props = getRarProperties(rarName);
		if (props != null) {
			for (ConfigPropertyMetaData p:props) {
				managedProperties.add(createConnectorProperty(p));
			}
		}
		return managedProperties;
	}
	
	
	private static Collection<ConfigPropertyMetaData> getRarProperties(String rarName){
		try {
			MBeanServer server = MBeanServerFactory.findMBeanServer(null).get(0);
			ObjectName on = new ObjectName("jboss.jca:service=RARDeployment,name='"+rarName+"'");//$NON-NLS-1$	//$NON-NLS-2$	
			ConnectorMetaData obj = (ConnectorMetaData)server.getAttribute(on, "MetaData");//$NON-NLS-1$	
			ConnectionDefinitionMetaData metadata = obj.getConnectionDefinition(ConnectionFactory.class.getName());
			if (metadata != null) {
				return metadata.getProperties();
			}
		} catch (MalformedObjectNameException e) {
			//ignore
		} catch (AttributeNotFoundException e) {
			//ignore
		} catch (InstanceNotFoundException e) {
			//ignore
		} catch (MBeanException e) {
			//ignore
		} catch (ReflectionException e) {
			//ignore
		}		
		return null;
	}
	
	private static ManagedProperty createConnectorProperty(ConfigPropertyMetaData metadata) {
		
		DescriptionMetaData descMetadata = metadata.getDescription();
		String description = descMetadata.getDescription();
		if (description != null) {
			ExtendedPropertyMetadata extended = new ExtendedPropertyMetadata(metadata.getName(), metadata.getType(), description, metadata.getValue());
			return ManagedPropertyUtil.convert(extended);
		}
		
		DefaultFieldsImpl fields = new DefaultFieldsImpl(metadata.getName());
		SimpleMetaType metaType = SimpleMetaType.resolve(metadata.getType());
		fields.setMetaType(metaType);		
		if (metadata.getValue() != null && metadata.getValue().trim().length() > 0) {
			fields.setField(Fields.DEFAULT_VALUE, ManagedUtil.wrap(metaType, metadata.getValue()));
		}
		
		return new ManagedPropertyImpl(fields);
	}	
}
