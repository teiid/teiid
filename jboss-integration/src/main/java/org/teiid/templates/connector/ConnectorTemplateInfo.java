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

import org.jboss.managed.api.Fields;
import org.jboss.managed.api.ManagedComponent;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.BasicDeploymentTemplateInfo;
import org.jboss.managed.plugins.DefaultFieldsImpl;
import org.jboss.managed.plugins.ManagedPropertyImpl;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.MapCompositeValueSupport;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.teiid.adminapi.jboss.ExtendedPropertyInfo;
import org.teiid.connector.api.Connector;

/**
 * This class some magic in it. First off all through the configuration it extends the
 * NoTxConnectionFactoryTemplate. Then using the JMX adds the properties defined inside a connector
 * RAR file's ra.xml dynamically the above template. The RAR file name is supplied in the "description" 
 * field of the configuration. Also, it uses the NoTxConnectionFactoryTemplate "applyTemplate" to write
 * the custom properties that have been added thru JMX as "config-property" in the eventual "-ds.xml" file.
 */
public class ConnectorTemplateInfo extends BasicDeploymentTemplateInfo implements ExtendedPropertyInfo {
	
	private static final long serialVersionUID = 9066758787789280783L;
	private String rarName;
	static final String TEMPLATE_NAME = "template-name"; //$NON-NLS-1$
	private static final String TEIID_PROPERTY = "teiid-property"; //$NON-NLS-1$
	
	
	public ConnectorTemplateInfo(String name, String description, Map<String, ManagedProperty> properties) {
		super(name, description, properties);
	}

	public void start() {
		populate();
	}

	@Override
	public ConnectorTemplateInfo copy() {
		ConnectorTemplateInfo copy = new ConnectorTemplateInfo(getName(), getDescription(), getProperties());
		copy.setRarName(getRarName());
		super.copy(copy);
		copy.populate();
		
		ManagedProperty mp = copy.getProperties().get("connection-definition");//$NON-NLS-1$
		mp.setValue(SimpleValueSupport.wrap(Connector.class.getName())); 
		
		mp = copy.getProperties().get("rar-name");//$NON-NLS-1$	
		mp.setValue(SimpleValueSupport.wrap(getRarName()));
		return copy;
	}
	
	private void populate() {
		List<ManagedProperty> props = RaXmlPropertyConverter.getAsManagedProperties(getRarName());
		for (ManagedProperty p:props) {
			addProperty(p);
		}
		addProperty(buildTemplateProperty(getName()));
	}
	
	public String getRarName() {
		return rarName;
	}

	public void setRarName(String rarName) {
		this.rarName = rarName;
	}
	
	static ManagedProperty buildTemplateProperty(String name) {
		ManagedProperty mp = buildProperty(TEMPLATE_NAME, SimpleMetaType.STRING, "Template Name", "The Name of the Teiid Connector Template", true, name);//$NON-NLS-1$ //$NON-NLS-2$
		mp.setField(Fields.READ_ONLY, SimpleValueSupport.wrap(true));
		return mp;
	}	

	static ManagedProperty buildProperty(String name, SimpleMetaType type, String displayName, String description, boolean mandatory, String value) {
		DefaultFieldsImpl fields = new DefaultFieldsImpl(name);
		fields.setDescription(description);
		fields.setField(Fields.MAPPED_NAME,displayName);
		fields.setMetaType(type);
		fields.setField(Fields.MANDATORY, SimpleValueSupport.wrap(mandatory));
		fields.setField(TEIID_PROPERTY, SimpleValueSupport.wrap(true));
		if (value != null) {
			fields.setField(Fields.DEFAULT_VALUE, SimpleValueSupport.wrap(value));
		}
		return  new ManagedPropertyImpl(fields);		
	}	
	
	static void markAsTeiidProperty(ManagedProperty mp) {
		mp.setField(TEIID_PROPERTY, SimpleValueSupport.wrap(true)); 
	}
	
	@Override
	public void updateProperty(String name, String value, ManagedComponent main) {
		List<String> connectorNames = RaXmlPropertyConverter.getPropertyNames(getRarName());
		if (connectorNames.contains(name)) {
			updateManagedConnectionFactory(name, value, main);
		}
	}

	static void updateManagedConnectionFactory(String name, String value, ManagedComponent mc) {
		// Update the Container connection factory
		MapCompositeValueSupport previousValues = (MapCompositeValueSupport)mc.getProperty("config-property").getValue(); //$NON-NLS-1$
		if (previousValues != null) {
			previousValues.put(name, SimpleValueSupport.wrap(value));
		}
	}
}
