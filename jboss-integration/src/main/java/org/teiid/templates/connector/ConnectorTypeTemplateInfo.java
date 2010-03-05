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

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.jboss.managed.api.Fields;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.plugins.BasicDeploymentTemplateInfo;
import org.jboss.managed.plugins.DefaultFieldsImpl;
import org.jboss.managed.plugins.ManagedPropertyImpl;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.resource.metadata.ConfigPropertyMetaData;
import org.jboss.resource.metadata.ConnectionDefinitionMetaData;
import org.jboss.resource.metadata.ConnectorMetaData;
import org.jboss.resource.metadata.DescriptionMetaData;
import org.teiid.adminapi.jboss.ManagedUtil;

/**
 * This class some magic in it. First off all through the configuration it extends the
 * NoTxConnectionFactoryTemplate. Then using the JMX adds the properties defined inside a connector
 * RAR file's ra.xml dynamically the above template. The RAR file name is supplied in the "description" 
 * field of the configuration. Also, it uses the NoTxConnectionFactoryTemplate "applyTemplate" to write
 * the custom properties that have been added thru JMX as "config-property" in the eventual "-ds.xml" file.
 */
public class ConnectorTypeTemplateInfo extends BasicDeploymentTemplateInfo {

	private static final long serialVersionUID = 9066758787789280783L;

	public ConnectorTypeTemplateInfo(String arg0, String arg1, Map<String, ManagedProperty> arg2) {
		super(arg0, arg1, arg2);
	}

	public void start() {
		populate();
	}

	@Override
	public ConnectorTypeTemplateInfo copy() {
		ConnectorTypeTemplateInfo copy = new ConnectorTypeTemplateInfo(getName(), getDescription(), getProperties());
		super.copy(copy);
		copy.populate();
		return copy;
	}
	
	
	private void populate() {
		try {
			MBeanServer server = MBeanServerFactory.findMBeanServer(null).get(0);
			ObjectName on = new ObjectName("jboss.jca:service=RARDeployment,name='"+getName()+".rar'");
			ConnectorMetaData obj = (ConnectorMetaData)server.getAttribute(on, "MetaData");
			ConnectionDefinitionMetaData metadata = obj.getConnectionDefinition("org.teiid.connector.api.Connector");
			Collection<ConfigPropertyMetaData> props = metadata.getProperties();
			for (ConfigPropertyMetaData p:props) {
				addConnectorProperty(p);
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
	}

	private void addConnectorProperty(ConfigPropertyMetaData metadata) {
		SimpleMetaType metaType = SimpleMetaType.resolve(metadata.getType());
		
		DefaultFieldsImpl fields = new DefaultFieldsImpl(metadata.getName());
		DescriptionMetaData descMetadata = metadata.getDescription();
		String description = descMetadata.getDescription();
		if (description != null) {
			ExtendedPropertyMetadata extended = new ExtendedPropertyMetadata(description);
			if (extended.getDescription() != null) {
				fields.setDescription(description);
			}
			
			if (extended.getDisplayName() != null) {
				fields.setField(Fields.MAPPED_NAME, extended.getDisplayName());
			}
			
			if (extended.getAllowed() != null) {
				HashSet<MetaValue> values = new HashSet<MetaValue>();
				for (String value:extended.getAllowed()) {
					values.add(ManagedUtil.wrap(SimpleMetaType.STRING, value));
				}
				fields.setField(Fields.LEGAL_VALUES, values);
			}
			fields.setField(Fields.MANDATORY, ManagedUtil.wrap(SimpleMetaType.BOOLEAN_PRIMITIVE, String.valueOf(extended.isRequired())));
			fields.setField(Fields.READ_ONLY,  ManagedUtil.wrap(SimpleMetaType.BOOLEAN_PRIMITIVE, String.valueOf(!extended.isEditable())));
			fields.setField("advanced",  ManagedUtil.wrap(SimpleMetaType.BOOLEAN_PRIMITIVE, String.valueOf(extended.isAdvanced())));
			fields.setField("masked",  ManagedUtil.wrap(SimpleMetaType.BOOLEAN_PRIMITIVE, String.valueOf(extended.isMasked())));
		}
		
		fields.setMetaType(metaType);		
		if (metadata.getValue() != null && metadata.getValue().trim().length() > 0) {
			fields.setField(Fields.DEFAULT_VALUE, ManagedUtil.wrap(metaType, metadata.getValue()));
		}
		
		ManagedPropertyImpl dsTypeMP = new ManagedPropertyImpl(fields);
		addProperty(dsTypeMP);
	}
}
