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
package org.teiid.templates;

import java.util.ArrayList;
import java.util.List;

import org.jboss.beans.info.spi.BeanInfo;
import org.jboss.beans.info.spi.PropertyInfo;
import org.jboss.managed.api.Fields;
import org.jboss.managed.api.ManagedProperty;
import org.jboss.managed.api.factory.ManagedObjectFactory;
import org.jboss.managed.plugins.factory.AbstractInstanceClassFactory;
import org.jboss.metadata.spi.MetaData;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.MapCompositeValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.teiid.adminapi.impl.PropertyMetadata;
import org.teiid.adminapi.impl.TranslatorMetaData;

/**
 * This class is defined as bean in the deployer's -jboss-beans.xml file. This used to construct the 
 * TranslatorMetadata object with "translator-property"
 */
public class TranslatorMetadataICF extends	AbstractInstanceClassFactory<TranslatorMetaData> {

	private static final String TRANSLATOR_PROPERTY = "translator-property"; //$NON-NLS-1$

	public TranslatorMetadataICF(ManagedObjectFactory mof) {
		super(mof);
	}

	@Override
	public MetaValue getValue(BeanInfo beanInfo, ManagedProperty property, MetaData metaData, TranslatorMetaData attachment) {
		// Get the property name
		String propertyName = property.getMappedName();
		if (propertyName == null)
			propertyName = property.getName();

		// Get the property info
		PropertyInfo propertyInfo = property.getField(Fields.PROPERTY_INFO, PropertyInfo.class);
		if (propertyInfo == null)
			propertyInfo = beanInfo.getProperty(propertyName);

		// Check if the property is readable
		if (propertyInfo != null && propertyInfo.isReadable() == false)
			return null;

		MetaValue value = null;
		if (TRANSLATOR_PROPERTY.equals(property.getName())) { 
			MapCompositeValueSupport mapValue = new MapCompositeValueSupport(SimpleMetaType.STRING);
			List<PropertyMetadata> list = attachment.getJAXBProperties();
			if (list != null) {
				for (PropertyMetadata prop : list) {
					String name = prop.getName();
					MetaValue svalue = SimpleValueSupport.wrap(prop.getValue());
					mapValue.put(name, svalue);
				}
			}
			value = mapValue;
		} else {
			value = super.getValue(beanInfo, property, metaData, attachment);
		}
		return value;
	}

	@Override
	protected Object unwrapValue(BeanInfo beanInfo, ManagedProperty property, MetaValue value) {
		Object unwrapValue = null;
		if (TRANSLATOR_PROPERTY.equals(property.getName())) { 
			
			if ((value instanceof MapCompositeValueSupport) == false) {
				return super.unwrapValue(beanInfo, property, value);
			}

			MapCompositeValueSupport mapValue = (MapCompositeValueSupport) value;

			List<PropertyMetadata> list = new ArrayList<PropertyMetadata>();
			for (String name : mapValue.getMetaType().keySet()) {
				list.add(new PropertyMetadata(name, (String) getMetaValueFactory().unwrap(mapValue.get(name))));
			}
			unwrapValue = list;
		} else {
			unwrapValue = super.unwrapValue(beanInfo, property, value);
		}
		return unwrapValue;
	}

	@Override
	public Class<TranslatorMetaData> getType() {
		return TranslatorMetaData.class;
	}
}
