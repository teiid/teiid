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
package org.teiid.adminapi.impl;

import java.lang.reflect.Type;
import java.util.Properties;

import org.jboss.metatype.api.types.CompositeMetaType;
import org.jboss.metatype.api.types.MapCompositeMetaType;
import org.jboss.metatype.api.types.MetaType;
import org.jboss.metatype.api.types.SimpleMetaType;
import org.jboss.metatype.api.values.CompositeValue;
import org.jboss.metatype.api.values.CompositeValueSupport;
import org.jboss.metatype.api.values.MapCompositeValueSupport;
import org.jboss.metatype.api.values.MetaValue;
import org.jboss.metatype.api.values.MetaValueFactory;
import org.jboss.metatype.api.values.PropertiesMetaValue;
import org.jboss.metatype.api.values.SimpleValueSupport;
import org.jboss.metatype.plugins.types.MutableCompositeMetaType;
import org.jboss.metatype.spi.values.MetaMapper;

public class ModelMetadataMapper extends MetaMapper<ModelMetaData> {
	private static final MutableCompositeMetaType metaType;
	private static final MetaValueFactory metaValueFactory = MetaValueFactory.getInstance();
	
	static {
		metaType = new MutableCompositeMetaType(ModelMetaData.class.getName(), "The Model meta data");
		metaType.addItem("name", "name", SimpleMetaType.STRING);
		metaType.addItem("visible", "visible", SimpleMetaType.BOOLEAN_PRIMITIVE);
		metaType.addItem("modelType", "modelType", SimpleMetaType.STRING);
		
		
		
		//metaType.addItem("properties", "properties", new MapCompositeMetaType());
		metaType.freeze();
	}
	
	@Override
	public Type mapToType() {
		return ModelMetaData.class;
	}
	
	@Override
	public MetaType getMetaType() {
		return metaType;
	}
	
	@Override
	public MetaValue createMetaValue(MetaType metaType, ModelMetaData object) {
		if (object == null)
			return null;
		if (metaType instanceof CompositeMetaType) {
			CompositeMetaType composite = (CompositeMetaType) metaType;
			CompositeValueSupport request = new CompositeValueSupport(composite);
			
			request.set("modelType", SimpleValueSupport.wrap(object.getModelType().name()));
			request.set("visible", SimpleValueSupport.wrap(object.isVisible()));
			request.set("name",SimpleValueSupport.wrap(object.getName()));
			request.set("properties", new PropertiesMetaValue(object.getProperties()));
			
			return request;
		}
		throw new IllegalArgumentException("Cannot convert request " + object);
	}

	@Override
	public ModelMetaData unwrapMetaValue(MetaValue metaValue) {
		if (metaValue == null)
			return null;

		if (metaValue instanceof CompositeValue) {
			CompositeValue compositeValue = (CompositeValue) metaValue;
			
			ModelMetaData model = new ModelMetaData();
			String type = (String) metaValueFactory.unwrap(compositeValue.get("modelType"));
			if (type != null) {
				model.setModelType(type);
			}
			else {
				model.setModelType("PHYSICAL");	
			}
			
			model.setVisible((Boolean) metaValueFactory.unwrap(compositeValue.get("visible")));
			model.setName((String) metaValueFactory.unwrap(compositeValue.get("name")));
			model.setProperties((Properties) metaValueFactory.unwrap(compositeValue.get("properties")));
			return model;
		}
		throw new IllegalStateException("Unable to unwrap request " + metaValue);
	}

}
