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

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlElement;

import org.jboss.managed.api.annotation.ManagementObject;
import org.jboss.managed.api.annotation.ManagementObjectID;
import org.jboss.managed.api.annotation.ManagementProperties;
import org.jboss.managed.api.annotation.ManagementProperty;
import org.jboss.managed.api.annotation.ManagementPropertyFactory;
import org.teiid.adminapi.Translator;


@XmlAccessorType(XmlAccessType.NONE)
@ManagementObject(properties=ManagementProperties.EXPLICIT)
public class VDBTranslatorMetaData extends AdminObjectImpl implements Translator {
	private static final long serialVersionUID = -3454161477587996138L;
	private String type;
	private Class<?> executionClass;
	private String description;
	
	@Override
	@ManagementProperty(description="Name of the Translator", mandatory = true)
	@ManagementObjectID(type="translator")
	public String getName() {
		return super.getName();
	}	
	
	@XmlAttribute(name = "name", required = true)
	public void setName(String name) {
		super.setName(name);
	}
	
	@Override
	@ManagementProperty(description="Base type of Translator", mandatory = true)
	public String getType() {
		return type;
	}
	
	@XmlAttribute(name = "type",required = true)
	public void setType(String type) {
		this.type = type;
	}	
	
	@Override
	@XmlElement(name = "property", type = PropertyMetadata.class)
	@ManagementProperty(name="property", description = "Translator Properties", managed=true)
	@ManagementPropertyFactory(TranslatorPropertyFactory.class)
	public List<PropertyMetadata> getJAXBProperties(){
		return super.getJAXBProperties();
	}	
	
	public String toString() {
		return getName();
	}

	public Class<?> getExecutionFactoryClass() {
		return this.executionClass;
	}	
	
	public void setExecutionFactoryClass(Class<?> clazz) {
		this.executionClass = clazz;
		addProperty(EXECUTION_FACTORY_CLASS, clazz.getName());
	}
	
	@ManagementProperty(description="Translator Description")
	public String getDescription() {
		return this.description;
	}
	
	@XmlAttribute(name = "description")
	public void setDescription(String desc) {
		this.description = desc;
	}
}
