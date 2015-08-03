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

import org.teiid.adminapi.Translator;


public class VDBTranslatorMetaData extends AdminObjectImpl implements Translator {
	private static final long serialVersionUID = -3454161477587996138L;
	private String type;
	private Class<?> executionClass;
	private String description;
	private String moduleName;
	private transient VDBTranslatorMetaData parent;
	
	@Override
	public String getName() {
		return super.getName();
	}	
	
	public void setName(String name) {
		super.setName(name);
	}
	
	@Override
	public String getType() {
		return type;
	}
	
	public void setType(String type) {
		this.type = type;
	}	
	
	public String toString() {
		return getName();
	}

	public Class<?> getExecutionFactoryClass() {
		if (this.executionClass == null && this.parent != null) {
			return this.parent.getExecutionFactoryClass();
		}
		return this.executionClass;
	}	
	
	public void setExecutionFactoryClass(Class<?> clazz) {
		this.executionClass = clazz;
		addProperty(EXECUTION_FACTORY_CLASS, clazz.getName());
	}
	
	public String getDescription() {
		return this.description;
	}
	
	public void setDescription(String desc) {
		this.description = desc;
	}
	
	public String getModuleName() {
		return this.moduleName;
	}
	
	public void setModuleName(String name) {
		this.moduleName = name;
	}

	public void setParent(VDBTranslatorMetaData parent) {
		this.parent = parent;
	}
	
	public VDBTranslatorMetaData getParent() {
		return parent;
	}
}
