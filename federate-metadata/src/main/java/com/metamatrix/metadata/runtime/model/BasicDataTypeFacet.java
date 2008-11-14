/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.metadata.runtime.model;

import com.metamatrix.metadata.runtime.api.DataTypeFacet;

/**
 * This class represents Datatype facet defined in XML Schema
 * Part 2: Datatypes.
 */
public class BasicDataTypeFacet implements DataTypeFacet {
	private String name;
	private Object value;
	private String annotation;
	private boolean isFixed;
	
	public BasicDataTypeFacet(){
	}
	
	public BasicDataTypeFacet(String name, Object value, boolean isFixed, String annotation){
		this.name = name;
		this.value = value;
		this.isFixed = isFixed;
		this.annotation = annotation;
	}
	 
	/*
	 * @see DataTypeFacet#getName()
	 */
	public String getName() {
		return name;
	}

	/*
	 * @see DataTypeFacet#getValue()
	 */
	public Object getValue() {
		return value;
	}

	/*
	 * @see DataTypeFacet#isFixed()
	 */
	public boolean isFixed() {
		return isFixed;
	}

	/*
	 * @see DataTypeFacet#getAnnotation()
	 */
	public String getAnnotation() {
		return annotation;
	}

	public void setName(String name) {
		this.name = name;
	}
	
	public void setValue(Object value) {
		this.value = value;
	}
	
	public void setIsFixed(boolean isFixed) {
		this.isFixed = isFixed;
	}
	
	public void setAnnotation(String annotation) {
		this.annotation = annotation;
	}
}
