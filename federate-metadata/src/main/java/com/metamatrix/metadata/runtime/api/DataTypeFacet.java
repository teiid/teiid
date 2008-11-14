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

package com.metamatrix.metadata.runtime.api;

/**
 * This class represents Datatype facet defined in XML Schema
 * Part 2: Datatypes.
 */
public interface DataTypeFacet {
	/**
	 * When converted to Properties, this suffox is appended to the end of
	 * the name of the facet to form the key for "isFixed" property.
	 */
	public final String IS_FIXED_SUFFIX = "IsFixed";
	
	/**
	 * When converted to Properties, this suffox is appended to the end of
	 * the name of the facet to form the key for "annotation" property. 
	 */
	public final String ANNOTATION_SUFFIX = "Annotation";
	
	/**
	 * Return the name of the Facet.
	 */
	public String getName();
	
	/**
	 * Return the value of this DataTypeFacet. The actually value can be of
	 * type Boolean, Integer, Float, List(<String>) depending on 
	 * the name of the facet.
	 */
	public Object getValue();

	/**
	 * Return true if the datatypes derived from this datatype can not specify 
	 * another value for this facet. False otherwise.
	 */
	public boolean isFixed();
	
	/**
	 * Return the annotation of this DataTypeFacet. May be null.
	 */
	public String getAnnotation();

}

