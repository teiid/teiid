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
package org.teiid.translator.infinispan.hotrod.metadata;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.infinispan.protostream.annotations.ProtoField;
import org.teiid.translator.object.metadata.JavaBeanMetadataProcessor;



/**
 * The AnnotationMetadataProcessor is used when the metadata is derived from annotations in the pojo.
 * 
 * @author vanhalbert
 *
 */
public class AnnotationMetadataProcessor extends JavaBeanMetadataProcessor {

	/**
	 * @param useAnnotations
	 */
	public AnnotationMetadataProcessor(boolean useAnnotations) {
		super(useAnnotations);
		
	}
	
	/**
	 * @param useAnnotations
	 * @param classObjectAsColumn 
	 */
	public AnnotationMetadataProcessor(boolean useAnnotations, boolean classObjectAsColumn) {
		super(useAnnotations, classObjectAsColumn);
		
	}

	/**
	 * {@inheritDoc}
	 *
	 * @see org.teiid.translator.object.metadata.JavaBeanMetadataProcessor#isMethodSearchable(java.lang.Class, java.lang.reflect.Method)
	 */
	@Override
	protected boolean isMethodSearchable(Class<?> entity, Method m) {
		
		ProtoField ax = m.getAnnotation(ProtoField.class);
		
		if (ax != null) {
			return true;
		}
		
		return false;
	}
	

	@Override
	protected boolean isValueRequired(Method m) {
		ProtoField ax = m.getAnnotation(ProtoField.class);
		
		if (ax != null && ax.required()) {
				return true;		
		}
		return false;
	}

	@Override
	protected boolean isFieldSearchable(Class<?> entity, Field f) {
		
		ProtoField ax = f.getAnnotation(ProtoField.class);
		
		if (ax != null) {
			return true;
		}
		
		return false;
	}

	@Override
	protected boolean isValueRequired(Field f) {
		ProtoField ax = f.getAnnotation(ProtoField.class);
		
		if (ax != null && ax.required()) {
			return true;		
		}
		return false;
	}
	
}
