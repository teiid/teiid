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
package org.teiid.resource.cci;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;


@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface TranslatorProperty {

	public static final String EMPTY_STRING = ""; //$NON-NLS-1$
	public static final String GENERATED = "%GENERATED%"; //$NON-NLS-1$

	// name of the property
	String name() default GENERATED;
	
	// type of the property
	Class type() default java.lang.String.class;

	// description
	String description() default GENERATED;

	// display name to be used in tools
	String display() default GENERATED;

	// is this mandatory property
	boolean required() default false;

	// is it modifiable
	boolean readOnly() default false;
	
	// is advanced?
	boolean advanced() default false;
	
	// should mask the values of this property in the tools
	boolean masked() default false;
	
	// if this represents a enum what are the legal values?
	String[] allowed() default {};
	
	// what is the default in the string form
	String defaultValue() default EMPTY_STRING;
}
