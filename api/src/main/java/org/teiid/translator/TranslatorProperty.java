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
package org.teiid.translator;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotates a property that can be externally configured.  
 * The property name will be inferred from the method.
 * Keep in mind that TranslatorProprties name are treated as case-insensitive 
 * - do not annotate two methods in the same ExecutionFactory with the same case-insensitive name.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@Documented
public @interface TranslatorProperty {

	public static final String EMPTY_STRING = ""; //$NON-NLS-1$

	/**
	 * Description to be shown in tools
	 * @return
	 */
	String description() default EMPTY_STRING;

	/**
	 * Display name to be shown in tools
	 * @return
	 */
	String display() default EMPTY_STRING;

	/**
	 * True if a non-null value must be supplied
	 * @return
	 */
	boolean required() default false;

	/**
	 * True if this property should be shown in an advanced panel of properties.
	 * @return
	 */
	boolean advanced() default false;
	
	/**
	 * True if this is property should be masked when displayed - this has no effect on how the value is persisted.
	 * @return
	 */
	boolean masked() default false;
}
