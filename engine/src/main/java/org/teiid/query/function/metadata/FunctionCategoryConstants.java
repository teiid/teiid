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

package org.teiid.query.function.metadata;

/**
 * This interface defines the default set of function category constants.
 */
public interface FunctionCategoryConstants {

    /** 
     * "String" functions typically operate on or otherwise manipulate 
     *  strings, such as concat, substring, etc.
     */
    public static final String STRING = "String"; //$NON-NLS-1$
    
    /** 
     * "Numeric" functions typically operate on or otherwise manipulate 
     *  numbers, such as +, sqrt, etc.
     */    
    public static final String NUMERIC = "Numeric"; //$NON-NLS-1$
    
    /** 
     * "Datetime" functions typically operate on or otherwise manipulate 
     *  dates, times, or timestamps.
     */    
    public static final String DATETIME = "Datetime"; //$NON-NLS-1$
    
    /** 
     * "Conversion" functions convert an object of one type to another type.
     */    
    public static final String CONVERSION = "Conversion"; //$NON-NLS-1$

	/**
	 * "System" functions expose system information.
	 */
	public static final String SYSTEM = "System"; //$NON-NLS-1$
    
    /**
     * "Miscellaneous" functions are for functions that don't fit in any obvious category.
     */
    public static final String MISCELLANEOUS = "Miscellaneous"; //$NON-NLS-1$

    /**
     * "XML" functions are for manipulating XML documents.
     */
    public static final String XML = "XML"; //$NON-NLS-1$

    /**
     * "Security" functions check authentication or authorization information
     */
    public static final String SECURITY = "Security"; //$NON-NLS-1$ 

}
