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

package com.metamatrix.connector.text;

/**
 * Property names used in the text connector.
 */
public class TextPropertyNames {
    public static final String DESCRIPTOR_FILE = "DescriptorFile"; //$NON-NLS-1$
    public static final String PARTIAL_STARTUP_ALLOWED = "PartialStartupAllowed"; //$NON-NLS-1$
    public static final String LOCATION = "LOCATION"; //$NON-NLS-1$
    public static final String FILE_LOCATION = "FILE"; //$NON-NLS-1$
    public static final String URL_LOCATION = "URL"; //$NON-NLS-1$
    public static final String DELIMITER  = "DELIMITER"; //$NON-NLS-1$
    public static final String QUALIFIER  = "QUALIFIER"; //$NON-NLS-1$
    public static final String HEADER_LINES  = "SKIPHEADERLINES"; //$NON-NLS-1$
    public static final String COLUMNS  = "COLUMNS"; //$NON-NLS-1$
    public static final String TYPES  = "TYPES"; //$NON-NLS-1$
    
    /**
     * If HEADER_LINES is non-zero this property defines which row of HEADER_LINES 
     * can be used as the header row.  The value is 1-based and must be < HEADER_LINES.
     * @since 5.0.3
     */
    public static final String HEADER_ROW  = "HEADERLINE"; //$NON-NLS-1$
    /**
    * This property can be used to specify specific date formats for String
    * type results that need to be converted into java.util.Date objects for 
    * joins or comparisons.  This property will be a String of formats
    * seperated by the DATE_RESULT_FORMATS_DELIMTER property value.  No spaces
    * are allowed between these property values and their delimiters.
    */
    public static final String DATE_RESULT_FORMATS = "DateResultFormats"; //$NON-NLS-1$
    
    /**
    * This is the delimiter for the String property value of the DATE_RESULT_FORMATS
    * property.
    */
    public static final String DATE_RESULT_FORMATS_DELIMITER = "DateResultFormatsDelimiter"; //$NON-NLS-1$

    
    /**
     * This property can be used to force the edit that ensures the number of
     * columns in the text file matches the number of columns modeled.
     */
    public static final String COLUMN_CNT_MUST_MATCH_MODEL  = "EnforceColumnCount"; //$NON-NLS-1$

}
