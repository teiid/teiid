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

package com.metamatrix.connector.metadata;


/** 
 * Constants used in MetadataConnector, some of the values for these constants
 * are used in NameInSource of the SystemPhysical model. 
 * @since 4.3
 */
public class MetadataConnectorConstants {
		
    /**
     * Extension for property files containing enumeration of property values,
     * some of the properties in the files are like standard ModelTypes, Search types etc.
     * This is used in nameinsource for a table which would get populated with the values
     * from the property file.
     */
    public static final String PROPERTIES_FILE_EXTENSION = ".properties"; //$NON-NLS-1$
    
    /**
     * Charcter used in NameInSource of a table, the table is be populated by object resulting
     * from calling the method enclosed in () charcters on the metadata record for the table.  
     */
    public static final char START_METHOD_NAME_CHAR = '(';
    
    /**
     * Charcter used in NameInSource of a table, the table is be populated by object resulting
     * from calling the method enclosed in () charcters on the metadata record for the table.  
     */
    public static final char END_METHOD_NAME_CHAR = ')';
    
    /**
     * Charcter used to seperate method names used in NameInSource, if multiple
     * methods need to be invoked to arrive at value for a column of object from the table,
     * each method to be invoked on the resulting object is seperated by this charachter.
     */
    public static final char METHOD_DELIMITER = '.';

    /**
     * Charcter used in NameInSource of a table to seperate the index file and the record type
     * charchter. This may be necessary if a given index file has multiple types of records.
     */
    public static final char RECORD_TYPE_SEPERATOR = '#';

    /**
     * The prefix to the NameInsource for column/parameter to arrive at a method name
     * to be invoked on a MetadataRecord to get the value used to populate the column/parameter. 
     */
    public static final String GET_METHOD_PREFIX = "get"; //$NON-NLS-1$
    
    
    /**
     * The prefix to the NameInSource for column/parameter to arrive at a method name to be invoked
     * on a MetadataRecord to set the value used for some computation using the record.
     */
    public static final String SET_METHOD_PREFIX = "set";     //$NON-NLS-1$

}