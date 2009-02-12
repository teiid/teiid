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

package com.metamatrix.connector.object;

/**
 * Property names used in the text connector.
 */
public class ObjectPropertyNames {
    
    public static final String TRIM_STRINGS = "TrimStrings"; //$NON-NLS-1$
    
    /**
     * This property can be used to specify a limit on the size of Blobs, in bytes, that 
     * will be retrieved from a source.
     * @since 3.0 
     */
    public static final String MAX_BLOB_BYTES = "MaxBlobBytes";    //$NON-NLS-1$

    /**
     * This property can be used to specify a limit on the size of Clobs, in characters, that 
     * will be retrieved from a source.
     * @since 3.0 
     */
    public static final String MAX_CLOB_CHARS= "MaxClobChars"; //$NON-NLS-1$  
    
    /**
     * This is the property name of the ConnectorService property that defines
     * the time zone of the source database.  This property should only be used in 
     * cases where the source database is in a different time zone than the 
     * ConnectorService VM and the database/driver is not already handling 
     * time zones correctly.
     */
    public static final String DATABASE_TIME_ZONE = "DatabaseTimeZone"; //$NON-NLS-1$
    
    
    //***** Extension properties *****//
    /**
     * This property is used to specify the implementation of
     * com.metamatrix.data.ConnectorCapabilities. 
     */
    public static final String EXT_CAPABILITY_CLASS= "CapabilitiesClass"; //$NON-NLS-1$


    /**
     * This property is used to specify the implementation of
     * com.metamatrix.data.pool.SourceConnectionFactory
     */
    public static final String EXT_CONNECTION_FACTORY_CLASS= "ExtensionConnectionFactoryClass"; //$NON-NLS-1$
    
    
    /**
     * This property is used to specify the implementation of
     * com.metamatrix.connector.object.extension.SourceTranslator. 
     */
    public static final String EXT_RESULTS_TRANSLATOR_CLASS= "ExtensionResultsTranslationClass"; //$NON-NLS-1$
    
    
    

}
