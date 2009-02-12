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

/*
 */
package com.metamatrix.connector.object.extension;

import java.util.List;
import java.util.TimeZone;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.TypeFacility;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;

/**
 * Specify source-specific behavior for translating results.
 */
public interface ISourceTranslator {
    
    /**
     * Initialize the results translator with the connector's environment, which
     * can be used to retrieve configuration parameters
     * @param env The connector environment
     * @throws ConnectorException If an error occurs during initialization
     */
    void initialize(ConnectorEnvironment env) throws ConnectorException; 

      /**
     * Determine the time zone the database is located in.  Typically, this time zone is 
     * the same as the local time zone, in which case, null should be returned.
     * @return Database time zone
     */
    TimeZone getDatabaseTimezone();
    
    /**
     * Get a list of ValueTranslator objects that specify database-specific value
     * translation logic.  By default, the JDBC connector has a large set of available
     * translator.
     * @return List of ValueTranslator
     */
    List getValueTranslators();

    /**
     * Used to specify a special value retriever.  By default, the BasicValueRetriever
     * will be used to retrieve objects via the getObject() method. 
     * @return The specialized ValueRetriever
     */
    IValueRetriever getValueRetriever();      
    
    
    /** 
     * Called to create the ObjectPrcoedure used to translate the command 
     * and provide related method information for execution and value translation.
     *  
     * @return
     * @since 4.3
     */
    IObjectCommand createObjectCommand(RuntimeMetadata metadata, ICommand command) throws ConnectorException;
    
    TypeFacility getTypeFacility();
 }
