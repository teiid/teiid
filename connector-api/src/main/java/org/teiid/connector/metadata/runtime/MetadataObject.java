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

package org.teiid.connector.metadata.runtime;

import java.util.Properties;

import org.teiid.connector.api.ConnectorException;


/**
 * Represents a runtime metadata object.
 */
public interface MetadataObject {
    
    /**
     * Get name in source for this object, as provided in the model
     * @return Name in source
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     */
    String getNameInSource() throws ConnectorException;

    /**
     * Get any arbitrary properties that are provided on this object.
     * Typically these properties are provided via metamodel extensions.
     * @return Properties
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     */
    Properties getProperties() throws ConnectorException;
    
    /**
     * Get the short name from the metadataID
     * @return String shortName
     */
    String getName();
    
    /**
     * Get the full name from the metadataID
     * @return String fullName
     */
    String getFullName();
}
