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
 * Date: Aug 18, 2003
 * Time: 11:58:05 AM
 */
package org.teiid.connector.metadata.runtime;

import org.teiid.connector.api.ConnectorException;

/**
 * Represents an element, such as a column, in runtime metadata.  
 */
public interface Element extends MetadataObject, TypeModel {

    /**
     * Get position of this element in it's group.
     * @return Position, 0-based
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     */
    int getPosition() throws ConnectorException;
    
    /**
     * Get minimum value
     * @return Minimum value, may be null
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     */
    Object getMinimumValue() throws ConnectorException;
    
    /**
     * Get maximum value
     * @return Maximum value, may be null
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     */
    Object getMaximumValue() throws ConnectorException;
    
    /**
     * Is auto-incremented?
     * @return True if auto-incremented, false otherwise
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     */
    boolean isAutoIncremented() throws ConnectorException;
        
    /**
     * Get searchability of this column.  
     * @return Code indicating searchability
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     * @see #NOT_SEARCHABLE
     * @see #SEARCHABLE
     * @see #SEARCHABLE_COMPARE
     * @see #SEARCHABLE_LIKE
     */
    int getSearchability() throws ConnectorException;
    
    /**
     * Is case sensitive?
     * @return True if case sensitive, false otherwise
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     */
    boolean isCaseSensitive() throws ConnectorException;

    /**
     * Get the native type imported for this column. 
     * @return The native type, may be null
     * @throws ConnectorException If an error occurs retrieving the data
     * @since 4.2
     */
    String getNativeType() throws ConnectorException;
    
    /**
     * Get the Format property
     * @return
     * @throws ConnectorException
     */
    String getFormat() throws ConnectorException; 
    
    /**
     * Get the parent
     * @return Parent
     */
    Group getParent() throws ConnectorException;

}
