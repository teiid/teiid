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

import org.teiid.connector.api.ConnectorException;


/** 
 * @since 4.3
 */
public interface TypeModel {

    /**
     * Nullability constant - indicates column does not allow nulls
     */
    public static final int NOT_NULLABLE = 0;
    /**
     * Searchability constant - indicates column is not searchable (cannot be  evaluated in a comparison).
     */
    public static final int NOT_SEARCHABLE = 0;
    /**
     * Nullability constant - indicates column does allow nulls
     */
    public static final int NULLABLE = 1;
    /**
     * Nullability constant - indicates column may or may not allow nulls
     */
    public static final int NULLABLE_UNKNOWN = 2;
    /**
     * Searchability constant - indicates column can be searched by either a comparison or a LIKE.
     */
    public static final int SEARCHABLE = 3;
    /**
     * Searchability constant - indicates column can be searched with a comparison but not with a LIKE
     */
    public static final int SEARCHABLE_COMPARE = 1;
    /**
     * Searchability constant - indicates column can be searched with a LIKE but not with a comparison
     */
    public static final int SEARCHABLE_LIKE = 2;
    /**
     * Get the expected Java class that should be returned for this element. 
     * @return Data type as Java class
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     */
    Class<?> getJavaType() throws ConnectorException;
    
    /**
     * Get nullability of this column.  
     * @return Code indicating nullability
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     * @see #NOT_NULLABLE
     * @see #NULLABLE
     * @see #NULLABLE_UNKNOWN
     */
    int getNullability() throws ConnectorException;

    /**
     * Get default value of this element.
     * @return Default value, may be null
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     */
    Object getDefaultValue() throws ConnectorException;

    /**
     * Get length of this element or 0 if no length is available.
     * @return Length of this element
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     */
    int getLength() throws ConnectorException;

    /**
     * Get precision of this column.  
     * @return Precision
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     * @since 4.2
     */
    int getPrecision() throws ConnectorException;

    /**
     * Get scale of this column.  
     * @return Scale
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     * @since 4.3.2
     */
    int getScale() throws ConnectorException;
    
    /**
     * Get the design-time model type name.
     * @return Model type name
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     * @since 5.0
     */
    String getModeledType() throws ConnectorException;
    
    /**
     * Get the base type of the design-time model type name.
     * @return Model base type name
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     * @since 5.0
     */
    String getModeledBaseType() throws ConnectorException;

    /**
     * Get the primitive type of the design-time model type name.
     * @return Model primitive type name
     * @throws ConnectorException If an error occurs retrieving the data
     * from runtime metadata
     * @since 5.0
     */
    String getModeledPrimitiveType() throws ConnectorException;

}
