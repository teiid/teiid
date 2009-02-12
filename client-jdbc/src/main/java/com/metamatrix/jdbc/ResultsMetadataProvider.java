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

package com.metamatrix.jdbc;

import java.sql.SQLException;

/**
 * Interface to provide result set metadata.  
 */
public interface ResultsMetadataProvider {

    /**
     * Get number of columns in the result set metadata
     * @return Column count
     * @throws SQLException
     */
    int getColumnCount() throws SQLException;
    
    /**
     * Get metadata value for the column at columnIndex for a given 
     * metadata property.
     * @param columnIndex The column index
     * @param metadataPropertyKey The metadata property
     * @return The value to return
     * @throws SQLException
     */
    Object getValue(int columnIndex, Integer metadataPropertyKey) throws SQLException;    
    
    /**
     * Get metadata value for the column at columnIndex for a given 
     * metadata property.
     * @param columnIndex The column index
     * @param metadataPropertyKey The metadata property
     * @return The value to return
     * @throws SQLException
     */
    String getStringValue(int columnIndex, Integer metadataPropertyKey) throws SQLException;

    /**
     * Get metadata value for the column at columnIndex for a given 
     * metadata property.
     * @param columnIndex The column index
     * @param metadataPropertyKey The metadata property
     * @return The value to return
     * @throws SQLException
     */
    int getIntValue(int columnIndex, Integer metadataPropertyKey) throws SQLException;

    /**
     * Get metadata value for the column at columnIndex for a given 
     * metadata property.
     * @param columnIndex The column index
     * @param metadataPropertyKey The metadata property
     * @return The value to return
     * @throws SQLException
     */
    boolean getBooleanValue(int columnIndex, Integer metadataPropertyKey) throws SQLException;

    /**
     * Get parameter count from original command
     * @return count
     */
    int getParameterCount() throws SQLException; 
}
