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

package com.metamatrix.jdbc.api;

import java.sql.SQLException;

/**
 * The MetaMatrix-specific interface for using result set metadata from JDBC.  This 
 * interface provides some additional MetaMatrix-specific functionality that 
 * is not available through the standard JDBC interfaces.  
 */
public interface ResultSetMetaData extends java.sql.ResultSetMetaData {

    /**
     * Get name of the VirtualDatabase at the column index.
     * @param index Column index
     * @return Virtual database name
     * @throws MetadataAccessException if there is an error while trying to access metadata.
     * @throws IllegalArgumentException if column index is invalid
     */
    String getVirtualDatabaseName(int index) throws SQLException;

    /**
     * Get version of the VirtualDatabase at the column index.
     * @param index Column index
     * @return name of the VirtualDatabase.
     * @throws MetadataAccessException if there is an error while trying to access metadata.
     * @throws IllegalArgumentException if column index is invalid
     */
    String getVirtualDatabaseVersion(int index) throws SQLException;


    /**
     * Get the number of parameters in the original command if that command 
     * was a prepared command.
     * @return Number of parameters in original command 
     */
    int getParameterCount() throws SQLException;
}
