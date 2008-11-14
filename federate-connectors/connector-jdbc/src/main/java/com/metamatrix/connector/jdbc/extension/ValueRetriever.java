/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.connector.jdbc.extension;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;

import com.metamatrix.data.api.TypeFacility;

/**
 * Specifies how value objects are retrieved from JDBC ResultSet for different 
 * expected output types.  This allows database-specific connectors to use 
 * specialized methods like getClob() rather than generic getObject() methods.  
 */
public interface ValueRetriever {
    
    /**
     * Retrieve the value at <code>columnIndex</code> from the specified <code>results</code>.
     * 
     * @param results The results to retrieve the value from 
     * @param columnIndex The index to use when retrieving the value
     * @param expectedType The MetaMatrix runtime type class that is expected to be returned, as 
     * specified by the select statement of the query
     * @param cal The Calendar to be used for Date, Time, and Timestamp values
     * @return The object that was retrieved
     * @throws SQLException If an error occurred retrieving the value
     */
    Object retrieveValue(ResultSet results, int columnIndex, Class expectedType, int nativeSQLType, Calendar cal, TypeFacility typeFacility) throws SQLException;
        
    /**
     * Retrieve the value at <code>parameterIndex</code> from the callable statement
     * @param results
     * @param parameterIndex
     * @param expectedType
     * @param cal
     * @return
     * @throws SQLException
     */
    Object retrieveValue(CallableStatement results, int parameterIndex, Class expectedType, Calendar cal, TypeFacility typeFacility) throws SQLException;    
}
