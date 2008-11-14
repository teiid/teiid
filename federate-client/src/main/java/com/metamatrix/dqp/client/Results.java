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

package com.metamatrix.dqp.client;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;


/**
 * Represents a results returned from the server for a request.  The normal case is to 
 * return a batch of data, but this interface also supports procedure output parameters,
 * update count, etc.  All row and column access is 1-based.
 *  
 * @since 4.3
 */
public interface Results {
    
    public static final int PARAMETER_TYPE_IN = 0;
    public static final int PARAMETER_TYPE_OUT = 1;
    public static final int PARAMETER_TYPE_INOUT = 2;
    public static final int PARAMETER_TYPE_RETURN = 3;
    public static final int PARAMETER_TYPE_RESULTSET = 4;

    /**
     * Returns a flag as to whether this is the last batch in the result set or not. 
     * @return True if end row is last row
     * @since 4.3
     */
    boolean isLast();
    
    /**
     * Get row number (1-based) of first row in batch 
     * @return Row number
     * @since 4.3
     */
    int getBeginRow();
    
    /**
     * Get row number (1-based) of last row in batch 
     * @return Row number
     * @since 4.3
     */
    int getEndRow();
    
    /**
     * Get count of rows in batch (>= 0) 
     * @return Row count
     * @since 4.3
     */
    int getRowCount();
    
    /**
     * Get number of columns in batch 
     * @return Column count
     * @since 4.3
     */
    int getColumnCount();
    
    /**
     * Get value at row and column specified, may be null.   
     * @param row Row index (1-based). beginRow <= row <= endRow
     * @param column Column index (1-based)
     * @return Data value
     * @throws MetaMatrixComponentException If the operation fails due to an internal error
     * @throws MetaMatrixProcessingException If the operation fails due to invalid user input
     * @since 4.3
     */
    Object getValue(int row, int column) throws MetaMatrixComponentException, MetaMatrixProcessingException;
    
    /**
     * Gets the number of parameters for a stored procedure.
     * @return the number of parameters that are part of the stored procedure definition
     * @since 4.3
     */
    int getParameterCount();
    
    /**
     * Gets the parameter type (IN, OUT, INOUT) of a parameter. 
     * @param index the index of the parameter
     * @return the parameter type
     * @since 4.3
     */
    int getParameterType(int index) throws MetaMatrixComponentException, MetaMatrixProcessingException;
    
    /**
     * For stored procedures, get output parameter - this value not be 
     * available until the last batch of a result set returned from this request. 
     * @param index Output parameter index (1-based)
     * @return Output parameter value
     * @throws MetaMatrixComponentException If the operation fails due to an internal error
     * @throws MetaMatrixProcessingException If the operation fails due to invalid user input
     * @since 4.3
     */
    Object getOutputParameter(int index) throws MetaMatrixComponentException, MetaMatrixProcessingException;
    
    /**
     * Gets whether this result represents an update count for an update command 
     * @return
     * @since 4.3
     */
    boolean isUpdate();
    /**
     * Helper method - returns <code>getValue(1, 1);</code>.
     * @return Get update count for updates 
     * @throws MetaMatrixComponentException If the operation fails due to an internal error
     * @since 4.3
     */
    int getUpdateCount() throws MetaMatrixComponentException;
    
    /**
     * Warning that occurred during execution (not cumulative across batches)
     * @return  
     * @since 4.3
     */
    Exception[] getWarnings();
}
