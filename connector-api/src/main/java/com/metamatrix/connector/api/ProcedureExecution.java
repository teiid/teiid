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

package com.metamatrix.connector.api;

import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.IParameter;

/**
 * The procedure execution represents the case where a connector can 
 * execute a procedural call (such as a stored procedure).  This command
 * takes a procedure with input values and executes the procedure.  The 
 * output may include 0 or more output parameters and optionally a result 
 * set.   
 */
public interface ProcedureExecution extends ResultSetExecution {

    /**
     * Get the output parameter value for the given parameter.
     * @param parameter The parameter (either OUT or INOUT direction)
     * @return The value or null if null
     * @throws ConnectorException If an error occurs while retrieving the output value
     */
    Object getOutputValue(IParameter parameter) throws ConnectorException;
    
}
