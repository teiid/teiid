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

package com.metamatrix.data.api;

import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.language.IParameter;

/**
 * The procedure execution represents the case where a connector can 
 * execute a procedural call (such as a stored procedure).  This command
 * takes a procedure with input values and executes the procedure.  The 
 * output may include 0 or more output parameters and optionally a result 
 * set.   
 */
public interface ProcedureExecution extends Execution, BatchedExecution {

    /**
     * Execute the procedure execution.  Output values will be retrieved
     * via the other methods. 
     * @param procedure The command to execute
     * @throws ConnectorException If an error occurs during execution
     */
    void execute(IProcedure procedure, int maxBatchSize) throws ConnectorException;

    /**
     * Get the output parameter value for the given parameter.
     * @param parameter The parameter (either OUT or INOUT direction)
     * @return The value or null if null
     * @throws ConnectorException If an error occurs while retrieving the output value
     */
    Object getOutputValue(IParameter parameter) throws ConnectorException;
        
    
}
