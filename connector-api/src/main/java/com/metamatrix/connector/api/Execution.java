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

package com.metamatrix.connector.api;

import com.metamatrix.connector.exception.ConnectorException;

/**
 * An execution represents the state and lifecycle for a particular 
 * command execution.  The methods provided on this interface define
 * standard lifecycle methods.  
 * When execution completes, the {@link #close()} will be called.  If 
 * execution must be aborted, due to user or administrator action, the 
 * {@link #cancel()} will be called.
 */
public interface Execution {

    /**
     * Terminates the execution normally.
     */
    void close() throws ConnectorException;
    
    /**
     * Cancels the execution abnormally.  This will happen via
     * a different thread from the one performing the execution, so
     * should be expected to happen in a multi-threaded scenario.
     */
    void cancel() throws ConnectorException;
    
    void execute() throws ConnectorException;
    
    //List<ConnectorException> getWarnings();
}
