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

/*
 */
package com.metamatrix.connector.xa.api;

import com.metamatrix.connector.api.*;
import com.metamatrix.connector.exception.ConnectorException;


public interface XAConnector extends Connector{
    
    /**
     * Obtain a connection with the connector. The connection must have XAResource in
     * order to participate in distributed transaction. The connection typically is associated
     * with a particular security context. The connection is assumed to be pooled in 
     * the underlying source if pooling is necessary - the connection will be closed 
     * when execution has completed against it.  
     * @param executionContext The context of the current MetaMatrix user that will be using this connection
     * @param transactionContext The context of the transaction under which the connection will be used. May be null.
     * @return A Connection, created by the Connector
     * @throws ConnectorException If an error occurred obtaining a connection
     */
    XAConnection getXAConnection( ExecutionContext executionContext, TransactionContext transactionContext) throws ConnectorException;

}
