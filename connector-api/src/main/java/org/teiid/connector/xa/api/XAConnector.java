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
 */
package org.teiid.connector.xa.api;

import org.teiid.connector.api.*;


public interface XAConnector extends Connector{
    
    /**
     * Obtain a connection with the connector. The connection must have XAResource in
     * order to participate in distributed transaction. The connection typically is associated
     * with a particular context.   
     * @param context The context of the current user that will be using this connection, 
     * may be null if this connection is for an administrative operation. 
     * @param transactionContext The context of the transaction under which the connection will be used. May be null.
     * @return A Connection, created by the Connector
     * @throws ConnectorException If an error occurred obtaining a connection
     */
    XAConnection getXAConnection( ExecutionContext executionContext, TransactionContext transactionContext) throws ConnectorException;

}
