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

package com.metamatrix.metadata.runtime.spi.jdbc;

import java.util.Properties;

import com.metamatrix.common.connection.ManagedConnection;
import com.metamatrix.common.connection.ManagedConnectionException;
import com.metamatrix.common.connection.TransactionFactory;
import com.metamatrix.common.connection.TransactionInterface;
import com.metamatrix.common.connection.jdbc.JDBCMgdResourceConnection;

public class JDBCConnectorFactory implements TransactionFactory  {
    /**
     *@link dependency
     * @stereotype instantiate
     */

    /**
     * Create a new instance of a metadata connection.
     * @param env the environment properties for the new connection.
     * @param userName is the name of the user of the transactions
     * @throws ManagedConnectionException if there is an error creating the connection.
     */
    public ManagedConnection createConnection(Properties env, String userName) throws ManagedConnectionException {
        return new JDBCMgdResourceConnection(env, userName);  
    }

    /**
     * Create a new instance of a transaction for a managed connection.
     * @param connection the connection that should be used and that was created using this
     * factory's <code>createConnection</code> method (thus the transaction subclass may cast to the
     * type created by the <code>createConnection</code> method.
     * @param readonly true if the transaction is to be readonly, or false otherwise
     * @throws ManagedConnectionException if there is an error creating the transaction.
     */
    public TransactionInterface createTransaction( ManagedConnection connection, boolean readonly )throws ManagedConnectionException {
        return new JDBCConnector(connection,readonly);
    }
}

