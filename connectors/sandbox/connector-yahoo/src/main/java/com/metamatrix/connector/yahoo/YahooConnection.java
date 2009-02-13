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

package com.metamatrix.connector.yahoo;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.ResultSetExecution;
import com.metamatrix.connector.basic.BasicConnection;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.language.IQuery;
import com.metamatrix.connector.language.IQueryCommand;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;

/**
 * Serves as a connection for the Yahoo connector.  Since there is no actual
 * connection, this "connection" doesn't really have any state.  
 */
public class YahooConnection extends BasicConnection {

    private ConnectorEnvironment env;

    /**
     * 
     */
    public YahooConnection(ConnectorEnvironment env) {
        this.env = env;
    }

    @Override
    public ResultSetExecution createResultSetExecution(IQueryCommand command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
    	return new YahooExecution((IQuery)command, env, metadata);
    }
    

    /* 
     * @see com.metamatrix.data.Connection#close()
     */
    public void close() {
        // nothing to do
    }

}
