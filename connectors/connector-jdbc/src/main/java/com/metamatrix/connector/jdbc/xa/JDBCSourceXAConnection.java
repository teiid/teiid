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
package com.metamatrix.connector.jdbc.xa;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.transaction.xa.XAResource;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.jdbc.ConnectionListener;
import com.metamatrix.connector.jdbc.ConnectionStrategy;
import com.metamatrix.connector.jdbc.JDBCSourceConnection;
import com.metamatrix.connector.xa.api.XAConnection;

public class JDBCSourceXAConnection extends JDBCSourceConnection implements XAConnection {
    private javax.sql.XAConnection xaConn;
    private XAResource resource;
    private boolean errorOccurred;
    
    public JDBCSourceXAConnection(Connection conn, javax.sql.XAConnection xaConn, ConnectorEnvironment environment, ConnectionStrategy connectionStrategy, ConnectionListener connectionListener) throws ConnectorException, SQLException {       
        super(conn, environment, connectionStrategy, connectionListener);
        this.xaConn = xaConn;
        this.xaConn.addConnectionEventListener(new ConnectionEventListener() {
        	@Override
        	public void connectionClosed(ConnectionEvent event) {
        		
        	}
        	@Override
        	public void connectionErrorOccurred(ConnectionEvent event) {
        		errorOccurred = true;
        	}
        });
        this.resource = xaConn.getXAResource();
    }
    
    /**
     * @see com.metamatrix.connector.xa.api.XAConnection#getXAResource()
     */
    public XAResource getXAResource() throws ConnectorException {
        return resource;
    }
    
    @Override
    public void close() {
        super.close();
        
        try {
			this.xaConn.close();
		} catch (SQLException e) {
			this.environment.getLogger().logDetail("Exception while closing: " + e.getMessage());
		}
    }
    
    /**
     * XAConnection Connections should be cycled to ensure proper cleanup after the transaction.
     */
    @Override
    public void closeCalled() {
    	super.closeCalled();
    	try {
			this.physicalConnection = this.xaConn.getConnection();
		} catch (SQLException e) {
			this.environment.getLogger().logDetail("Exception while cycling connection: " + e.getMessage());
		}
    }
    
    @Override
    public boolean isAlive() {
    	if (errorOccurred) {
    		return false;
    	}
    	return super.isAlive();
    }
    
}
