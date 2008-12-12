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
package com.metamatrix.connector.jdbc.xa;

import java.sql.Connection;
import java.sql.SQLException;

import javax.transaction.xa.XAResource;

import com.metamatrix.common.util.exception.SQLExceptionUnroller;
import com.metamatrix.connector.jdbc.ConnectionListener;
import com.metamatrix.connector.jdbc.ConnectionStrategy;
import com.metamatrix.connector.jdbc.JDBCSourceConnection;
import com.metamatrix.data.api.ConnectorEnvironment;
import com.metamatrix.data.exception.ConnectorException;
import com.metamatrix.data.xa.api.XAConnection;

public class JDBCSourceXAConnection extends JDBCSourceConnection implements XAConnection {
    private javax.sql.XAConnection xaConn;
    private XAResource resource;
    private boolean isInTxn;
    private int leaseCount=0;
    
    public JDBCSourceXAConnection(Connection conn, javax.sql.XAConnection xaConn, ConnectorEnvironment environment, ConnectionStrategy connectionStrategy, ConnectionListener connectionListener) throws ConnectorException, SQLException {       
        super(conn, environment, connectionStrategy, connectionListener);
        this.xaConn = xaConn;
    }
    
    /**
     * @see com.metamatrix.data.xa.api.XAConnection#getXAResource()
     */
    public synchronized XAResource getXAResource() throws ConnectorException {
        try {
            if (resource == null) {
                this.resource = xaConn.getXAResource();
            }                
        } catch (SQLException err) {
            throw new ConnectorException(err);
        }
        return resource;
    }
    
    void setInTxn(boolean isInTxn) {
        this.isInTxn = isInTxn;
    }
    
    public synchronized void release() {
        this.setLeased(false);
        if (!isLeased() && !isInTxn) {
//            if () {
//                this.environment.getLogger().logWarning(JDBCPlugin.Util.getString("JDBCSourceXAConnecton:Connection_still_leased")); //$NON-NLS-1$
//            }
            try {
                closeSource();
            } catch (ConnectorException e) {
                this.environment.getLogger().logWarning(e.getMessage());
            }
            this.physicalConnection = null;
            super.release();
        }
    }
    
    public synchronized boolean isLeased() {
        return this.leaseCount > 0;
    }    
    
    public synchronized void setLeased(boolean leased) {
        if (leased) {
            this.leaseCount++;
        }
        else {
            this.leaseCount--;
        }
    }

    /** 
     * @see com.metamatrix.connector.jdbc.JDBCSourceConnection#getPhysicalConnection()
     */
    protected Connection getPhysicalConnection() throws ConnectorException {
        if (this.physicalConnection != null) {
            return this.physicalConnection;
        }
        try {
            this.physicalConnection = xaConn.getConnection();
        } catch (SQLException err) {
            throw new ConnectorException(err);
        }
        return this.physicalConnection;
    }
}
