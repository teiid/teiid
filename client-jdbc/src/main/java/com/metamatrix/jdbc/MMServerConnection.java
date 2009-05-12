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

package com.metamatrix.jdbc;

import java.sql.SQLException;
import java.util.Properties;

import com.metamatrix.admin.api.core.Admin;
import com.metamatrix.admin.api.server.ServerAdmin;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.platform.client.ServerAdminFactory;
import com.metamatrix.common.comm.platform.socket.client.SocketServerConnection;

/** 
 * @since 4.3
 */
public class MMServerConnection extends MMConnection {

    // constant value giving product name
    private final static String SERVER_NAME = "MetaMatrix Enterprise"; //$NON-NLS-1$
    
    private ServerAdmin serverAdmin;
    
    /**
     * Factory Constructor 
     * @param serverConn
     * @param info
     * @param url
     */
    public static MMServerConnection newInstance(ServerConnection serverConn, Properties info, String url) {
        return new MMServerConnection(serverConn, info, url);        
    }
    
    /** 
     * @param driver
     * @param serverConn
     * @param info
     * @param url
     * @param tracker
     * @since 4.3
     */
    public MMServerConnection(ServerConnection serverConn, Properties info, String url) {
        super(serverConn, info, url);
    }

    /** 
     * @see com.metamatrix.jdbc.api.Connection#getAdminAPI()
     * @since 4.3
     */
    public synchronized Admin getAdminAPI() throws SQLException {
        try {
            if (serverAdmin == null) {
                ServerAdminFactory factory = ServerAdminFactory.getInstance();
                serverAdmin = factory.createAdmin(this.propInfo);
            }
        } catch(Exception e) {
        	throw MMSQLException.create(e);
		} 
        return serverAdmin;
    }

    /** 
     * @see com.metamatrix.jdbc.MMConnection#close()
     * @since 4.3
     */
    public synchronized void close() throws SQLException {
        super.close();
        if (serverAdmin != null) {
            serverAdmin.close();
        }
    }
    
    /** 
     * @see com.metamatrix.jdbc.MMConnection#getDatabaseName()
     */
    public String getDatabaseName() {
        return SERVER_NAME;
    }

	@Override
	public BaseDriver getBaseDriver() {
		return new MMDriver();
	}
	
	@Override
	public void recycleConnection() {
		super.recycleConnection();
		//perform load balancing
		if (this.serverConn instanceof SocketServerConnection) {
			SocketServerConnection.selectNewServerInstance(this.getDQP());
		}
	}

	@Override
	public boolean isSameProcess(MMConnection conn) throws CommunicationException {
		if (conn instanceof MMServerConnection
				&& this.serverConn instanceof SocketServerConnection
				&& conn.serverConn instanceof SocketServerConnection) {
			return SocketServerConnection.isSameInstance(this.getDQP(), conn.getDQP());
		}
		return false;
	}
}
