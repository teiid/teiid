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

package org.teiid.jdbc;

import java.sql.SQLException;
import java.util.Properties;

import org.teiid.core.TeiidException;
import org.teiid.net.ServerConnection;
import org.teiid.net.socket.SocketServerConnectionFactory;


/**
 * <p> The java.sql.DriverManager class uses this class to connect to Teiid Server.
 * The TeiidDriver class has a static initializer, which
 * is used to instantiate and register itself with java.sql.DriverManager. The
 * DriverManager's <code>getConnection</code> method calls <code>connect</code>
 * method on available registered drivers. </p>
 */

final class SocketProfile implements ConnectionProfile {
	
    /**
     * This method tries to make a connection to the given URL. This class
     * will return a null if this is not the right driver to connect to the given URL.
     * @param The URL used to establish a connection.
     * @return Connection object created
     * @throws SQLException if it is unable to establish a connection to the server.
     */
    public ConnectionImpl connect(String url, Properties info) throws TeiidSQLException {

        ServerConnection serverConn;
		try {
			serverConn = SocketServerConnectionFactory.getInstance().getConnection(info);
		} catch (TeiidException e) {
			throw TeiidSQLException.create(e);
		}

        // construct a MMConnection object.
        ConnectionImpl connection = new ConnectionImpl(serverConn, info, url);
        return connection;
    }

}
