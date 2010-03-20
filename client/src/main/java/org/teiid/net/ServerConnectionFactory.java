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

package org.teiid.net;

import java.util.Properties;


public interface ServerConnectionFactory {
	
    /**
     * Establish a connection to the server.  
     * @param connProps The properties used by the transport to find a connection.  These 
     * properties are typically specific to the transport.
     * @return A connection, never null
     * @throws ConnectionException If an error occurs communicating between client and server
     * @throws CommunicationException If an error occurs in connecting, typically due to 
     * problems with the connection properties (bad user name, bad password, bad host name, etc)
     */
	ServerConnection getConnection(Properties connectionProperties) throws CommunicationException, ConnectionException;
}
