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

package com.metamatrix.jdbc.transport;

import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.api.ServerConnectionFactory;
import com.metamatrix.common.comm.api.ServerConnectionListener;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.dqp.client.ClientSideDQP;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;

public class LocalTransportHandler implements ServerConnectionFactory {
    
    private AtomicInteger SESSION_ID = new AtomicInteger();
    private ConnectionListenerList listenerList = new ConnectionListenerList();
    private ClientSideDQP dqp;
    
    /**
     * Default constructor - used by reflection to create a new instance. 
     */
    public LocalTransportHandler(ClientSideDQP dqp) {
		this.dqp = dqp;
    }
    
	public ServerConnection createConnection(final Properties connectionProperties) throws ConnectionException {        
        return new LocalServerConnection(new MetaMatrixSessionID(SESSION_ID.getAndIncrement()), connectionProperties, dqp, listenerList);
	}

	public synchronized void registerListener(ServerConnectionListener listener) {
		this.listenerList.add(listener);
	}
	
	
	private final class ConnectionListenerList extends ArrayList<ServerConnectionListener> implements ServerConnectionListener{

		@Override
		public void connectionAdded(ServerConnection connection) {
			for (ServerConnectionListener l: this) {
				l.connectionAdded(connection);
			}
		}

		@Override
		public void connectionRemoved(ServerConnection connection) {
			for (ServerConnectionListener l: this) {
				l.connectionRemoved(connection);
			}			
		}
	}
}
