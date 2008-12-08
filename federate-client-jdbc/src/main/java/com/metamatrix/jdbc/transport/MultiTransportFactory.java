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

package com.metamatrix.jdbc.transport;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.metamatrix.api.exception.security.LogonException;
import com.metamatrix.common.api.MMURL;
import com.metamatrix.common.comm.api.ServerConnection;
import com.metamatrix.common.comm.api.ServerConnectionFactory;
import com.metamatrix.common.comm.exception.CommunicationException;
import com.metamatrix.common.comm.exception.ConnectionException;
import com.metamatrix.common.comm.platform.socket.client.SocketServerConnectionFactory;
import com.metamatrix.common.util.MetaMatrixProductNames;
import com.metamatrix.core.MetaMatrixCoreException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.ReflectionHelper;

/**
 * Factory for obtaining a server connection using the appropriate transport,
 * based on properties.
 */
public class MultiTransportFactory {
   
	public static final String LOCAL_TRANSPORT = "Local"; //$NON-NLS-1$
    public static final String SOCKET_TRANSPORT = "Socket"; //$NON-NLS-1$

    // Map of handlers, keyed by the transport name (String -> TransportHandler)
    private Map<String, ServerConnectionFactory> handlers = new HashMap<String, ServerConnectionFactory>();
    
    /**
     * Establish a connection and lazily create a connection factory for the 
     * connection type if necessary. 
     * @throws LogonException 
     * @see com.metamatrix.common.comm.api.ServerConnectionFactory#establishConnection(java.lang.String, java.util.Properties)
     */
    public ServerConnection establishConnection(String transport, Properties connProps) throws ConnectionException, CommunicationException, LogonException {
        ServerConnectionFactory handler = null;
        synchronized(handlers) {
            // Look for existing handler
            handler = handlers.get(transport);
            if (handler == null) {
                handler = createHandler(transport);  
                handlers.put(transport, handler);     
            }
        }
        
        //specific to JDBC
        connProps.setProperty(MMURL.CONNECTION.PRODUCT_NAME, MetaMatrixProductNames.MetaMatrixServer.PRODUCT_NAME);
        
        if (!connProps.containsKey(MMURL.CONNECTION.APP_NAME)) {
        	connProps.setProperty(MMURL.CONNECTION.APP_NAME, "JDBC API"); //$NON-NLS-1$
        }
        
        return handler.createConnection(connProps);
    }
    
    private ServerConnectionFactory createHandler(String transport) {
        if(transport.equals(LOCAL_TRANSPORT)) {
            try {
				return (ServerConnectionFactory)ReflectionHelper.create("com.metamatrix.jdbc.transport.LocalTransportHandler", null, Thread.currentThread().getContextClassLoader()); //$NON-NLS-1$
			} catch (MetaMatrixCoreException e) {
				throw new MetaMatrixRuntimeException(e);
			} 
        } else if(transport.equals(SOCKET_TRANSPORT)) {
            return SocketServerConnectionFactory.getInstance();            
        } else {
            throw new AssertionError("unknown transport"); //$NON-NLS-1$
        }
    }

}
