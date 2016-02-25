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
import org.teiid.core.TeiidRuntimeException;
import org.teiid.net.ServerConnection;


public class LocalProfile implements ConnectionProfile {
	
    public static final String USE_CALLING_THREAD = "useCallingThread"; //$NON-NLS-1$
	public static final String WAIT_FOR_LOAD = "waitForLoad"; //$NON-NLS-1$
	public static final String TRANSPORT_NAME = "transportName"; //$NON-NLS-1$
	public static final Object DQP_WORK_CONTEXT = "dqpWorkContext"; //$NON-NLS-1$

	/**
     * This method tries to make a connection to the given URL. This class
     * will return a null if this is not the right driver to connect to the given URL.
     * @param The URL used to establish a connection.
     * @return Connection object created
     * @throws SQLException if it is unable to establish a connection
     */
    public ConnectionImpl connect(String url, Properties info) 
        throws TeiidSQLException {
        try {
        	ServerConnection sc = createServerConnection(info);
			return new ConnectionImpl(sc, info, url);
		} catch (TeiidRuntimeException e) {
			throw TeiidSQLException.create(e);
		} catch (TeiidException e) {
			throw TeiidSQLException.create(e);
		}
    }

	public ServerConnection createServerConnection(Properties info) throws TeiidException {
		return ModuleHelper.createFromModule(info);
	}

}
