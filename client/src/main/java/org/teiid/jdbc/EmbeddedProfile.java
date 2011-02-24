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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.logging.Logger;

import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.net.CommunicationException;
import org.teiid.net.ConnectionException;
import org.teiid.net.ServerConnection;


final class EmbeddedProfile {
    
    private static Logger logger = Logger.getLogger("org.teiid.jdbc"); //$NON-NLS-1$
    
    /**
     * This method tries to make a connection to the given URL. This class
     * will return a null if this is not the right driver to connect to the given URL.
     * @param The URL used to establish a connection.
     * @return Connection object created
     * @throws SQLException if it is unable to establish a connection
     */
    public static Connection connect(String url, Properties info) 
        throws SQLException {
        ConnectionImpl conn = createConnection(url, info);
        logger.fine(JDBCPlugin.Util.getString("JDBCDriver.Connection_sucess")); //$NON-NLS-1$ 
        return conn;
    }
    
    static ConnectionImpl createConnection(String url, Properties info) throws SQLException{
        
        // first validate the properties as this may called from the EmbeddedDataSource
        // and make sure we have all the properties we need.
        validateProperties(info);
        try {
        	ServerConnection sc = (ServerConnection)ReflectionHelper.create("org.teiid.transport.LocalServerConnection", Arrays.asList(info), Thread.currentThread().getContextClassLoader()); //$NON-NLS-1$
			return new ConnectionImpl(sc, info, url);
		} catch (TeiidRuntimeException e) {
			throw TeiidSQLException.create(e);
		} catch (ConnectionException e) {
			throw TeiidSQLException.create(e);
		} catch (CommunicationException e) {
			throw TeiidSQLException.create(e);
		} catch (TeiidException e) {
			throw TeiidSQLException.create(e);
		}
    }
    
    /** 
     * validate some required properties 
     * @param info the connection properties to be validated
     * @throws SQLException
     * @since 4.3
     */
    static void validateProperties(Properties info) throws SQLException {
        // VDB Name has to be there
        String value = null;
        value = info.getProperty(BaseDataSource.VDB_NAME);
        if (value == null || value.trim().length() == 0) {
            String logMsg = JDBCPlugin.Util.getString("MMDataSource.Virtual_database_name_must_be_specified"); //$NON-NLS-1$
            throw new SQLException(logMsg);
        }

    }
    
}
