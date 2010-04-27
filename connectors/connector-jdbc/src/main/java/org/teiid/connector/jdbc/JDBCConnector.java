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

package org.teiid.connector.jdbc;

import java.sql.SQLException;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.teiid.connector.api.Connection;
import org.teiid.connector.api.ConnectorCapabilities;
import org.teiid.connector.api.ConnectorEnvironment;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ConnectorLogger;
import org.teiid.connector.basic.BasicConnector;

/**
 * JDBC implementation of Connector interface.
 */
public class JDBCConnector extends BasicConnector {
	
	private JDBCManagedConnectionFactory config;
    private ConnectorCapabilities capabilities;
    
    
	@Override
	public void initialize(ConnectorEnvironment env) throws ConnectorException {
		super.initialize(env);
				
		this.config = (JDBCManagedConnectionFactory)env;
		
		ConnectorLogger logger = config.getLogger();
		
		logger.logInfo(JDBCPlugin.Util.getString("JDBCConnector.JDBCConnector_initialized._1")); //$NON-NLS-1$
		
        capabilities = config.getTranslator().getConnectorCapabilities();
        
        logger.logInfo(JDBCPlugin.Util.getString("JDBCConnector.JDBCConnector_started._4")); //$NON-NLS-1$
    }
    
    
	@Override
    public Connection getConnection() throws ConnectorException {
		DataSource dataSource = getDataSource();
		try {
			// TODO: credential mapping facility is now gone. so, no more re-authenticating user.
			return new JDBCSourceConnection(dataSource.getConnection(), this.config);
		} catch (SQLException e) {
			throw new ConnectorException(e);
		}
    }

	
    @Override
	public ConnectorCapabilities getCapabilities() {
		return capabilities;
	}
	
    protected DataSource getDataSource() throws ConnectorException {
    	String dsName = this.config.getSourceJNDIName(); 
		try {
			InitialContext ic = new InitialContext();
			return (DataSource) ic.lookup(dsName);
		} catch (NamingException e) {
			throw new ConnectorException(e,JDBCPlugin.Util.getString("JDBCSourceConnectionFactory.Unable_to_find_jndi_ds", dsName)); //$NON-NLS-1$
		}
    }
}
