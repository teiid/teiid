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
package com.metamatrix.connector.jdbc;

import java.sql.SQLException;
import java.util.Properties;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.ProcedureExecution;
import com.metamatrix.connector.api.ResultSetExecution;
import com.metamatrix.connector.api.UpdateExecution;
import com.metamatrix.connector.basic.BasicConnection;
import com.metamatrix.connector.exception.ConnectorException;
import com.metamatrix.connector.jdbc.extension.ResultsTranslator;
import com.metamatrix.connector.jdbc.extension.SQLTranslator;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.IProcedure;
import com.metamatrix.connector.language.IQueryCommand;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;
import com.metamatrix.connector.pool.PoolAwareConnection;

/**
 * 
 */
public class JDBCSourceConnection extends BasicConnection implements PoolAwareConnection {
    protected java.sql.Connection physicalConnection;
    protected ConnectorEnvironment environment;
    private ConnectorLogger logger;
    private ResultsTranslator resultsTranslator;
    private ConnectionStrategy connectionStrategy;
    private ConnectionListener connectionListener;
	private Class sqlTransClass;
    /**
     * @param connection
     */
    public JDBCSourceConnection(java.sql.Connection connection, ConnectorEnvironment environment, ConnectionStrategy connectionStrategy) throws ConnectorException{
        this(connection, environment, connectionStrategy, null);
    }
    
    public JDBCSourceConnection(java.sql.Connection connection, ConnectorEnvironment environment, ConnectionStrategy connectionStrategy, ConnectionListener connectionListener) throws ConnectorException {
        physicalConnection = connection;
        this.environment = environment;
        this.logger = environment.getLogger();
        this.connectionStrategy = connectionStrategy;
        this.connectionListener = connectionListener;
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();

            Properties connectorProps = environment.getProperties();
            //create SQLTranslator
            String className = connectorProps.getProperty(JDBCPropertyNames.EXT_SQL_TRANSLATOR_CLASS);  
            if(className == null){
                throw new ConnectorException(JDBCPlugin.Util.getString("JDBCSourceConnection.Property_{0}_is_required,_but_not_defined_1", JDBCPropertyNames.EXT_SQL_TRANSLATOR_CLASS)); //$NON-NLS-1$
            }
			sqlTransClass = Thread.currentThread().getContextClassLoader().loadClass(className);
            
            //create ResultsTranslator
            className = connectorProps.getProperty(JDBCPropertyNames.EXT_RESULTS_TRANSLATOR_CLASS);  
            if(className == null){
                throw new ConnectorException(JDBCPlugin.Util.getString("JDBCSourceConnection.Property_{0}_is_required,_but_not_defined_1", JDBCPropertyNames.EXT_RESULTS_TRANSLATOR_CLASS)); //$NON-NLS-1$
            }
            Class resultsTransClass = loader.loadClass(className);
            resultsTranslator = (ResultsTranslator) resultsTransClass.newInstance();
            resultsTranslator.initialize(environment);           
        } catch (ClassNotFoundException e1) {
            throw new ConnectorException(e1);
        } catch (InstantiationException e2) {
            throw new ConnectorException(e2);
        } catch (IllegalAccessException e3) {
            throw new ConnectorException(e3);
        }

        // notify the listner that coneection created
        if (this.connectionListener != null) { 
            this.connectionListener.afterConnectionCreation(this.physicalConnection, this.environment);
        }
    }
    
    private SQLTranslator getSQLTranslator(RuntimeMetadata metadata) throws ConnectorException {
		try {
	        SQLTranslator sqlTranslator = (SQLTranslator) sqlTransClass.newInstance();
	        sqlTranslator.initialize(environment, metadata);
	        return sqlTranslator;
		} catch (InstantiationException e) {
			throw new ConnectorException(e);
		} catch (IllegalAccessException e) {
			throw new ConnectorException(e);
		}
    }

    @Override
    public ResultSetExecution createResultSetExecution(IQueryCommand command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
    	return new JDBCQueryExecution(command, this.physicalConnection, getSQLTranslator(metadata), resultsTranslator, logger, this.environment.getProperties(), executionContext, this.environment);
    }
    
    @Override
    public ProcedureExecution createProcedureExecution(IProcedure command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
    	return new JDBCProcedureExecution(command, this.physicalConnection, getSQLTranslator(metadata), resultsTranslator, logger, this.environment.getProperties(), metadata, executionContext, this.environment);
    }

    @Override
    public UpdateExecution createUpdateExecution(ICommand command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
    	return new JDBCUpdateExecution(command, this.physicalConnection, getSQLTranslator(metadata), resultsTranslator, logger, this.environment.getProperties(), executionContext);    
    }
    
    @Override
    public void close() {
        try {
            // notify the listener that connection being destroyed
            if (connectionListener != null) { 
                connectionListener.beforeConnectionClose(physicalConnection, environment);
            }
            this.physicalConnection.close();
        } catch(SQLException e) {
        	logger.logDetail("Exception during close: " + e.getMessage());
        }
    }

    @Override
    public boolean isAlive() {
        if (connectionStrategy == null) {
            try {
                return !this.physicalConnection.isClosed();
            } catch (SQLException e) {
                return false;
            }
        }
        return connectionStrategy.isConnectionAlive(this.physicalConnection);
    }
    
    @Override
    public void closeCalled() {
    	
    }
    
}
