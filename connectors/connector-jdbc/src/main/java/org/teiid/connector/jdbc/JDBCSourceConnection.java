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
package org.teiid.connector.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.teiid.connector.jdbc.translator.Translator;

import com.metamatrix.connector.api.ConnectorEnvironment;
import com.metamatrix.connector.api.ConnectorException;
import com.metamatrix.connector.api.ConnectorLogger;
import com.metamatrix.connector.api.ExecutionContext;
import com.metamatrix.connector.api.ProcedureExecution;
import com.metamatrix.connector.api.ResultSetExecution;
import com.metamatrix.connector.api.UpdateExecution;
import com.metamatrix.connector.language.ICommand;
import com.metamatrix.connector.language.IProcedure;
import com.metamatrix.connector.language.IQueryCommand;
import com.metamatrix.connector.metadata.runtime.RuntimeMetadata;

/**
 * 
 */
public class JDBCSourceConnection extends com.metamatrix.connector.basic.BasicConnection {
    protected java.sql.Connection physicalConnection;
    protected ConnectorEnvironment environment;
    private ConnectorLogger logger;
    private Translator sqlTranslator;

    public JDBCSourceConnection(java.sql.Connection connection, ConnectorEnvironment environment, Translator sqlTranslator) throws ConnectorException {
        this.physicalConnection = connection;
        this.environment = environment;
        this.logger = environment.getLogger();
        this.sqlTranslator = sqlTranslator;
        this.sqlTranslator.afterConnectionCreation(connection);
    }
    
    @Override
    public ResultSetExecution createResultSetExecution(IQueryCommand command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
    	return new JDBCQueryExecution(command, this.physicalConnection, sqlTranslator, logger, this.environment.getProperties(), executionContext, this.environment);
    }
    
    @Override
    public ProcedureExecution createProcedureExecution(IProcedure command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
    	return new JDBCProcedureExecution(command, this.physicalConnection, sqlTranslator, logger, this.environment.getProperties(), metadata, executionContext, this.environment);
    }

    @Override
    public UpdateExecution createUpdateExecution(ICommand command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
    	return new JDBCUpdateExecution(command, this.physicalConnection, sqlTranslator, logger, this.environment.getProperties(), executionContext);    
    }
    
    @Override
    public void close() {
		closeSourceConnection();
	}

	protected void closeSourceConnection() {
		try {
            this.physicalConnection.close();
        } catch(SQLException e) {
        	logger.logDetail("Exception during close: " + e.getMessage());
        }
	}

    @Override
    public boolean isAlive() {
    	Connection connection = this.physicalConnection;
        Statement statement = null;
    	try {
    		int timeout = this.sqlTranslator.getIsValidTimeout();
    		if (timeout >= 0) {
    			return connection.isValid(timeout);
    		}
            if(connection.isClosed()){
                return false;
            } 
            String connectionTestQuery = sqlTranslator.getConnectionTestQuery();
            if (connectionTestQuery != null) {
		        statement = connection.createStatement();
		        statement.executeQuery(connectionTestQuery);
            }
        } catch(SQLException e) {
        	return false;
        } finally {
            if ( statement != null ) {
                try {
                    statement.close();
                } catch ( SQLException e ) {
                }
            }
        }
        return true;
    }
    
    @Override
    public void closeCalled() {
    	
    }
    
}
