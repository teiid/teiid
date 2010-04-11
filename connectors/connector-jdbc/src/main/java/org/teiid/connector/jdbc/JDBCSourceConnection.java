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

import java.sql.SQLException;

import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.api.ExecutionContext;
import org.teiid.connector.api.MetadataProvider;
import org.teiid.connector.api.ProcedureExecution;
import org.teiid.connector.api.ResultSetExecution;
import org.teiid.connector.api.UpdateExecution;
import org.teiid.connector.basic.BasicConnection;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.language.Call;
import org.teiid.connector.language.Command;
import org.teiid.connector.language.QueryExpression;
import org.teiid.connector.metadata.runtime.MetadataFactory;
import org.teiid.connector.metadata.runtime.RuntimeMetadata;

import com.metamatrix.common.util.PropertiesUtils;


/**
 * 
 */
public class JDBCSourceConnection extends BasicConnection implements MetadataProvider{
    protected java.sql.Connection physicalConnection;
    protected JDBCManagedConnectionFactory environment;
    private Translator sqlTranslator;

    public JDBCSourceConnection(java.sql.Connection connection, JDBCManagedConnectionFactory environment) throws ConnectorException {
        this.physicalConnection = connection;
        this.environment = environment;
        this.sqlTranslator = environment.getTranslator();
        this.sqlTranslator.afterConnectionCreation(connection);
    }
    
    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
    	return new JDBCQueryExecution(command, this.physicalConnection, executionContext, this.environment);
    }
    
    @Override
    public ProcedureExecution createProcedureExecution(Call command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
    	return new JDBCProcedureExecution(command, this.physicalConnection, executionContext, this.environment);
    }

    @Override
    public UpdateExecution createUpdateExecution(Command command,
    		ExecutionContext executionContext, RuntimeMetadata metadata)
    		throws ConnectorException {
    	return new JDBCUpdateExecution(command, this.physicalConnection, executionContext, this.environment);    
    }
    
    @Override
    public void close() {
		closeSourceConnection();
	}

	protected void closeSourceConnection() {
		try {
            this.physicalConnection.close();
        } catch(SQLException e) {
        	environment.getLogger().logDetail("Exception during close: " + e.getMessage()); //$NON-NLS-1$
        }
	}

	@Override
	public void getConnectorMetadata(MetadataFactory metadataFactory) throws ConnectorException {
		try {
			JDBCMetdataProcessor metadataProcessor = new JDBCMetdataProcessor(this.environment.getLogger());
			PropertiesUtils.setBeanProperties(metadataProcessor, metadataFactory.getImportProperties(), "importer"); //$NON-NLS-1$
			metadataProcessor.getConnectorMetadata(this.physicalConnection, metadataFactory);
		} catch (SQLException e) {
			throw new ConnectorException(e);
		}
	}	
}
