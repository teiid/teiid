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

package org.teiid.translator.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.teiid.core.TeiidException;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.ReflectionHelper;
import org.teiid.core.util.StringUtil;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.BasicExecutionFactory;
import org.teiid.translator.ConnectorCapabilities;
import org.teiid.translator.ConnectorException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.MetadataProvider;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorProperty;
import org.teiid.translator.UpdateExecution;


/**
 * JDBC implementation of Connector interface.
 */
public class JDBCExecutionFactory extends BasicExecutionFactory implements MetadataProvider {
	
    private ConnectorCapabilities capabilities;
    private Translator sqlTranslator;
	private boolean useBindVariables;
	private String databaseTimeZone;
	private String extensionTranslationClassName;
	private boolean trimStrings;
	private boolean useCommentsInSourceQuery;
	private int fetchSize;
    
	@Override
	public void start() throws ConnectorException {
		super.start();
        capabilities = getTranslator().getConnectorCapabilities();
    }
	
	public Translator getTranslator() throws ConnectorException {
		if (this.sqlTranslator == null) {
	        try {
	        	String className = getExtensionTranslationClassName();
	        	if (!StringUtil.isValid(className)) {
	        		this.sqlTranslator = new Translator();
	        	} else { 
	        		this.sqlTranslator = (Translator)ReflectionHelper.create(className, null, Thread.currentThread().getContextClassLoader());
	        	}
	            sqlTranslator.initialize(this);
	        } catch (TeiidException e) {
	            throw new ConnectorException(e);
			}
		}
		return this.sqlTranslator;
	}	
    
    @Override
	public ConnectorCapabilities getCapabilities() {
		return capabilities;
	}
    
	@TranslatorProperty(name="UseBindVariables", display="Use Bind Variables", description="Use prepared statements and bind variables",advanced=true, defaultValue="false")
	public boolean isUseBindVariables() {
		return this.useBindVariables;
	}

	public void setUseBindVariables(boolean useBindVariables) {
		this.useBindVariables = useBindVariables;
	}

	@TranslatorProperty(name="DatabaseTimeZone", display="Database time zone", description="Time zone of the database, if different than Integration Server", advanced=true)
	public String getDatabaseTimeZone() {
		return this.databaseTimeZone;
	}

	public void setDatabaseTimeZone(String databaseTimeZone) {
		this.databaseTimeZone = databaseTimeZone;
	}
	
	@TranslatorProperty(name="ExtensionTranslationClassName", display="Extension SQL Translation Class", required=true, 
			defaultValue="org.teiid.translator.jdbc.translator.Translator", 
			allowed= {"org.teiid.translator.jdbc.translator.Translator",
			"org.teiid.translator.jdbc.access.AccessSQLTranslator",
			"org.teiid.translator.jdbc.db2.DB2SQLTranslator",
			"org.teiid.translator.jdbc.derby.DerbySQLTranslator",
			"org.teiid.translator.jdbc.h2.H2Translator",
			"org.teiid.translator.jdbc.hsql.HsqlTranslator",
			"org.teiid.translator.jdbc.mysql.MySQLTranslator",
			"org.teiid.translator.jdbc.mysql.MySQL5Translator",
			"org.teiid.translator.jdbc.oracle.OracleSQLTranslator",
			"org.teiid.translator.jdbc.postgresql.PostgreSQLTranslator", 
			"org.teiid.translator.jdbc.sqlserver.SqlServerSQLTranslator",
			"org.teiid.translator.jdbc.sybase.SybaseSQLTranslator"
			})
	public String getExtensionTranslationClassName() {
		return this.extensionTranslationClassName;
	}

	public void setExtensionTranslationClassName(String extensionTranslationClassName) {
		this.extensionTranslationClassName = extensionTranslationClassName;
	}
	
	@TranslatorProperty(name="TrimStrings",display="Trim string flag", description="Right Trim fixed character types returned as Strings - note that the native type must be char or nchar and the source must support the rtrim function.",advanced=true, defaultValue="false")
	public boolean isTrimStrings() {
		return this.trimStrings;
	}

	public void setTrimStrings(boolean trimStrings) {
		this.trimStrings = trimStrings;
	}

	@TranslatorProperty(name="UseCommentsInSourceQuery", display="Use informational comments in Source Queries", description="This will embed /*comment*/ style comment with session/request id in source SQL query for informational purposes", advanced=true, defaultValue="false")
	public boolean isUseCommentsInSourceQuery() {
		return this.useCommentsInSourceQuery;
	}

	public void setUseCommentsInSourceQuery(boolean useCommentsInSourceQuery) {
		this.useCommentsInSourceQuery = useCommentsInSourceQuery;
	}

	
	@TranslatorProperty(name="FetchSize", display="FetCh Size", description="fetch size used from the connector to its underlying source.", advanced=true, defaultValue="1024")
	public int getFetchSize() {
		return this.fetchSize;
	}
	
	@Override
	public boolean isSourceRequired() {
		return true;
	}

	public void setFetchSize(int fetchSize) {
		this.fetchSize = fetchSize;
	}
	
    @Override
    public ResultSetExecution createResultSetExecution(QueryExpression command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory)
    		throws ConnectorException {
    	try {
			DataSource ds = (DataSource)connectionFactory;
			Connection conn = ds.getConnection();
	    	//TODO: This is not correct; this should be only called once for connection creation    	
	    	getTranslator().afterConnectionCreation(conn);
	    	return new JDBCQueryExecution(command, conn, executionContext, this, getTranslator());
    	} catch(SQLException e) {
    		throw new ConnectorException(e);
    	}
    }
    
    @Override
    public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory)
    		throws ConnectorException {
    	try {
			DataSource ds = (DataSource)connectionFactory;
			Connection conn = ds.getConnection();
			//TODO: This is not correct; this should be only called once for connection creation    	
			getTranslator().afterConnectionCreation(conn);
			return new JDBCProcedureExecution(command, conn, executionContext, this, getTranslator());
		} catch (SQLException e) {
			throw new ConnectorException(e);
		}
    }

    @Override
    public UpdateExecution createUpdateExecution(Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Object connectionFactory)
    		throws ConnectorException {
    	try {
			DataSource ds = (DataSource)connectionFactory;
			Connection conn = ds.getConnection();
			
			//TODO: This is not correct; this should be only called once for connection creation
			getTranslator().afterConnectionCreation(conn);
			return new JDBCUpdateExecution(command, conn, executionContext, this, getTranslator());
		} catch (SQLException e) {
			throw new ConnectorException(e);
		}    
    }	
    
	@Override
	public void getConnectorMetadata(MetadataFactory metadataFactory, Object connectionFactory) throws ConnectorException {
		try {
	    	Connection conn = null;
			try {
		    	DataSource ds = (DataSource)connectionFactory;
		    	conn = ds.getConnection();
				
				JDBCMetdataProcessor metadataProcessor = new JDBCMetdataProcessor();
				PropertiesUtils.setBeanProperties(metadataProcessor, metadataFactory.getImportProperties(), "importer"); //$NON-NLS-1$
				metadataProcessor.getConnectorMetadata(conn, metadataFactory);
			} finally {
				if (conn != null) {
					conn.close();
				}
			}
		} catch (SQLException e) {
			throw new ConnectorException(e);
		}
	}    
}
