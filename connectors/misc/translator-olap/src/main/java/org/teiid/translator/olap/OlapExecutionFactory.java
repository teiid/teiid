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
package org.teiid.translator.olap;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.olap4j.OlapConnection;
import org.olap4j.OlapWrapper;
import org.teiid.language.Argument;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.visitor.SQLStringVisitor;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;

@Translator(name="olap", description="A translator for OLAP Cubes")
public class OlapExecutionFactory extends ExecutionFactory<DataSource, Connection> {
	private static final String INVOKE_MDX = "invokeMdx"; //$NON-NLS-1$
	
	public OlapExecutionFactory() {
		setSourceRequiredForMetadata(false);
		setSupportsDirectQueryProcedure(true);
		setDirectQueryProcedureName(INVOKE_MDX);
	}
	
    @Override
   	public ProcedureExecution createDirectExecution(List<Argument> arguments, Command command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection connection) throws TranslatorException {
    	return new OlapQueryExecution(arguments.subList(1, arguments.size()), command, unwrap(connection), executionContext, this, (String) arguments.get(0).getArgumentValue().getValue(), true);
	}    
    
    @Override
    public ProcedureExecution createProcedureExecution(Call command,
    		ExecutionContext executionContext, RuntimeMetadata metadata,
    		Connection connection) throws TranslatorException {
    	String nativeQuery = command.getMetadataObject().getProperty(SQLStringVisitor.TEIID_NATIVE_QUERY, false);
    	if (nativeQuery != null) {
    		return new OlapQueryExecution(command.getArguments(), command, unwrap(connection), executionContext, this, nativeQuery, false);
    	}
    	throw new TranslatorException("Missing native-query extension metadata."); //$NON-NLS-1$
    }

	private OlapConnection unwrap(Connection conn) throws TranslatorException {
    	try {
    		OlapWrapper wrapper = conn.unwrap(OlapWrapper.class);
    		OlapConnection olapConn = wrapper.unwrap(OlapConnection.class);
    		return olapConn;
    	} catch(SQLException e) {
    		throw new TranslatorException(e);
    	}		
	}
	
    @Override
    public Connection getConnection(DataSource ds)
    		throws TranslatorException {
		try {
	    	return ds.getConnection();
		} catch (SQLException e) {
			throw new TranslatorException(e);
		}
    }
    
    @Override
    public void closeConnection(Connection connection, DataSource factory) {
    	if (connection == null) {
    		return;
    	}
    	try {
			connection.close();
		} catch (SQLException e) {
			LogManager.logDetail(LogConstants.CTX_CONNECTOR, e, "Error closing"); //$NON-NLS-1$
		}
    }
}
