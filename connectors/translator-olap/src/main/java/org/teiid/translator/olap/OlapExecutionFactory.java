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

import javax.sql.DataSource;

import org.olap4j.OlapConnection;
import org.olap4j.OlapWrapper;
import org.teiid.language.Call;
import org.teiid.logging.LogConstants;
import org.teiid.logging.LogManager;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.BaseColumn.NullType;
import org.teiid.metadata.ProcedureParameter.Type;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.Translator;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

@Translator(name="olap", description="A translator for OLAP Cubes")
public class OlapExecutionFactory extends ExecutionFactory<DataSource, Connection> {
	private static final String INVOKE_MDX = "invokeMdx"; //$NON-NLS-1$
	
	@Override
	public void getMetadata(MetadataFactory metadataFactory, Connection conn) throws TranslatorException {
		Procedure p = metadataFactory.addProcedure(INVOKE_MDX);
		p.setAnnotation("Invokes a XMLA webservice with provided MDX query that returns an XML result"); //$NON-NLS-1$

		// mdx query in xml form
		ProcedureParameter param = metadataFactory.addProcedureParameter("request", TypeFacility.RUNTIME_NAMES.STRING, Type.In, p); //$NON-NLS-1$
		param.setAnnotation("The MDX query to execute"); //$NON-NLS-1$
		param.setNullType(NullType.Nullable);
		metadataFactory.addProcedureResultSetColumn("tuple", TypeFacility.RUNTIME_NAMES.OBJECT, p); //$NON-NLS-1$		
	}
    
    @Override
	public ProcedureExecution createProcedureExecution(Call command, ExecutionContext executionContext, RuntimeMetadata metadata, Connection connection) throws TranslatorException {
    	return new OlapQueryExecution(command, unwrap(connection), executionContext, this);
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
