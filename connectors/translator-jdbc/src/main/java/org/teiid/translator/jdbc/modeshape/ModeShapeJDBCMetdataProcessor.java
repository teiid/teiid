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

package org.teiid.translator.jdbc.modeshape;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.teiid.metadata.Datatype;
import org.teiid.metadata.FunctionMethod;
import org.teiid.metadata.FunctionParameter;
import org.teiid.metadata.MetadataFactory;
import org.teiid.translator.jdbc.JDBCMetdataProcessor;


/**
 * Reads from {@link DatabaseMetaData} and creates metadata through the {@link MetadataFactory}.
 * 
 * See https://issues.jboss.org/browse/TEIID-2786 which describes this implementation.
 * 
 */
public class ModeShapeJDBCMetdataProcessor extends JDBCMetdataProcessor {
	
	public ModeShapeJDBCMetdataProcessor() {
		setWidenUnsignedTypes(false);
		setUseQualifiedName(false);
		setUseCatalogName(false);
		setImportForeignKeys(false);
		setColumnNamePattern("%"); //$NON-NLS-1$
	}
	
	@Override
	public void getConnectorMetadata(Connection conn, MetadataFactory metadataFactory)
			throws SQLException {
			super.getConnectorMetadata(conn, metadataFactory);
			addModeShapeProcedures(metadataFactory);
	}	
	
	protected void addModeShapeProcedures(MetadataFactory metadataFactory)  {
		String[] names = new String[] {"path1", "path2"};
		String[] types = new String[] {"String", "String"};
		int[] lengths = new int[] {4000, 4000};
		createFunctionMethod(metadataFactory, ModeShapeExecutionFactory.JCR_ISCHILDNODE, "ModeShape ISCHILDNODE JCR Call", "JCR", "boolean", names, types, lengths, true );

		createFunctionMethod(metadataFactory, ModeShapeExecutionFactory.JCR_ISSAMENODE, "ModeShape ISSAMENODE JCR Call", "JCR", "boolean", names, types, lengths, true );

		createFunctionMethod(metadataFactory, ModeShapeExecutionFactory.JCR_ISDESCENDANTNODE, "ModeShape ISDESCENDANTNODE JCR Call", "JCR", "boolean", names, types, lengths, true);

		names = new String[] {"selectOrProperty"};
		types = new String[] {"String"};
		lengths = new int[] {4000};
		createFunctionMethod(metadataFactory, ModeShapeExecutionFactory.JCR_REFERENCE, "ModeShape REFERENCE JCR Call", "JCR", "boolean", names, types, lengths, true );

		names = new String[] {"selectOrProperty", "searchExpr"};
		types = new String[] {"String", "String"};
		lengths = new int[] {4000, 4000};
		createFunctionMethod(metadataFactory, ModeShapeExecutionFactory.JCR_CONTAINS, "ModeShape CONTAINS JCR Call", "JCR", "boolean", names, types, lengths, false );

	}

	private void createFunctionMethod(MetadataFactory metadataFactory, String name, String description, String category, String returnType, String[] names, String[] parameterTypes, int[] length, boolean deterministic) {
		FunctionParameter[] params = new FunctionParameter[parameterTypes.length];
		for (int i = 0; i < parameterTypes.length; i++) {
			params[i] = new FunctionParameter(names[i], parameterTypes[i]); //$NON-NLS-1$
			Datatype dt =  metadataFactory.getDataTypes().get(parameterTypes[i]);
			params[i].setDatatype(dt);
		}

		FunctionParameter returnParm = new FunctionParameter("result", returnType);
		returnParm.setName("result");
		FunctionMethod method = new FunctionMethod(name, description, category, params, returnParm); //$NON-NLS-1$
		method.setNameInSource(name);
		method.setDeterministicBoolean(Boolean.valueOf(deterministic));
		
		metadataFactory.getSchema().addFunction(method);	
	}	
	
}
