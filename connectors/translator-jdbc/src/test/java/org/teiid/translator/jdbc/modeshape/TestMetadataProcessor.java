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

import static org.junit.Assert.*;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;
import java.util.ArrayList;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.jdbc.JDBCMetdataProcessor;
import org.teiid.metadata.Procedure;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestMetadataProcessor {
	
	private static ModeShapeJDBCMetdataProcessor PROCESSOR;
	private static MetadataFactory MF;
	
    @BeforeClass
    public static void setUp() throws TranslatorException {
    	PROCESSOR  = new ModeShapeJDBCMetdataProcessor();
    	MF = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
     }
	
	@Test public void testGetProcedures() throws SQLException {
		
		DatabaseMetaData dmd = Mockito.mock(DatabaseMetaData.class);
		
		// make the DatabaseMetaData.getProcedures not supported so to test that ModeShapeJDBCMetdataProcessor.getProcedures doesn't fail under
		// this condition
		Mockito.stub(dmd.getProcedures(null, null, null)).toThrow(new java.sql.SQLFeatureNotSupportedException("SFNS"));
		
		PROCESSOR.getProcedures(MF, dmd);

		Procedure proc1 = MF.getSchema().getProcedure(ModeShapeJDBCMetdataProcessor.JCR_ISCHILDNODE_PROC);
		Procedure proc2 = MF.getSchema().getProcedure(ModeShapeJDBCMetdataProcessor.JCR_ISDESCENDANTNODE_PROC);
		Procedure proc3 = MF.getSchema().getProcedure(ModeShapeJDBCMetdataProcessor.JCR_ISSAMENODE_PROC);
		Procedure proc4 = MF.getSchema().getProcedure(ModeShapeJDBCMetdataProcessor.JCR_CONTAINS_PROC);
		Procedure proc5 = MF.getSchema().getProcedure(ModeShapeJDBCMetdataProcessor.JCR_REFERENCE_PROC);
		
		assertNotNull(proc1);
		assertNotNull(proc2);
		assertNotNull(proc3);
		assertNotNull(proc4);
		assertNotNull(proc5);
		
	}
	
	/**
	 * 
	 */
	@Test public void testGetIndexes() throws SQLException {
		
		DatabaseMetaData dmd = Mockito.mock(DatabaseMetaData.class);
		
		// make the DatabaseMetaData.getProcedures not supported so to test that ModeShapeJDBCMetdataProcessor.getProcedures doesn't fail under
		// this condition
		Mockito.stub(dmd.getPrimaryKeys(null, null, null)).toThrow(new java.sql.SQLFeatureNotSupportedException("SFNS"));
		Mockito.stub(dmd.getIndexInfo(null, null, null, false, false)).toThrow(new java.sql.SQLFeatureNotSupportedException("SFNS"));
		Mockito.stub(dmd.getImportedKeys(null, null, null)).toThrow(new java.sql.SQLFeatureNotSupportedException("SFNS"));
		
		JDBCMetdataProcessor.TableInfo ti = new JDBCMetdataProcessor.TableInfo("a", "b", "c", null);

		
		PROCESSOR.getPrimaryKeys(MF, dmd, new ArrayList(0));
		PROCESSOR.getIndexes(MF, dmd, new ArrayList(0), false );
		PROCESSOR.getForeignKeys(MF, dmd, new ArrayList(0), null );
		
	}	
	
}
