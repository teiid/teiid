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
package org.teiid.translator.hbase;

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import javax.resource.ResourceException;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.HBaseConnection;
import org.teiid.translator.TranslatorException;

public class TestHBaseMetadataProcessor {
	
	static String getDDL(String hbaseTableName, String columnQualifiers, String columnTypes) throws TranslatorException, ResourceException, SQLException {
		HBaseExecutionFactory translator = new HBaseExecutionFactory();
		translator.start();
		
		Properties props = new Properties();
		props.setProperty("importer.hbaseTableName", hbaseTableName);
		props.setProperty("importer.columnQualifiers", columnQualifiers);
		props.setProperty("importer.columnTypes", columnTypes);
		
		MetadataFactory mf = new MetadataFactory("vdb", 1, "customer", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
		
		HBaseConnection connection = Mockito.mock(HBaseConnection.class);
		Mockito.stub(connection.getConnection()).toReturn(Mockito.mock(Connection.class));
		Connection conn = connection.getConnection();
		Mockito.stub(conn.createStatement()).toReturn(Mockito.mock(Statement.class));
		
		translator.getMetadata(mf, connection);	
		
		String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
		return ddl;
	}

	
	/*
	 * Assuming the hbaseTableName, columnQualifiers and columnTypes can be passed when Designer tool do importer
	 *    importer.hbaseTableName
	 *    importer.columnQualifiers
	 *    importer.columnTypes
	 */
	@Test
	public void testMetadataProcessor() throws TranslatorException, ResourceException, SQLException {
		
		String hbaseTableName = "Customer";
		String columnQualifiers = "ROW_ID,customer:city,customer:name,sales:amount,sales:product";
		String columnTypes = "string,string,string,string,string";
		
		String ddl = getDDL(hbaseTableName, columnQualifiers, columnTypes);
		
		String expectedDDL = "SET NAMESPACE 'http://www.teiid.org/translator/hbase/2014' AS teiid_hbase;\n\n" +
					"CREATE FOREIGN TABLE Customer (\n" +
					"	ROW_ID string OPTIONS (\"teiid_hbase:CELL\" 'ROW_ID'),\n" +
					"	city string OPTIONS (\"teiid_hbase:CELL\" 'customer:city'),\n" +
					"	name string OPTIONS (\"teiid_hbase:CELL\" 'customer:name'),\n" +
					"	amount string OPTIONS (\"teiid_hbase:CELL\" 'sales:amount'),\n" +
					"	product string OPTIONS (\"teiid_hbase:CELL\" 'sales:product'),\n" +
					"	CONSTRAINT ROW_ID PRIMARY KEY(ROW_ID)\n" +
					") OPTIONS (\"teiid_hbase:TABLE\" 'Customer');" ;
		
		assertEquals(expectedDDL, ddl);

	}
	
}
