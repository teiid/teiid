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
package org.teiid.translator.excel;

import static org.junit.Assert.*;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.FileConnection;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestExcelExecution {

	private ArrayList helpExecute(String ddl, FileConnection connection, String query) throws Exception {
		ExcelExecutionFactory translator = new ExcelExecutionFactory();
    	translator.start();
    	
    	TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "vdb", "excel");
    	TranslationUtility utility = new TranslationUtility(metadata);
		
		Command cmd = utility.parseCommand(query);
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		
		ResultSetExecution execution = translator.createResultSetExecution((QueryExpression)cmd, context, utility.createRuntimeMetadata(), connection);
		try {
			execution.execute();
			
			ArrayList results = new ArrayList();
			while (true) {
				List<?> row = execution.next();
				if (row == null) {
					break;
				}
				results.add(row);
			}
			return results;
		} finally {
			execution.close();
		}
	}
		
	@Test
	public void testExecutionNoDataNumberXLS() throws Exception {
		String ddl = "CREATE FOREIGN TABLE Sheet1 (\n" + 
				"	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" + 
				"	column1 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '7'),\n" + 
				"	column2 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '8'),\n" + 
				"	column3 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '9'),\n" + 
				"	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" + 
				") OPTIONS (\"teiid_excel:FILE\" 'names.xls');";

    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xls")).toReturn(UnitTestUtil.getTestDataFile("names.xls"));

    	ArrayList results = helpExecute(ddl, connection, "select * from Sheet1");
    	assertEquals("[[13, FirstName, LastName, Age], [14, John, Doe, 44.0], [15, Jane, Smith, 40.0], [16, Matt, Liek, 13.0], [17, Sarah, Byne, 10.0], [18, Rocky, Dog, 3.0], [19, Total, null, 110.0]]", results.toString());
	}
	
	@Test
	public void testExecutionNoDataNumberXLSX() throws Exception {
		String ddl = "CREATE FOREIGN TABLE Sheet1 (\n" + 
				"	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" + 
				"	column1 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" + 
				"	column2 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" + 
				"	column3 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '3'),\n" + 
				"	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" + 
				") OPTIONS (\"teiid_excel:FILE\" 'names.xlsx');";

    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xlsx")).toReturn(UnitTestUtil.getTestDataFile("names.xlsx"));

    	ArrayList results = helpExecute(ddl, connection, "select * from Sheet1");
    	assertEquals("[[1, FirstName, LastName, Age], [2, John, Doe, null], [3, Jane, Smith, 40.0], [4, Matt, Liek, 13.0], [5, Sarah, Byne, 10.0], [6, Rocky, Dog, 3.0]]", results.toString());
	}	
	
	@Test
	public void testExecutionColumnWithNullCell() throws Exception {
		String ddl = "CREATE FOREIGN TABLE Sheet1 (\n" + 
				"	column1 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1')\n" + 
				") OPTIONS (\"teiid_excel:FILE\" '3219.xlsx');";

    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("3219.xlsx")).toReturn(UnitTestUtil.getTestDataFile("3219.xlsx"));

    	ArrayList results = helpExecute(ddl, connection, "select * from Sheet1");
    	assertEquals(results.size(), 7);
	}
	
	@Test
	public void testExecutionColumnsWithNullCell() throws Exception {
		String ddl = "CREATE FOREIGN TABLE Sheet1 (\n" + 
				"	column1 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" + 
				"	column2 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2')\n" + 
				") OPTIONS (\"teiid_excel:FILE\" '3219.xlsx');";

    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("3219.xlsx")).toReturn(UnitTestUtil.getTestDataFile("3219.xlsx"));

    	ArrayList results = helpExecute(ddl, connection, "select * from Sheet1");
    	assertEquals(results.size(), 7);
	}
	
	@Test
	public void testExecutionWithDataNumberXLS() throws Exception {
		String ddl = "CREATE FOREIGN TABLE Sheet1 (\n" + 
				"	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" + 
				"	column1 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '7'),\n" + 
				"	column2 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '8'),\n" + 
				"	column3 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '9'),\n" + 
				"	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" + 
				") OPTIONS (\"teiid_excel:FILE\" 'names.xls', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '18');";

    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xls")).toReturn(UnitTestUtil.getTestDataFile("names.xls"));

    	ArrayList results = helpExecute(ddl, connection, "select * from Sheet1");
    	assertEquals("[[18, Rocky, Dog, 3.0], [19, Total, null, 110.0]]", results.toString());
	}
	
	@Test
	public void testExecutionWithDataNumberXLSX() throws Exception {
		String ddl = "CREATE FOREIGN TABLE Sheet1 (\n" + 
				"	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" + 
				"	column1 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" + 
				"	column2 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" + 
				"	column3 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '3'),\n" + 
				"	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" + 
				") OPTIONS (\"teiid_excel:FILE\" 'names.xlsx', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '6');";

    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xlsx")).toReturn(UnitTestUtil.getTestDataFile("names.xlsx"));

    	ArrayList results = helpExecute(ddl, connection, "select * from Sheet1");
    	assertEquals("[[6, Rocky, Dog, 3.0]]", results.toString());
	}	
	
	@Test
	public void testExecutionWithDataNumberWithHeaderXLS() throws Exception {
		String ddl = "CREATE FOREIGN TABLE Sheet1 (\n" + 
				"	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" + 
				"	FirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '7'),\n" + 
				"	LastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '8'),\n" + 
				"	Age double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '9'),\n" + 
				"	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" + 
				") OPTIONS (\"teiid_excel:FILE\" 'names.xls', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '18');";

    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xls")).toReturn(UnitTestUtil.getTestDataFile("names.xls"));

    	ArrayList results = helpExecute(ddl, connection, "select * from Sheet1");
    	assertEquals("[[18, Rocky, Dog, 3.0], [19, Total, null, 110.0]]", results.toString());
	}	

	static String commonDDL = "CREATE FOREIGN TABLE Sheet1 (\n" + 
			"	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" + 
			"	FirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '7'),\n" + 
			"	LastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '8'),\n" + 
			"	Age double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '9'),\n" + 
			"	\"time\" time OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '4'),\n" +
			"	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" + 
			") OPTIONS (\"teiid_excel:FILE\" 'names.xls', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '14');";
	
	@Test
	public void testExecutionEquals() throws Exception {
    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xls")).toReturn(UnitTestUtil.getTestDataFile("names.xls"));

    	ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID=16");
    	assertEquals("[[Matt]]", results.toString());
	}
	
	@Test
	public void testExecutionGT() throws Exception {
    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xls")).toReturn(UnitTestUtil.getTestDataFile("names.xls"));

    	ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID>16");
    	assertEquals("[[Sarah], [Rocky], [Total]]", results.toString());
	}		
	
	@Test
	public void testExecutionGE() throws Exception {
    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xls")).toReturn(UnitTestUtil.getTestDataFile("names.xls"));

    	ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID>=16");
    	assertEquals("[[Matt], [Sarah], [Rocky], [Total]]", results.toString());
	}	
	
	@Test
	public void testExecutionLT() throws Exception {
    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xls")).toReturn(UnitTestUtil.getTestDataFile("names.xls"));

    	ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID < 16");
    	assertEquals("[[John], [Jane]]", results.toString());
	}	
	
	@Test
	public void testExecutionLE() throws Exception {
    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xls")).toReturn(UnitTestUtil.getTestDataFile("names.xls"));

    	ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID <= 16");
    	assertEquals("[[John], [Jane], [Matt]]", results.toString());
	}
	
	@Test
	public void testExecutionNE() throws Exception {
    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xls")).toReturn(UnitTestUtil.getTestDataFile("names.xls"));

    	ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID != 16");
    	assertEquals("[[John], [Jane], [Sarah], [Rocky], [Total]]", results.toString());
	}	
	
	@Test
	public void testExecutionLimit() throws Exception {
    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xls")).toReturn(UnitTestUtil.getTestDataFile("names.xls"));

    	ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 LIMIT 3,1");
    	assertEquals("[[Sarah]]", results.toString());
	}
	
	@Test
	public void testExecutionLimit2() throws Exception {
    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xls")).toReturn(UnitTestUtil.getTestDataFile("names.xls"));

    	ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 LIMIT 1");
    	assertEquals("[[John]]", results.toString());
	}	

	@Test
	public void testExecutionAnd() throws Exception {
    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xls")).toReturn(UnitTestUtil.getTestDataFile("names.xls"));

    	ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID > 16 and ROW_ID < 18");
    	assertEquals("[[Sarah]]", results.toString());
	}
	
	@Test
	public void testExecutionIN() throws Exception {
    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xls")).toReturn(UnitTestUtil.getTestDataFile("names.xls"));

    	ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID IN (13, 18)");
    	assertEquals("[[John], [Total]]", results.toString());
	}	
	
	@Test
	public void testTime() throws Exception {
    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xls")).toReturn(UnitTestUtil.getTestDataFile("names.xlsx"));

    	ArrayList results = helpExecute(commonDDL, connection, "select \"time\" from Sheet1");
    	assertEquals("[[10:12:14]]", results.toString());
	}	
	
	@Test(expected=TranslatorException.class)
	public void testExecutionNoFile() throws Exception {
    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile("names.xls")).toReturn(new File("does not exist"));

    	helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID != 16");
	}	
}
