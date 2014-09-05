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

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import javax.resource.ResourceException;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.FileConnection;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestExcelMetadataProcessor {

	static String getDDL(Properties props) throws TranslatorException, ResourceException {
		ExcelExecutionFactory translator = new ExcelExecutionFactory();
    	translator.start();
    	
    	String xlsName = props.getProperty("importer.excelFileName");
    	MetadataFactory mf = new MetadataFactory("vdb", 1, "people", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);    	
    	FileConnection connection = Mockito.mock(FileConnection.class);
    	Mockito.stub(connection.getFile(xlsName)).toReturn(UnitTestUtil.getTestDataFile(xlsName));
		translator.getMetadata(mf, connection);
		
		TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "vdb", new FunctionTree("foo", new UDFSource(translator.getPushDownFunctions())));
    	ValidatorReport report = new MetadataValidator().validate(metadata.getVdbMetaData(), metadata.getMetadataStore());
    	if (report.hasItems()) {
    		throw new RuntimeException(report.getFailureMessage());
    	}		
		
		String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
		return ddl;
	}	
	
	
	@Test
	public void testSchemaNoHeaderXLS() throws Exception {
		Properties props = new Properties();
		props.setProperty("importer.excelFileName", "names.xls");
				
		String ddl = getDDL(props);	
		
		String expectedDDL = "SET NAMESPACE 'http://www.teiid.org/translator/excel/2014' AS teiid_excel;\n\n" + 
		        "CREATE FOREIGN TABLE Sheet1 (\n" + 
				"	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" + 
				"	column1 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '7'),\n" + 
				"	column2 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '8'),\n" + 
				"	column3 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '9'),\n" + 
				"	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" + 
				") OPTIONS (\"teiid_excel:FILE\" 'names.xls', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '13');";
		
		assertEquals(expectedDDL, ddl);
	}
	
	@Test
	public void testSchemaNoHeaderXLSX() throws Exception {
		Properties props = new Properties();
		props.setProperty("importer.excelFileName", "names.xlsx");
				
		String ddl = getDDL(props);	
		
		String expectedDDL = "SET NAMESPACE 'http://www.teiid.org/translator/excel/2014' AS teiid_excel;\n\n" + 
				"CREATE FOREIGN TABLE Sheet1 (\n" + 
				"	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" + 
				"	column1 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" + 
				"	column2 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" + 
				"	column3 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '3'),\n" +
				"	column4 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '4'),\n" +				
				"	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" + 
				") OPTIONS (\"teiid_excel:FILE\" 'names.xlsx', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '1');";
		
		assertEquals(expectedDDL, ddl);
	}	

	@Test
	public void testSchemaWithHeaderXLS() throws Exception {
		Properties props = new Properties();
		props.setProperty("importer.excelFileName", "names.xls");
		props.setProperty("importer.headerRowNumber", "13");
				
		String ddl = getDDL(props);	
		
		String expectedDDL = "SET NAMESPACE 'http://www.teiid.org/translator/excel/2014' AS teiid_excel;\n\n" + 
		        "CREATE FOREIGN TABLE Sheet1 (\n" + 
				"	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" + 
				"	FirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '7'),\n" + 
				"	LastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '8'),\n" + 
				"	Age double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '9'),\n" + 
				"	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" + 
				") OPTIONS (\"teiid_excel:FILE\" 'names.xls', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '14');";
		
		assertEquals(expectedDDL, ddl);
	}
	
	@Test
	public void testSchemaWithHeaderXLSX() throws Exception {
		Properties props = new Properties();
		props.setProperty("importer.excelFileName", "names.xlsx");
		props.setProperty("importer.headerRowNumber", "1");
				
		String ddl = getDDL(props);	
		String expectedDDL ="SET NAMESPACE 'http://www.teiid.org/translator/excel/2014' AS teiid_excel;\n\n" + 
		        "CREATE FOREIGN TABLE Sheet1 (\n" + 
				"	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" + 
				"	FirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" + 
				"	LastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" + 
				"	Age double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '3'),\n" +
				"	\"time\" double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '4'),\n"+
				"	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" + 
				") OPTIONS (\"teiid_excel:FILE\" 'names.xlsx', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '2');";
		
		assertEquals(expectedDDL, ddl);
	}		
	
	@Test
	public void testSchemaSetDataRowXLS() throws Exception {
		Properties props = new Properties();
		props.setProperty("importer.excelFileName", "names.xls");
		props.setProperty("importer.dataRowNumber", "15");
				
		String ddl = getDDL(props);	
		
		String expectedDDL = "SET NAMESPACE 'http://www.teiid.org/translator/excel/2014' AS teiid_excel;\n\n" + 
		        "CREATE FOREIGN TABLE Sheet1 (\n" + 
				"	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" + 
				"	column1 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '7'),\n" + 
				"	column2 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '8'),\n" + 
				"	column3 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '9'),\n" + 
				"	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" + 
				") OPTIONS (\"teiid_excel:FILE\" 'names.xls', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '15');";
		
		assertEquals(expectedDDL, ddl);
	}
	
	@Test
	public void testSchemaSetDataRowXLSX() throws Exception {
		Properties props = new Properties();
		props.setProperty("importer.excelFileName", "names.xlsx");
		props.setProperty("importer.dataRowNumber", "3");
		
		String ddl = getDDL(props);	
		
		String expectedDDL = "SET NAMESPACE 'http://www.teiid.org/translator/excel/2014' AS teiid_excel;\n\n" + 
		        "CREATE FOREIGN TABLE Sheet1 (\n" + 
				"	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" + 
				"	column1 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" + 
				"	column2 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" + 
				"	column3 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '3'),\n" + 
				"	column4 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '4'),\n" +
				"	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" + 
				") OPTIONS (\"teiid_excel:FILE\" 'names.xlsx', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '3');";
		
		assertEquals(expectedDDL, ddl);
	}	
	
	@Test
	public void testSchemaWithHeaderAndDataXLS() throws Exception {
		Properties props = new Properties();
		props.setProperty("importer.excelFileName", "names.xls");
		props.setProperty("importer.headerRowNumber", "13");
		props.setProperty("importer.dataRowNumber", "15");
				
		String ddl = getDDL(props);	
		
		String expectedDDL = "SET NAMESPACE 'http://www.teiid.org/translator/excel/2014' AS teiid_excel;\n\n" + 
		        "CREATE FOREIGN TABLE Sheet1 (\n" + 
				"	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" + 
				"	FirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '7'),\n" + 
				"	LastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '8'),\n" + 
				"	Age double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '9'),\n" + 
				"	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" + 
				") OPTIONS (\"teiid_excel:FILE\" 'names.xls', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '15');";
		
		assertEquals(expectedDDL, ddl);
	}
	
	@Test
	public void testSchemaWithHeaderAndDataXLSX() throws Exception {
		Properties props = new Properties();
		props.setProperty("importer.excelFileName", "names.xlsx");
		props.setProperty("importer.headerRowNumber", "1");
		props.setProperty("importer.dataRowNumber", "3");				
		String ddl = getDDL(props);	
		String expectedDDL = "SET NAMESPACE 'http://www.teiid.org/translator/excel/2014' AS teiid_excel;\n\n" + 
		        "CREATE FOREIGN TABLE Sheet1 (\n" + 
				"	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" + 
				"	FirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" + 
				"	LastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" + 
				"	Age double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '3'),\n" + 
				"	\"time\" double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '4'),\n" +
				"	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" + 
				") OPTIONS (\"teiid_excel:FILE\" 'names.xlsx', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '3');";
		
		assertEquals(expectedDDL, ddl);
	}
	
    @Test
    public void testEmptySheetXLSX() throws Exception {
        Properties props = new Properties();
        props.setProperty("importer.excelFileName", "multi_sheet_empty_names.xlsx");
        props.setProperty("importer.headerRowNumber", "0");
        props.setProperty("importer.dataRowNumber", "2");               
        String ddl = getDDL(props); 
        String expectedDDL = "SET NAMESPACE 'http://www.teiid.org/translator/excel/2014' AS teiid_excel;\n\n" + 
                "CREATE FOREIGN TABLE Sheet1 (\n" + 
                "\tROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" + 
                "\tFirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" + 
                "\tLastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" + 
                "\tAge double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '3'),\n" + 
                "\tCONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n"+
                ") OPTIONS (\"teiid_excel:FILE\" 'multi_sheet_empty_names.xlsx', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '2');";
        
        assertEquals(expectedDDL, ddl);
    }	
    
    @Test
    public void testDataTypeFromNullCell() throws Exception {
        Properties props = new Properties();
        props.setProperty("importer.excelFileName", "names.xlsx");
        props.setProperty("importer.headerRowNumber", "1");
        props.setProperty("importer.dataRowNumber", "2");               
        String ddl = getDDL(props); 
        String expectedDDL = "SET NAMESPACE 'http://www.teiid.org/translator/excel/2014' AS teiid_excel;\n" + 
        		"\n" + 
        		"CREATE FOREIGN TABLE Sheet1 (\n" + 
        		"\tROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" + 
        		"\tFirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" + 
        		"\tLastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" + 
        		"\tAge double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '3'),\n" + 
        		"\t\"time\" double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '4'),\n"+
        		"\tCONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" + 
        		") OPTIONS (\"teiid_excel:FILE\" 'names.xlsx', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '2');";
        
        assertEquals(expectedDDL, ddl);
    }    
}
