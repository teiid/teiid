/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.excel;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.file.VirtualFileConnection;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestExcelMetadataProcessor {

    static String getDDL(Properties props) throws TranslatorException {
        ExcelExecutionFactory translator = new ExcelExecutionFactory();
        translator.start();

        String xlsName = props.getProperty("importer.excelFileName");
        MetadataFactory mf = new MetadataFactory("vdb", 1, "people", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles(xlsName)).toReturn(TestExcelExecution.getFile(xlsName));
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

        String expectedDDL = "CREATE FOREIGN TABLE Sheet1 (\n" +
                "	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" +
                "	column1 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '7'),\n" +
                "	column2 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '8'),\n" +
                "	column3 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '9'),\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" +
                ") OPTIONS (NAMEINSOURCE 'Sheet1', \"teiid_excel:FILE\" 'names.xls', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '13');";

        assertEquals(expectedDDL, ddl);
    }

    @Test
    public void testSchemaNoHeaderXLSX() throws Exception {
        Properties props = new Properties();
        props.setProperty("importer.excelFileName", "names.xlsx");

        String ddl = getDDL(props);

        String expectedDDL = "CREATE FOREIGN TABLE Sheet1 (\n" +
                "	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" +
                "	column1 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" +
                "	column2 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" +
                "	column3 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '3'),\n" +
                "	column4 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '4'),\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" +
                ") OPTIONS (NAMEINSOURCE 'Sheet1', \"teiid_excel:FILE\" 'names.xlsx', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '1');";

        assertEquals(expectedDDL, ddl);
    }

    @Test
    public void testSchemaWithHeaderXLS() throws Exception {
        Properties props = new Properties();
        props.setProperty("importer.excelFileName", "names.xls");
        props.setProperty("importer.headerRowNumber", "13");

        String ddl = getDDL(props);

        String expectedDDL = "CREATE FOREIGN TABLE Sheet1 (\n" +
                "	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" +
                "	FirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '7'),\n" +
                "	LastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '8'),\n" +
                "	Age double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '9'),\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" +
                ") OPTIONS (NAMEINSOURCE 'Sheet1', \"teiid_excel:FILE\" 'names.xls', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '14');";

        assertEquals(expectedDDL, ddl);
    }

    /**
     * Test a schema where there are empty cols in the header
     * @throws Exception
     */
    @Test
    public void testSchemaWithEmptyHeaderRowsXLS() throws Exception {
        Properties props = new Properties();
        props.setProperty("importer.excelFileName", "emptycols.xls");
        props.setProperty("importer.headerRowNumber", "1");

        String ddl = getDDL(props);

        String expectedDDL = "CREATE FOREIGN TABLE Sheet1 (\n" +
                "	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" +
                "	FirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" +
                "	LastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" +
                "	Age double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '3'),\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" +
                ") OPTIONS (NAMEINSOURCE 'Sheet1', \"teiid_excel:FILE\" 'emptycols.xls', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '2');";

        assertEquals(expectedDDL, ddl);
    }

    /**
     * Test a schema where there are empty cols in the header mixed with non-empty,
     * the table definition should only contain all non-empty columns
     * @throws Exception
     */
    @Test
    public void testSchemaWithIgnoreTrueHeaderRowsXLS() throws Exception {
        Properties props = new Properties();
        props.setProperty("importer.excelFileName", "empty-ignore.xls");
        props.setProperty("importer.headerRowNumber", "1");
        props.setProperty("importer.ignoreEmptyHeaderCells", "true");

        String ddl = getDDL(props);

        String expectedDDL = "CREATE FOREIGN TABLE Sheet1 (\n" +
                "	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" +
                "	FirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" +
                "	LastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" +
                "	Age double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '4'),\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" +
                ") OPTIONS (NAMEINSOURCE 'Sheet1', \"teiid_excel:FILE\" 'empty-ignore.xls', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '2');";

        assertEquals(expectedDDL, ddl);
    }

    /**
     * Test a schema where there are empty cols in the header mixed with non-empty,
     * the table definition should only contain the columns up to the first empty cell.
     * @throws Exception
     */
    @Test
    public void testSchemaWithIgnoreFalseHeaderRowsXLS() throws Exception {
        Properties props = new Properties();
        props.setProperty("importer.excelFileName", "empty-ignore.xls");
        props.setProperty("importer.headerRowNumber", "1");
        props.setProperty("importer.ignoreEmptyHeaderCells", "false");

        String ddl = getDDL(props);

        String expectedDDL = "CREATE FOREIGN TABLE Sheet1 (\n" +
                "	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" +
                "	FirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" +
                "	LastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" +
                ") OPTIONS (NAMEINSOURCE 'Sheet1', \"teiid_excel:FILE\" 'empty-ignore.xls', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '2');";

        assertEquals(expectedDDL, ddl);
    }

    @Test
    public void testSchemaWithHeaderXLSX() throws Exception {
        Properties props = new Properties();
        props.setProperty("importer.excelFileName", "names.xlsx");
        props.setProperty("importer.headerRowNumber", "1");

        String ddl = getDDL(props);
        String expectedDDL ="CREATE FOREIGN TABLE Sheet1 (\n" +
                "	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" +
                "	FirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" +
                "	LastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" +
                "	Age double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '3'),\n" +
                "	\"time\" timestamp OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '4'),\n"+
                "	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" +
                ") OPTIONS (NAMEINSOURCE 'Sheet1', \"teiid_excel:FILE\" 'names.xlsx', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '2');";

        assertEquals(expectedDDL, ddl);
    }

    @Test
    public void testSchemaSetDataRowXLS() throws Exception {
        Properties props = new Properties();
        props.setProperty("importer.excelFileName", "names.xls");
        props.setProperty("importer.dataRowNumber", "15");

        String ddl = getDDL(props);

        String expectedDDL = "CREATE FOREIGN TABLE Sheet1 (\n" +
                "	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" +
                "	column1 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '7'),\n" +
                "	column2 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '8'),\n" +
                "	column3 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '9'),\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" +
                ") OPTIONS (NAMEINSOURCE 'Sheet1', \"teiid_excel:FILE\" 'names.xls', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '15');";

        assertEquals(expectedDDL, ddl);
    }

    @Test
    public void testSchemaSetDataRowXLSX() throws Exception {
        Properties props = new Properties();
        props.setProperty("importer.excelFileName", "names.xlsx");
        props.setProperty("importer.dataRowNumber", "3");

        String ddl = getDDL(props);

        String expectedDDL = "CREATE FOREIGN TABLE Sheet1 (\n" +
                "	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" +
                "	column1 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" +
                "	column2 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" +
                "	column3 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '3'),\n" +
                "	column4 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '4'),\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" +
                ") OPTIONS (NAMEINSOURCE 'Sheet1', \"teiid_excel:FILE\" 'names.xlsx', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '3');";

        assertEquals(expectedDDL, ddl);
    }

    @Test
    public void testSchemaWithHeaderAndDataXLS() throws Exception {
        Properties props = new Properties();
        props.setProperty("importer.excelFileName", "names.xls");
        props.setProperty("importer.headerRowNumber", "13");
        props.setProperty("importer.dataRowNumber", "15");

        String ddl = getDDL(props);

        String expectedDDL = "CREATE FOREIGN TABLE Sheet1 (\n" +
                "	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" +
                "	FirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '7'),\n" +
                "	LastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '8'),\n" +
                "	Age double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '9'),\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" +
                ") OPTIONS (NAMEINSOURCE 'Sheet1', \"teiid_excel:FILE\" 'names.xls', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '15');";

        assertEquals(expectedDDL, ddl);
    }

    @Test
    public void testSchemaWithHeaderAndDataXLSX() throws Exception {
        Properties props = new Properties();
        props.setProperty("importer.excelFileName", "names.xlsx");
        props.setProperty("importer.headerRowNumber", "1");
        props.setProperty("importer.dataRowNumber", "3");
        String ddl = getDDL(props);
        String expectedDDL = "CREATE FOREIGN TABLE Sheet1 (\n" +
                "	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" +
                "	FirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" +
                "	LastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" +
                "	Age double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '3'),\n" +
                "	\"time\" timestamp OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '4'),\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" +
                ") OPTIONS (NAMEINSOURCE 'Sheet1', \"teiid_excel:FILE\" 'names.xlsx', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '3');";

        assertEquals(expectedDDL, ddl);
    }

    @Test
    public void testEmptySheetXLSX() throws Exception {
        Properties props = new Properties();
        props.setProperty("importer.excelFileName", "multi_sheet_empty_names.xlsx");
        props.setProperty("importer.headerRowNumber", "0");
        props.setProperty("importer.dataRowNumber", "2");
        String ddl = getDDL(props);
        String expectedDDL = "CREATE FOREIGN TABLE Sheet1 (\n" +
                "\tROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" +
                "\tFirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" +
                "\tLastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" +
                "\tAge double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '3'),\n" +
                "\tCONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n"+
                ") OPTIONS (NAMEINSOURCE 'Sheet1', \"teiid_excel:FILE\" 'multi_sheet_empty_names.xlsx', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '2');";

        assertEquals(expectedDDL, ddl);
    }

    @Test
    public void testDataTypeFromNullCell() throws Exception {
        Properties props = new Properties();
        props.setProperty("importer.excelFileName", "names.xlsx");
        props.setProperty("importer.headerRowNumber", "1");
        props.setProperty("importer.dataRowNumber", "2");
        String ddl = getDDL(props);
        String expectedDDL = "CREATE FOREIGN TABLE Sheet1 (\n" +
                "\tROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" +
                "\tFirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" +
                "\tLastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" +
                "\tAge double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '3'),\n" +
                "\t\"time\" timestamp OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '4'),\n"+
                "\tCONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" +
                ") OPTIONS (NAMEINSOURCE 'Sheet1', \"teiid_excel:FILE\" 'names.xlsx', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '2');";

        assertEquals(expectedDDL, ddl);
    }

}
