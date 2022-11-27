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

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.apache.poi.util.LocaleUtil;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.file.JavaVirtualFile;
import org.teiid.file.VirtualFile;
import org.teiid.file.VirtualFileConnection;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestExcelExecution {

    static ArrayList helpExecute(String ddl, VirtualFileConnection connection, String query) throws Exception {
        return helpExecute(ddl, connection, query, false);
    }

    static ArrayList helpExecute(String ddl, VirtualFileConnection connection, String query, boolean format) throws Exception {
        ExcelExecutionFactory translator = new ExcelExecutionFactory();
        translator.setFormatStrings(format);
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

        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xls"));

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

        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xlsx")).toReturn(TestExcelExecution.getFile("names.xlsx"));

        ArrayList results = helpExecute(ddl, connection, "select * from Sheet1");
        assertEquals("[[1, FirstName, LastName, Age], [2, John, Doe, null], [3, Jane, Smith, 40.0], [4, Matt, Liek, 13.0], [5, Sarah, Byne, 10.0], [6, Rocky, Dog, 3.0]]", results.toString());
    }

    @Test
    public void testExecutionColumnWithNullCell() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Sheet1 (\n" +
                "	column1 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1')\n" +
                ") OPTIONS (\"teiid_excel:FILE\" '3219.xlsx');";

        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("3219.xlsx")).toReturn(TestExcelExecution.getFile("3219.xlsx"));

        ArrayList results = helpExecute(ddl, connection, "select * from Sheet1");
        assertEquals(results.size(), 7);
    }

    /**
     * Test a sheet with a header row where 1 column is empty
     * @throws Exception
     */
    @Test
    public void testExecutionHeaderWithEmptyCell() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Sheet1 (\n" +
                "	ROW_ID integer OPTIONS (SEARCHABLE 'All_Except_Like', \"teiid_excel:CELL_NUMBER\" 'ROW_ID'),\n" +
                "	FirstName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" +
                "	LastName string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2'),\n" +
                "	Age double OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '4'),\n" +
                "	CONSTRAINT PK0 PRIMARY KEY(ROW_ID)\n" +
                ") OPTIONS (NAMEINSOURCE 'Sheet1', \"teiid_excel:FILE\" 'empty-ignore.xls', \"teiid_excel:FIRST_DATA_ROW_NUMBER\" '2');";

        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("empty-ignore.xls")).toReturn(TestExcelExecution.getFile("empty-ignore.xls"));

        ArrayList results = helpExecute(ddl, connection, "select * from Sheet1");
        assertEquals(6, results.size());
        ArrayList row = (ArrayList) results.get(4);
        assertEquals(4, row.size());
    }

    @Test
    public void testExecutionColumnsWithNullCell() throws Exception {
        String ddl = "CREATE FOREIGN TABLE Sheet1 (\n" +
                "	column1 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '1'),\n" +
                "	column2 string OPTIONS (SEARCHABLE 'Unsearchable', \"teiid_excel:CELL_NUMBER\" '2')\n" +
                ") OPTIONS (\"teiid_excel:FILE\" '3219.xlsx');";

        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("3219.xlsx")).toReturn(TestExcelExecution.getFile("3219.xlsx"));

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

        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xls"));

        ArrayList results = helpExecute(ddl, connection, "select * from Sheet1");
        assertEquals("[[18, Rocky, Dog, 3.0], [19, Total, null, 110.0]]", results.toString());

        results = helpExecute(ddl, connection, "select * from Sheet1", true);
        assertEquals("[[18, Rocky, Dog, 3], [19, Total, null, 110]]", results.toString());
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

        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xlsx")).toReturn(TestExcelExecution.getFile("names.xlsx"));

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

        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xls"));

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
        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xls"));

        ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID=16");
        assertEquals("[[Matt]]", results.toString());
    }

    @Test
    public void testExecutionGT() throws Exception {
        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xls"));

        ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID>16");
        assertEquals("[[Sarah], [Rocky], [Total]]", results.toString());
    }

    @Test
    public void testExecutionGE() throws Exception {
        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xls"));

        ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID>=16");
        assertEquals("[[Matt], [Sarah], [Rocky], [Total]]", results.toString());
    }

    @Test
    public void testExecutionLT() throws Exception {
        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xls"));

        ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID < 16");
        assertEquals("[[John], [Jane]]", results.toString());
    }

    @Test
    public void testExecutionLE() throws Exception {
        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xls"));

        ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID <= 16");
        assertEquals("[[John], [Jane], [Matt]]", results.toString());
    }

    @Test
    public void testExecutionNE() throws Exception {
        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xls"));

        ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID != 16");
        assertEquals("[[John], [Jane], [Sarah], [Rocky], [Total]]", results.toString());
    }

    @Test
    public void testExecutionAnd() throws Exception {
        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xls"));

        ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID > 16 and ROW_ID < 18");
        assertEquals("[[Sarah]]", results.toString());
    }

    @Test
    public void testExecutionIN() throws Exception {
        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xls"));

        ArrayList results = helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID IN (13, 18)");
        assertEquals("[[John], [Total]]", results.toString());
    }

    @Test
    public void testStartBeyondRows() throws Exception {
        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xlsx"));

        ArrayList results = helpExecute(commonDDL, connection, "select * from Sheet1");
        //typed as time
        assertEquals("[]", results.toString());
    }

    @Test
    public void testTime() throws Exception {
        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xlsx"));

        String ddl = commonDDL.replace("14", "6");
        ArrayList results = helpExecute(ddl, connection, "select \"time\" from Sheet1");
        //typed as time
        assertEquals("[[10:12:14]]", results.toString());

        ddl = ddl.replace("\"time\" time", "\"time\" string");
        results = helpExecute(ddl, connection, "select \"time\" from Sheet1", true);
        //typed as string with formatting - Excel format
        assertEquals("[[10:12:14 AM]]", results.toString());


        TimeZone timeZone = TimeZone.getTimeZone("America/New_York");
        TimeZone defaultTz = TimeZone.getDefault();
        TimestampWithTimezone.resetCalendar(timeZone); //$NON-NLS-1$
        LocaleUtil.setUserTimeZone(timeZone);
        try {
            results = helpExecute(ddl, connection, "select \"time\" from Sheet1", false);
            //typed as string without formatting - SQL / Teiid format
            //note this seems like an issue with poi - if the sheet is not using 1904 dates, then it will start the calendar at the 0 day, not the first.
            //however this behavior is slightly better than the previous, which would have shown the numeric value instead
            assertEquals("[[1899-12-31 10:12:14.0]]", results.toString());
        } finally {
            TimestampWithTimezone.resetCalendar(null);
            LocaleUtil.setUserTimeZone(defaultTz);
        }
    }

    @Test(expected=TranslatorException.class)
    public void testExecutionNoFile() throws Exception {
        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xlsx")).toReturn(TestExcelExecution.getFile("names.xlsx"));

        helpExecute(commonDDL, connection, "select FirstName from Sheet1 WHERE ROW_ID != 16");
    }

    static VirtualFile[] getFile(String name) {
        return new JavaVirtualFile[] {new JavaVirtualFile(UnitTestUtil.getTestDataFile(name))};
    }

    static VirtualFile[] getStratchFile(String name) {
        return new JavaVirtualFile[] {new JavaVirtualFile(UnitTestUtil.getTestScratchFile(name))};
    }
}
