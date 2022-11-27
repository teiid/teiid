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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.file.JavaVirtualFile;
import org.teiid.file.VirtualFileConnection;
import org.teiid.language.Command;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;

@SuppressWarnings("nls")
public class TestExcelUpdateExecution {

    @Test
    public void testDeleteAll() throws Exception {
        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xls"));

        int[] results = helpExecute(TestExcelExecution.commonDDL, connection, "delete from Sheet1", true, "scratch_file.xls");
        assertArrayEquals(new int[] {6}, results);

        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getStratchFile("scratch_file.xls"));

        List<?> result = TestExcelExecution.helpExecute(TestExcelExecution.commonDDL, connection, "select * from sheet1");
        assertEquals(Collections.emptyList(), result);
    }

    @Test
    public void testDeleteSome() throws Exception {
        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xls"));

        int[] results = helpExecute(TestExcelExecution.commonDDL, connection, "delete from Sheet1 WHERE ROW_ID < 16", true, "scratch_file.xls");
        assertArrayEquals(new int[] {2}, results);

        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getStratchFile("scratch_file.xls"));

        List<?> result = TestExcelExecution.helpExecute(TestExcelExecution.commonDDL, connection, "select * from sheet1");
        assertEquals("[[16, Matt, Liek, 13.0, null], [17, Sarah, Byne, 10.0, null], [18, Rocky, Dog, 3.0, null], [19, Total, null, 26.0, null]]", result.toString());
    }

    @Test
    public void testInsert() throws Exception {
        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xls"));

        int[] results = helpExecute(TestExcelExecution.commonDDL, connection, "insert into sheet1 (FirstName, Age) values ('Prince', 55)", true, "scratch_file.xls");
        assertArrayEquals(new int[] {1}, results);

        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getStratchFile("scratch_file.xls"));

        List<?> result = TestExcelExecution.helpExecute(TestExcelExecution.commonDDL, connection, "select * from sheet1");
        assertEquals(7, result.size());
        assertEquals("[20, Prince, null, 55.0, null]", result.get(6).toString());
    }

    @Test
    public void testUpdate() throws Exception {
        VirtualFileConnection connection = Mockito.mock(VirtualFileConnection.class);
        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getFile("names.xls"));

        int[] results = helpExecute(TestExcelExecution.commonDDL, connection, "update sheet1 set age = 12 where row_id = 17", true, "scratch_file.xls");
        assertArrayEquals(new int[] {1}, results);

        Mockito.stub(connection.getFiles("names.xls")).toReturn(TestExcelExecution.getStratchFile("scratch_file.xls"));

        List<?> result = TestExcelExecution.helpExecute(TestExcelExecution.commonDDL, connection, "select * from sheet1");
        assertEquals("[[14, John, Doe, 44.0, null], [15, Jane, Smith, 40.0, null], [16, Matt, Liek, 13.0, null], [17, Sarah, Byne, 12.0, null], [18, Rocky, Dog, 3.0, null], [19, Total, null, 112.0, null]]", result.toString());
    }

    private int[] helpExecute(String ddl, VirtualFileConnection connection, String query, boolean format, String resultFile) throws Exception {
        ExcelExecutionFactory translator = new ExcelExecutionFactory();
        translator.setFormatStrings(format);
        translator.start();

        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "vdb", "excel");
        TranslationUtility utility = new TranslationUtility(metadata);

        Command cmd = utility.parseCommand(query);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);
        Mockito.stub(context.getCommandContext()).toReturn(new org.teiid.query.util.CommandContext());

        ExcelUpdateExecution execution = translator.createUpdateExecution(cmd, context, utility.createRuntimeMetadata(), connection);
        execution.setWriteTo(new JavaVirtualFile(UnitTestUtil.getTestScratchFile(resultFile)));
        try {
            execution.execute();
            return execution.getUpdateCounts();
        } finally {
            execution.close();
        }
    }

}
