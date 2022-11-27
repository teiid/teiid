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

package org.teiid.translator.google;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.google.api.GoogleSpreadsheetConnection;
import org.teiid.translator.google.api.metadata.SpreadsheetInfo;
import org.teiid.translator.google.api.result.RowsResult;
import org.teiid.translator.google.api.result.SheetRow;

public class TestNativeSpreadsheet {

    @Test public void testDirect() throws TranslatorException {
        SpreadsheetExecutionFactory sef = new SpreadsheetExecutionFactory();
        sef.setSupportsDirectQueryProcedure(true);

        String input = "call native('worksheet=x;query=$1 foo;limit=2', 'a')";

        TranslationUtility util = FakeTranslationFactory.getInstance().getExampleTranslationUtility();
        Command command = util.parseCommand(input);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        GoogleSpreadsheetConnection connection = Mockito.mock(GoogleSpreadsheetConnection.class);
        SpreadsheetInfo info = new SpreadsheetInfo();
        info.createWorksheet("x");
        Mockito.stub(connection.getSpreadsheetInfo()).toReturn(info);

        RowsResult result = Mockito.mock(RowsResult.class);
        Mockito.stub(result.iterator()).toReturn(Arrays.asList(new SheetRow()).iterator());
        Mockito.stub(connection.executeQuery(info.getWorksheetByName("x"), "'a' foo", null, 2, 0)).toReturn(result);

        ResultSetExecution execution = (ResultSetExecution)sef.createExecution(command, ec, rm, connection);
        execution.execute();

        List<?> vals = execution.next();
        assertTrue(vals.get(0) instanceof Object[]);
    }

}
