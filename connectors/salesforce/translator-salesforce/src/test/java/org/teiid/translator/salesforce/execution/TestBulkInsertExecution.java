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
package org.teiid.translator.salesforce.execution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.types.DataTypeManager;
import org.teiid.language.ColumnReference;
import org.teiid.language.Expression;
import org.teiid.language.ExpressionValueSource;
import org.teiid.language.Insert;
import org.teiid.language.NamedTable;
import org.teiid.language.Parameter;
import org.teiid.metadata.Column;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesforceConnection;

import com.sforce.async.BatchResult;
import com.sforce.async.Error;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.async.Result;
import com.sforce.async.StatusCode;

@SuppressWarnings("nls")
public class TestBulkInsertExecution {

    @Test
    public void testFlowAndInvocationStack() throws Exception {
        NamedTable table = new NamedTable("temp", null, Mockito.mock(Table.class));

        ArrayList<ColumnReference> elements = new ArrayList<ColumnReference>();
        elements.add(new ColumnReference(table, "one", Mockito.mock(Column.class), Integer.class));
        elements.add(new ColumnReference(table, "two", Mockito.mock(Column.class), String.class));

        List<Expression> values = new ArrayList<Expression>();
        Parameter param = new Parameter();
        param.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        param.setValueIndex(0);
        values.add(param);

        param = new Parameter();
        param.setType(DataTypeManager.DefaultDataClasses.STRING);
        param.setValueIndex(1);
        values.add(param);

        ExpressionValueSource valueSource = new ExpressionValueSource(values);

        Insert insert = new Insert(table, elements, valueSource);
        insert.setParameterValues(Arrays.asList(Arrays.asList(2, '2'), Arrays.asList(2, '2'), Arrays.asList(3, '3')).iterator());

        Result r1 = Mockito.mock(Result.class);
        Result r2 = Mockito.mock(Result.class);
        Result r3 = Mockito.mock(Result.class);
        Mockito.when(r1.isSuccess()).thenReturn(true);
        Mockito.when(r1.isCreated()).thenReturn(true);
        Mockito.when(r2.isSuccess()).thenReturn(true);
        Mockito.when(r2.isCreated()).thenReturn(true);
        Mockito.when(r3.isSuccess()).thenReturn(true);
        Mockito.when(r3.isCreated()).thenReturn(true);

        BatchResult batchResult = Mockito.mock(BatchResult.class);
        Mockito.when(batchResult.getResult()).thenReturn(new Result[] {r1}).thenReturn((new Result[] {r2})).thenReturn(new Result[] {r3});

        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);
        JobInfo jobInfo = Mockito.mock(JobInfo.class);
        Mockito.when(connection.createBulkJob(Mockito.anyString(), Mockito.eq(OperationEnum.insert), Mockito.eq(false))).thenReturn(jobInfo);
        Mockito.when(connection.getBulkResults(Mockito.any(JobInfo.class), Mockito.anyList())).thenReturn(new BatchResult[] {batchResult, batchResult, batchResult});

        SalesForceExecutionFactory config = new SalesForceExecutionFactory();
        config.setMaxBulkInsertBatchSize(1);

        InsertExecutionImpl updateExecution = new InsertExecutionImpl(config, insert, connection, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class));
        while(true) {
            try {
                updateExecution.execute();
                org.junit.Assert.assertArrayEquals(new int[] {1, 1, 1}, updateExecution.getUpdateCounts());
                break;
            } catch(DataNotAvailableException e) {
                continue;
            }
        }
        Mockito.verify(connection, Mockito.times(1)).createBulkJob(Mockito.anyString(), Mockito.eq(OperationEnum.insert), Mockito.eq(false));
        Mockito.verify(connection, Mockito.times(1)).getBulkResults(Mockito.any(JobInfo.class), Mockito.anyList());
    }


    @Test
    public void testFlowAndErrorReturn() throws Exception {
        NamedTable table = new NamedTable("temp", null, Mockito.mock(Table.class));

        ArrayList<ColumnReference> elements = new ArrayList<ColumnReference>();
        elements.add(new ColumnReference(table, "one", Mockito.mock(Column.class), Integer.class));
        elements.add(new ColumnReference(table, "two", Mockito.mock(Column.class), String.class));

        List<Expression> values = new ArrayList<Expression>();
        Parameter param = new Parameter();
        param.setType(DataTypeManager.DefaultDataClasses.INTEGER);
        param.setValueIndex(0);
        values.add(param);

        param = new Parameter();
        param.setType(DataTypeManager.DefaultDataClasses.STRING);
        param.setValueIndex(1);
        values.add(param);

        ExpressionValueSource valueSource = new ExpressionValueSource(values);

        Insert insert = new Insert(table, elements, valueSource);
        insert.setParameterValues(Arrays.asList(Arrays.asList(2, '2'), Arrays.asList(2, '2'), Arrays.asList(3, '3')).iterator());

        Result r1 = Mockito.mock(Result.class);
        Result r2 = Mockito.mock(Result.class);
        Result r3 = Mockito.mock(Result.class);
        Mockito.when(r1.isSuccess()).thenReturn(true);
        Mockito.when(r1.isCreated()).thenReturn(true);
        Mockito.when(r2.isSuccess()).thenReturn(true);
        Mockito.when(r2.isCreated()).thenReturn(true);
        Mockito.when(r3.isSuccess()).thenReturn(false);
        Mockito.when(r3.isCreated()).thenReturn(false);
        com.sforce.async.Error error = new com.sforce.async.Error();
        error.setMessage("failed, check your data");
        error.setStatusCode(StatusCode.CANNOT_DISABLE_LAST_ADMIN);
        Mockito.when(r3.getErrors()).thenReturn(new Error[] {error});

        BatchResult batchResult = Mockito.mock(BatchResult.class);
        Mockito.when(batchResult.getResult()).thenReturn(new Result[] {r1}).thenReturn((new Result[] {r2})).thenReturn(new Result[] {r3});

        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);
        JobInfo jobInfo = Mockito.mock(JobInfo.class);
        Mockito.when(connection.createBulkJob(Mockito.anyString(), Mockito.eq(OperationEnum.insert), Mockito.eq(false))).thenReturn(jobInfo);
        Mockito.when(connection.getBulkResults(Mockito.any(JobInfo.class), Mockito.anyList())).thenReturn(new BatchResult[] {batchResult, batchResult, batchResult});

        SalesForceExecutionFactory config = new SalesForceExecutionFactory();
        config.setMaxBulkInsertBatchSize(1);

        InsertExecutionImpl updateExecution = new InsertExecutionImpl(config, insert, connection, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class));
        while(true) {
            try {
                updateExecution.execute();
                org.junit.Assert.assertArrayEquals(new int[] {1, 1, -3}, updateExecution.getUpdateCounts());
                break;
            } catch(DataNotAvailableException e) {
                continue;
            }
        }
        Mockito.verify(connection, Mockito.times(1)).createBulkJob(Mockito.anyString(), Mockito.eq(OperationEnum.insert), Mockito.eq(false));
        Mockito.verify(connection, Mockito.times(1)).getBulkResults(Mockito.any(JobInfo.class), Mockito.anyList());
    }
}
