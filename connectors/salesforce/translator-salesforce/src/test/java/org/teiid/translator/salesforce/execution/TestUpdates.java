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

import static org.junit.Assert.assertEquals;

import java.sql.Time;
import java.util.List;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Delete;
import org.teiid.language.Update;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.Util;
import org.teiid.translator.salesforce.execution.visitors.TestVisitors;

import com.sforce.async.BatchResult;
import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.async.Result;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

@SuppressWarnings("nls")
public class TestUpdates {

    private static TranslationUtility translationUtility = new TranslationUtility(TestVisitors.exampleSalesforce());

    @BeforeClass
    public static void oneTimeSetup() {
        Util.resetTimeZone();
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT-1"));
    }

    @AfterClass
    public static void oneTimeTeardown() {
        Util.resetTimeZone();
        TimestampWithTimezone.resetCalendar(null);
    }

    @Test
    public void testIds() throws Exception {
        Delete delete = (Delete) translationUtility.parseCommand("delete from contacts");

        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);

        SalesForceExecutionFactory config = new SalesForceExecutionFactory();

        DeleteExecutionImpl updateExecution = new DeleteExecutionImpl(config, delete, connection, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class));

        ArgumentCaptor<String> queryArgument = ArgumentCaptor.forClass(String.class);
        QueryResult qr = new QueryResult();
        SObject so = new SObject();
        so.setType("Contact");
        so.addField("Id", "x");
        qr.setRecords(new SObject[] {so});
        qr.setSize(1);
        qr.setDone(true);

        Mockito.stub(connection.query(queryArgument.capture(), Mockito.anyInt(), Mockito.anyBoolean())).toReturn(qr);
        Mockito.stub(connection.delete(new String[] {"x"})).toReturn(1);

        while(true) {
            try {
                updateExecution.execute();
                org.junit.Assert.assertArrayEquals(new int[] {1}, updateExecution.getUpdateCounts());
                break;
            } catch(DataNotAvailableException e) {
                continue;
            }
        }

        Mockito.verify(connection, Mockito.times(1)).query(queryArgument.capture(), Mockito.anyInt(), Mockito.anyBoolean());

        String query = queryArgument.getValue();
        assertEquals("SELECT Id FROM Contact ", query);

    }

    @Test
    public void testIdsIn() throws Exception {
        Delete delete = (Delete) translationUtility.parseCommand("delete from contacts where contactid in ('123', '456')");

        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);

        SalesForceExecutionFactory config = new SalesForceExecutionFactory();

        Mockito.stub(connection.delete(new String[] {"123", "456"})).toReturn(2);

        DeleteExecutionImpl updateExecution = new DeleteExecutionImpl(config, delete, connection, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class));

        while(true) {
            try {
                updateExecution.execute();
                org.junit.Assert.assertArrayEquals(new int[] {2}, updateExecution.getUpdateCounts());
                break;
            } catch(DataNotAvailableException e) {
                continue;
            }
        }
    }

    @Test
    public void testBulkIds() throws Exception {
        Delete delete = (Delete) translationUtility.parseCommand("delete from contacts where name like '_a'");

        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);

        SalesForceExecutionFactory config = new SalesForceExecutionFactory();

        ExecutionContext mock = Mockito.mock(ExecutionContext.class);
        Mockito.stub(mock.getSourceHint()).toReturn("bulk");

        DeleteExecutionImpl updateExecution = new DeleteExecutionImpl(config, delete, connection, Mockito.mock(RuntimeMetadata.class), mock);

        ArgumentCaptor<String> queryArgument = ArgumentCaptor.forClass(String.class);
        QueryResult qr = new QueryResult();
        SObject so = new SObject();
        so.setType("Contact");
        so.addField("Id", "x");
        qr.setRecords(new SObject[] {so});
        qr.setSize(1);
        qr.setDone(true);

        Mockito.stub(connection.query(queryArgument.capture(), Mockito.anyInt(), Mockito.anyBoolean())).toReturn(qr);

        JobInfo jobInfo = Mockito.mock(JobInfo.class);

        Result r1 = Mockito.mock(Result.class);
        Result r2 = Mockito.mock(Result.class);
        Result r3 = Mockito.mock(Result.class);
        //two successes for an update count of 2
        Mockito.when(r1.isSuccess()).thenReturn(true);
        Mockito.when(r2.isSuccess()).thenReturn(true);
        Mockito.when(r3.isSuccess()).thenReturn(false);
        Mockito.when(r3.getErrors()).thenReturn(new com.sforce.async.Error[0]);

        BatchResult batchResult = Mockito.mock(BatchResult.class);
        Mockito.when(batchResult.getResult()).thenReturn(new Result[] {r1}).thenReturn((new Result[] {r2})).thenReturn(new Result[] {r3});

        Mockito.when(connection.createBulkJob(Mockito.anyString(), Mockito.eq(OperationEnum.delete), Mockito.eq(false))).thenReturn(jobInfo);
        Mockito.when(connection.getBulkResults(Mockito.any(JobInfo.class), Mockito.anyList())).thenReturn(new BatchResult[] {batchResult, batchResult, batchResult});

        while(true) {
            try {
                updateExecution.execute();
                org.junit.Assert.assertArrayEquals(new int[] {2}, updateExecution.getUpdateCounts());
                break;
            } catch(DataNotAvailableException e) {
                continue;
            }
        }

        Mockito.verify(connection, Mockito.times(1)).query(queryArgument.capture(), Mockito.anyInt(), Mockito.anyBoolean());

        String query = queryArgument.getValue();
        assertEquals("SELECT Id FROM Contact WHERE ContactName LIKE '_a' ", query);
    }

    @Test
    public void testEmptyBulkIds() throws Exception {
        Delete delete = (Delete) translationUtility.parseCommand("delete from contacts where name = 'abc'");

        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);

        SalesForceExecutionFactory config = new SalesForceExecutionFactory();

        ExecutionContext mock = Mockito.mock(ExecutionContext.class);
        Mockito.stub(mock.getSourceHint()).toReturn("bulk");

        DeleteExecutionImpl updateExecution = new DeleteExecutionImpl(config, delete, connection, Mockito.mock(RuntimeMetadata.class), mock);

        ArgumentCaptor<String> queryArgument = ArgumentCaptor.forClass(String.class);
        QueryResult qr = new QueryResult();
        qr.setSize(0);
        qr.setDone(true);

        Mockito.stub(connection.query(queryArgument.capture(), Mockito.anyInt(), Mockito.anyBoolean())).toReturn(qr);

        while(true) {
            try {
                updateExecution.execute();
                org.junit.Assert.assertArrayEquals(new int[] {0}, updateExecution.getUpdateCounts());
                break;
            } catch(DataNotAvailableException e) {
                continue;
            }
        }

        Mockito.verify(connection, Mockito.times(1)).query(queryArgument.capture(), Mockito.anyInt(), Mockito.anyBoolean());

        String query = queryArgument.getValue();
        assertEquals("SELECT Id FROM Contact WHERE ContactName = 'abc' ", query);
    }

    @Test
    public void testUpdateValues() throws Exception {
        TranslationUtility tu = new TranslationUtility(RealMetadataFactory.fromDDL(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("sf.ddl")), "sf", "sf"));
        Update update = (Update) tu.parseCommand("update contact set birthdate = DATE'2000-01-01'");

        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);

        SalesForceExecutionFactory config = new SalesForceExecutionFactory();

        UpdateExecutionImpl updateExecution = new UpdateExecutionImpl(config, update, connection, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class));

        ArgumentCaptor<String> queryArgument = ArgumentCaptor.forClass(String.class);
        QueryResult qr = new QueryResult();
        SObject so = new SObject();
        so.setType("Contact");
        so.addField("Id", "x");
        qr.setRecords(new SObject[] {so});
        qr.setSize(1);
        qr.setDone(true);

        ArgumentCaptor<List> data = ArgumentCaptor.forClass(List.class);

        Mockito.stub(connection.query(queryArgument.capture(), Mockito.anyInt(), Mockito.anyBoolean())).toReturn(qr);
        Mockito.stub(connection.update(data.capture())).toReturn(1);

        while(true) {
            try {
                updateExecution.execute();
                org.junit.Assert.assertArrayEquals(new int[] {1}, updateExecution.getUpdateCounts());
                break;
            } catch(DataNotAvailableException e) {
                continue;
            }
        }

        Mockito.verify(connection, Mockito.times(1)).query(queryArgument.capture(), Mockito.anyInt(), Mockito.anyBoolean());

        String query = queryArgument.getValue();
        assertEquals("SELECT Id FROM Contact ", query);

        List<DataPayload> payloads = data.getValue();
        assertEquals(TimestampUtil.createDate(100, 0, 1), payloads.get(0).getMessageElements().get(0).value);

        //spot check boolean and time values
        assertEquals(Boolean.TRUE, Util.toSalesforceObjectValue(Boolean.TRUE, Boolean.class));
        assertEquals(new com.sforce.ws.types.Time("02:01:01.000Z"), Util.toSalesforceObjectValue(TimestampUtil.createTime(1, 1, 1), Time.class));
    }
}
