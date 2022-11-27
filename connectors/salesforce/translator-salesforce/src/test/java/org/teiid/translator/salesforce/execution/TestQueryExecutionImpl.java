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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.salesforce.SalesForceExecutionFactory;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.SalesforceConnection.BatchResultInfo;
import org.teiid.translator.salesforce.SalesforceConnection.BulkBatchResult;
import org.teiid.translator.salesforce.execution.QueryExecutionImpl.BulkValidator;
import org.teiid.translator.salesforce.execution.visitors.TestVisitors;

import com.sforce.async.JobInfo;
import com.sforce.async.OperationEnum;
import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

@SuppressWarnings("nls")
public class TestQueryExecutionImpl {

    private static TranslationUtility translationUtility = new TranslationUtility(TestVisitors.exampleSalesforce());

    @Test public void testBatching() throws Exception {
        Select command = (Select)translationUtility.parseCommand("select Name from Account"); //$NON-NLS-1$
        SalesforceConnection sfc = Mockito.mock(SalesforceConnection.class);
        QueryResult qr = new QueryResult();
        SObject so = new SObject();
        so.setType("Account");
        so.addField("Name", null);
        qr.setRecords(new SObject[] {so});
        qr.setDone(false);
        QueryResult finalQr = new QueryResult();
        finalQr.setRecords(new SObject[] {so});
        finalQr.setDone(true);
        Mockito.stub(sfc.query("SELECT Name FROM Account", 0, false)).toReturn(qr);
        Mockito.stub(sfc.queryMore(null, 0)).toReturn(finalQr);
        QueryExecutionImpl qei = new QueryExecutionImpl(command, sfc, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class), new SalesForceExecutionFactory());
        qei.execute();
        assertNotNull(qei.next());
        assertNotNull(qei.next());
        assertNull(qei.next());
    }

    @Test public void testJoinChildToParent() throws Exception {
        Select command = (Select)translationUtility.parseCommand("select Account.Name, Contact.Id from Account inner join Contact on Account.Id = Contact.AccountId"); //$NON-NLS-1$
        SalesforceConnection sfc = Mockito.mock(SalesforceConnection.class);
        QueryResult qr = new QueryResult();
        SObject so = new SObject();
        so.setType("Account");
        so.addField("Name", "account name");
        SObject so1 = new SObject();
        so1.setType("Contact");
        so1.addField("Id", "contact id");
        so1.addField("Account", so);
        qr.setRecords(new SObject[] {so1});
        qr.setDone(true);
        Mockito.stub(sfc.query("SELECT Account.Name, Id FROM Contact WHERE AccountId != NULL", 0, false)).toReturn(qr);
        QueryExecutionImpl qei = new QueryExecutionImpl(command, sfc, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class), new SalesForceExecutionFactory());
        qei.execute();
        assertEquals(Arrays.asList("account name", "contact id"), qei.next());
        assertNull(qei.next());
    }

    @Test public void testJoinChildToGrandParent() throws Exception {
        Select command = (Select)translationUtility.parseCommand("select Account.Name, Contact.Id, a1.id from Contact left outer join Account on Account.Id = Contact.AccountId left outer join Account a1 on Account.parentid = a1.id"); //$NON-NLS-1$
        SalesforceConnection sfc = Mockito.mock(SalesforceConnection.class);
        QueryResult qr = new QueryResult();
        SObject so = new SObject();
        so.setType("Account");
        so.addField("Name", "account name");
        SObject so1 = new SObject();
        so1.setType("Contact");
        so1.addField("Id", "contact id");
        so1.addField("Account", so);
        qr.setRecords(new SObject[] {so1});
        qr.setDone(true);
        Mockito.stub(sfc.query("SELECT Account.Name, Id, Account.Parent.Id FROM Contact", 0, false)).toReturn(qr);
        QueryExecutionImpl qei = new QueryExecutionImpl(command, sfc, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class), new SalesForceExecutionFactory());
        qei.execute();
        assertEquals(Arrays.asList("account name", "contact id", null), qei.next());
        assertNull(qei.next());
    }

    @Test public void testJoinParentToChild() throws Exception {
        Select command = (Select)translationUtility.parseCommand("select Account.Name, Contact.Id from Account left outer join Contact on Account.Id = Contact.AccountId"); //$NON-NLS-1$
        SalesforceConnection sfc = Mockito.mock(SalesforceConnection.class);
        QueryResult qr = new QueryResult();
        SObject so = new SObject();
        so.setType("Account");
        so.addField("Name", "account name");
        SObject so1 = new SObject();
        so1.setType("Contact");
        so1.addField("Id", "contact id");
        SObject records = new SObject();
        records.addField("records", so1);
        so.addField("Contacts", records);
        qr.setRecords(new SObject[] {so});
        qr.setDone(true);
        Mockito.stub(sfc.query("SELECT Name, (SELECT Id FROM Contacts) FROM Account", 0, false)).toReturn(qr);
        QueryExecutionImpl qei = new QueryExecutionImpl(command, sfc, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class), new SalesForceExecutionFactory());
        qei.execute();
        assertEquals(Arrays.asList("account name", "contact id"), qei.next());
        assertNull(qei.next());
    }

    @Test public void testJoinOwnerWithLongPathTraversal() throws Exception {
        Select command = (Select)translationUtility.parseCommand("select Account.Name, Contact.Id, u.id from Contact left outer join Account on Account.Id = Contact.AccountId left outer join User_ as u on Account.OwnerId = u.id"); //$NON-NLS-1$
        SalesforceConnection sfc = Mockito.mock(SalesforceConnection.class);
        QueryResult qr = new QueryResult();
        SObject so = new SObject();
        so.setType("Account");
        so.addField("Name", "account name");
        SObject so1 = new SObject();
        so1.setType("Contact");
        so1.addField("Id", "contact id");
        so1.addField("Account", so);
        SObject so2 = new SObject();
        so2.setType("User");
        so2.addField("Id", "user id");
        so.addField("Owner", so2);
        qr.setRecords(new SObject[] {so1});
        qr.setDone(true);
        Mockito.stub(sfc.query("SELECT Account.Name, Id, Account.Owner.Id FROM Contact", 0, false)).toReturn(qr);
        QueryExecutionImpl qei = new QueryExecutionImpl(command, sfc, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class), new SalesForceExecutionFactory());
        qei.execute();
        assertEquals(Arrays.asList("account name", "contact id", "user id"), qei.next());
        assertNull(qei.next());
    }

    @Test public void testJoinParentToChildSelf() throws Exception {
        Select command = (Select)translationUtility.parseCommand("select a1.Name, a2.Id from Account a1 left outer join Account a2 on a1.Id = a2.ParentId"); //$NON-NLS-1$
        SalesforceConnection sfc = Mockito.mock(SalesforceConnection.class);
        QueryResult qr = new QueryResult();
        SObject so = new SObject();
        so.setType("Account");
        so.addField("Name", "account name");
        SObject so1 = new SObject();
        so1.setType("Account");
        so1.addField("Id", "account id1");
        so.addField("ChildAccounts", so1);
        SObject so2 = new SObject();
        so2.setType("Account");
        so2.addField("Id", "account id2");
        so.addField("ChildAccounts", so2);
        qr.setRecords(new SObject[] {so});
        qr.setDone(true);
        Mockito.stub(sfc.query("SELECT Name, (SELECT Id FROM ChildAccounts) FROM Account", 0, false)).toReturn(qr);
        QueryExecutionImpl qei = new QueryExecutionImpl(command, sfc, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class), new SalesForceExecutionFactory());
        qei.execute();
        assertEquals(Arrays.asList("account name", "account id1"), qei.next());
        assertEquals(Arrays.asList("account name", "account id2"), qei.next());
        assertNull(qei.next());
    }

    @BeforeClass static public void oneTimeSetup() {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT-06:00"));
    }

    @AfterClass static public void oneTimeTearDown() {
        TimeZone.setDefault(null);
    }

    @Test public void testValueParsing() throws Exception {
        assertEquals(TimestampUtil.createTime(2, 0, 0), QueryExecutionImpl.parseDateTime("08:00:00.000Z", Time.class, Calendar.getInstance()));
    }

    @Test public void testValueParsing1() throws Exception {
        assertEquals(TimestampUtil.createTimestamp(101, 0, 1, 2, 0, 0, 1000000), QueryExecutionImpl.parseDateTime("2001-01-01T08:00:00.001Z", Timestamp.class, Calendar.getInstance()));
    }

    @Test public void testRetrieve() throws Exception {
        Select command = (Select)translationUtility.parseCommand("select Name from Account where id = 'abc'"); //$NON-NLS-1$
        SalesforceConnection sfc = Mockito.mock(SalesforceConnection.class);

        Mockito.stub(sfc.retrieve("Name", "Account", Arrays.asList("abc"))).toReturn(new SObject[] {null});
        QueryExecutionImpl qei = new QueryExecutionImpl(command, sfc, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class), new SalesForceExecutionFactory());
        qei.execute();
        assertNull(qei.next());
    }

    @Test public void testBulkValidator() throws Exception {
        helpTestBulkValidator("select Name from Account where id = 'abc'", true, true);
        helpTestBulkValidator("select max(Name) from Account", false, false);
        helpTestBulkValidator("select Name from Account limit 1", true, false);
        helpTestBulkValidator("select Name from Account limit 1,1", false, false);
    }

    private void helpTestBulkValidator(String sql, boolean bulk, boolean pkChunk) {
        Select command = (Select)translationUtility.parseCommand(sql); //$NON-NLS-1$
        BulkValidator validator = new BulkValidator();
        validator.visit(command);
        assertEquals(bulk, validator.isBulkEligible());
        assertEquals(pkChunk, validator.usePkChunking());
    }

    @Test
    public void testBulkFlow() throws Exception {
        Select command = (Select)translationUtility.parseCommand("select Name from Account"); //$NON-NLS-1$

        SalesforceConnection connection = Mockito.mock(SalesforceConnection.class);
        JobInfo jobInfo = Mockito.mock(JobInfo.class);

        Mockito.when(connection.createBulkJob(Mockito.anyString(), Mockito.eq(OperationEnum.query), Mockito.eq(true))).thenReturn(jobInfo);

        final BatchResultInfo info = new BatchResultInfo("x");

        Mockito.when(connection.getBatchQueryResults(Mockito.anyString(), Mockito.eq(info))).thenAnswer(new Answer<BulkBatchResult>() {
            boolean first = true;
            @Override
            public BulkBatchResult answer(InvocationOnMock invocation)
                    throws Throwable {
                if (first) {
                    first = false;
                    throw new DataNotAvailableException();
                }
                if (info.getAndIncrementResultNum() == 0) {
                    final Iterator<List<String>> i = Arrays.asList(Arrays.asList("Name"), Arrays.asList("X")).iterator();
                    return new BulkBatchResult() {

                        @Override
                        public List<String> nextRecord() throws IOException {
                            if (!i.hasNext()) {
                                return null;
                            }
                            return i.next();
                        }

                        @Override
                        public void close() {

                        }
                    };
                }
                return null;
            }
        });

        Mockito.when(connection.addBatch("SELECT Name FROM Account", jobInfo)).thenReturn(info);

        ExecutionContext mock = Mockito.mock(ExecutionContext.class);
        Mockito.stub(mock.getSourceHint()).toReturn("bulk");

        QueryExecutionImpl execution = new QueryExecutionImpl(command, connection, Mockito.mock(RuntimeMetadata.class), mock, new SalesForceExecutionFactory());

        execution.execute();

        try {
            execution.next();
            fail();
        } catch (DataNotAvailableException e) {

        }
        List<?> row = execution.next();
        assertEquals(Arrays.asList("X"), row);
        assertNull(execution.next());
    }

}
