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

package org.teiid.translator.salesforce.execution;

import static org.junit.Assert.*;

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
		Mockito.stub(sfc.query("SELECT Account.Name FROM Account", 0, false)).toReturn(qr);
		Mockito.stub(sfc.queryMore(null, 0)).toReturn(finalQr);
		QueryExecutionImpl qei = new QueryExecutionImpl(command, sfc, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class), new SalesForceExecutionFactory());
		qei.execute();
		assertNotNull(qei.next());
		assertNotNull(qei.next());
		assertNull(qei.next());
	}
	
	@Test public void testJoin() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select Account.Name, Contact.Id from Account inner join Contact on Account.Id = Contact.AccountId"); //$NON-NLS-1$
		SalesforceConnection sfc = Mockito.mock(SalesforceConnection.class);
		QueryResult qr = new QueryResult();
		SObject so = new SObject();
		so.setType("Account");
		so.addField("Name", "account name");
		SObject so1 = new SObject();
		so1.setType("Contact");
		so1.addField("Id", "contact id");
		so.addField("Contacts", so1);
		qr.setRecords(new SObject[] {so});
		qr.setDone(true);
		Mockito.stub(sfc.query("SELECT Account.Name, Contact.Id FROM Contact WHERE Contact.AccountId != NULL", 0, false)).toReturn(qr);
		QueryExecutionImpl qei = new QueryExecutionImpl(command, sfc, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class), new SalesForceExecutionFactory());
		qei.execute();
		assertEquals(Arrays.asList("account name", "contact id"), qei.next());
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
		
		Mockito.stub(sfc.retrieve("Account.Name", "Account", Arrays.asList("abc"))).toReturn(new SObject[] {null});
		QueryExecutionImpl qei = new QueryExecutionImpl(command, sfc, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class), new SalesForceExecutionFactory());
		qei.execute();
		assertNull(qei.next());
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
		
		Mockito.when(connection.addBatch("SELECT Account.Name FROM Account", jobInfo)).thenReturn(info);
			
		ExecutionContext mock = Mockito.mock(ExecutionContext.class);
		Mockito.stub(mock.getSourceHints()).toReturn(Arrays.asList("bulk"));
		
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
