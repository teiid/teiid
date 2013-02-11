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
		Mockito.when(connection.executeBulkJob(Mockito.anyString(), Mockito.anyList())).thenReturn(Mockito.mock(JobInfo.class));
		Mockito.when(connection.getBulkResults(Mockito.any(JobInfo.class))).thenReturn(batchResult);
		
		SalesForceExecutionFactory config = new SalesForceExecutionFactory();
		config.setMaxBulkInsertBatchSize(1);
		
		InsertExecutionImpl updateExecution = new InsertExecutionImpl(config, insert, connection, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class));
		while(true) {
			try {
				updateExecution.execute();
				org.junit.Assert.assertArrayEquals(new int[] {3}, updateExecution.getUpdateCounts());
				break;
			} catch(DataNotAvailableException e) {
				continue;
			}
		}
		Mockito.verify(connection, Mockito.times(3)).executeBulkJob(Mockito.anyString(), Mockito.anyList());
		Mockito.verify(connection, Mockito.times(3)).getBulkResults(Mockito.any(JobInfo.class));
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
		Mockito.when(connection.executeBulkJob(Mockito.anyString(), Mockito.anyList())).thenReturn(Mockito.mock(JobInfo.class));
		Mockito.when(connection.getBulkResults(Mockito.any(JobInfo.class))).thenReturn(batchResult);
		
		SalesForceExecutionFactory config = new SalesForceExecutionFactory();
		config.setMaxBulkInsertBatchSize(1);
		
		InsertExecutionImpl updateExecution = new InsertExecutionImpl(config, insert, connection, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class));
		while(true) {
			try {
				updateExecution.execute();
				org.junit.Assert.assertArrayEquals(new int[] {2}, updateExecution.getUpdateCounts());
				break;
			} catch(DataNotAvailableException e) {
				continue;
			}
		}
		Mockito.verify(connection, Mockito.times(3)).executeBulkJob(Mockito.anyString(), Mockito.anyList());
		Mockito.verify(connection, Mockito.times(3)).getBulkResults(Mockito.any(JobInfo.class));
	}
}
