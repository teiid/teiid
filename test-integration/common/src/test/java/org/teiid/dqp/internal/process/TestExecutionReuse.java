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
package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.internal.progress.OngoingStubbing;
import org.teiid.client.util.ResultsFuture;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.dqp.internal.datamgr.ConnectorManager;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.RequestOptions;
import org.teiid.jdbc.StatementCallback;
import org.teiid.jdbc.TeiidStatement;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.ReusableExecution;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestExecutionReuse {

	private static final int EXEC_COUNT = 3;
	private static FakeServer server;
	
	private static class FakeReusableExecution implements ResultSetExecution, ReusableExecution<Object> {

		@Override
		public List<?> next() throws TranslatorException,
				DataNotAvailableException {
			return null;
		}

		@Override
		public void cancel() throws TranslatorException {
		}

		@Override
		public void close() {
		}

		@Override
		public void execute() throws TranslatorException {
		}

		@Override
		public void dispose() {
		}

		@Override
		public void reset(Command c, ExecutionContext executionContext,
				Object connection) {
		}
		
	}
	
	private static FakeReusableExecution execution;
	
	@Before public void setup() throws DataNotAvailableException, TranslatorException {
		execution = Mockito.mock(FakeReusableExecution.class);
		OngoingStubbing stubbing = Mockito.stub(execution.next()).toReturn((List) Arrays.asList((Object)null)).toReturn(null);
		for (int i = 1; i < EXEC_COUNT; i++) {
			stubbing.toReturn((List<Object>) Arrays.asList((Object)null)).toReturn(null);
		}
	}
    
    @BeforeClass public static void oneTimeSetUp() throws Exception {
    	DQPConfiguration config = new DQPConfiguration();
    	config.setUserRequestSourceConcurrency(1);
    	server = new FakeServer(config);
		server.setConnectorManagerRepository(new ConnectorManagerRepository() {
			private ConnectorManager cm = new ConnectorManager("x", "y") {
				private ExecutionFactory<Object, Object> ef = new ExecutionFactory<Object, Object>() {
					
					@Override
					public ResultSetExecution createResultSetExecution(
							QueryExpression command,
							ExecutionContext executionContext,
							RuntimeMetadata metadata, Object connection)
							throws TranslatorException {
						return execution;
					};
				};
				@Override
				public ExecutionFactory<Object, Object> getExecutionFactory() {
					return ef;
				}
				
				@Override
				protected Object getConnectionFactory()
						throws TranslatorException {
					return null;
				}
			};
			@Override
			public ConnectorManager getConnectorManager(String connectorName) {
				return cm;
			}
		});
		server.deployVDB("PartsSupplier", UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
    }
    
    @AfterClass public static void oneTimeTearDown() throws Exception {
    	server.stop();
    }
    
	@Test public void testReusableAsynchContinuous() throws Exception {
		Connection c = server.createConnection("jdbc:teiid:partssupplier");
		Statement s = c.createStatement();
		TeiidStatement ts = s.unwrap(TeiidStatement.class);
		final ResultsFuture<Integer> result = new ResultsFuture<Integer>(); 
		ts.submitExecute("select part_id from parts", new StatementCallback() {
			int rowCount;
			@Override
			public void onRow(Statement s, ResultSet rs) throws SQLException {
				rowCount++;
				if (rowCount == EXEC_COUNT) {
					s.close();
				}
			}
			
			@Override
			public void onException(Statement s, Exception e) {
				result.getResultsReceiver().receiveResults(rowCount);
			}
			
			@Override
			public void onComplete(Statement s) {
				result.getResultsReceiver().receiveResults(rowCount);
			}
		}, new RequestOptions().continuous(true));
		assertEquals(EXEC_COUNT, result.get().intValue());
		Mockito.verify(execution, Mockito.times(1)).dispose();
		Mockito.verify(execution, Mockito.times(EXEC_COUNT)).execute();
		Mockito.verify(execution, Mockito.times(EXEC_COUNT)).close();
		Mockito.verify(execution, Mockito.times(EXEC_COUNT - 1)).reset((Command)Mockito.anyObject(), (ExecutionContext)Mockito.anyObject(), Mockito.anyObject());
	}

}
