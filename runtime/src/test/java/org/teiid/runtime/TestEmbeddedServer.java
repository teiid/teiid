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

package org.teiid.runtime;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.runtime.EmbeddedServer.ConnectionFactoryProvider;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;

@SuppressWarnings("nls")
public class TestEmbeddedServer {
	EmbeddedServer es;
	
	@Before public void setup() {
		es = new EmbeddedServer();
	}
	
	@After public void teardown() {
		es.stop();
	}
	
	@Test public void testDeploy() throws Exception {
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		ec.setUseDisk(false);
		es.start(ec);
		
		es.addTranslator("y", new ExecutionFactory<AtomicInteger, Object> () {
			@Override
			public Object getConnection(AtomicInteger factory)
					throws TranslatorException {
				return factory.incrementAndGet();
			}
			
			@Override
			public void closeConnection(Object connection, AtomicInteger factory) {
				
			}
			
			@Override
			public void getMetadata(MetadataFactory metadataFactory, Object conn)
					throws TranslatorException {
				assertEquals(conn, Integer.valueOf(1));
				Table t = metadataFactory.addTable("my-table");
				metadataFactory.addColumn("my-column", TypeFacility.RUNTIME_NAMES.STRING, t);
			}
			
			@Override
			public ResultSetExecution createResultSetExecution(
					QueryExpression command, ExecutionContext executionContext,
					RuntimeMetadata metadata, Object connection)
					throws TranslatorException {
				ResultSetExecution rse = new ResultSetExecution() {
					
					@Override
					public void execute() throws TranslatorException {
						
					}
					
					@Override
					public void close() {
						
					}
					
					@Override
					public void cancel() throws TranslatorException {
						
					}
					
					@Override
					public List<?> next() throws TranslatorException, DataNotAvailableException {
						return null;
					}
				};
				return rse;
			}
		});
		final AtomicInteger counter = new AtomicInteger();
		ConnectionFactoryProvider<AtomicInteger> cfp = new ConnectionFactoryProvider<AtomicInteger>() {
			@Override
			public AtomicInteger getConnectionFactory()
					throws TranslatorException {
				return counter;
			}
		};
		
		es.addConnectionFactoryProvider("z", cfp);
		
		ModelMetaData mmd = new ModelMetaData();
		mmd.setName("my-schema");
		mmd.addSourceMapping("x", "y", "z");

		es.deployVDB("test", Arrays.asList(mmd));
		
		TeiidDriver td = es.getDriver();
		Connection c = td.connect("jdbc:teiid:test", null);
		Statement s = c.createStatement();
		ResultSet rs = s.executeQuery("select * from \"my-table\"");
		assertFalse(rs.next());
		assertEquals("my-column", rs.getMetaData().getColumnLabel(1));
	}

}
