/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2009 Red Hat, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.jdbc.AbstractQueryTest;
import org.teiid.jdbc.HardCodedExecutionFactory;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

@SuppressWarnings({"nls"})
public class TestSort extends AbstractQueryTest {
	
	private static final int TABLES = 1;
	private static final int COLS = 100;
	private static final int ROWS = 250000;
	
	EmbeddedServer es;
	static Random r = new Random(1);
	
	@Before public void setup() throws VirtualDatabaseException, ConnectorManagerException, TranslatorException {
		es = new EmbeddedServer();
		//es.bufferService.setUseDisk(false);
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		//ec.setUseDisk(false);
		//UnitTestUtil.enableTraceLogging("org.teiid");
		es.start(ec);
		ModelMetaData mmm = new ModelMetaData();
		mmm.setName("test");
		mmm.setSchemaSourceType("native");
		mmm.addSourceMapping("x", "hc", null);
		HardCodedExecutionFactory hardCodedExecutionFactory = new HardCodedExecutionFactory() {
			@Override
			public void getMetadata(MetadataFactory metadataFactory, Object conn)
					throws TranslatorException {
				String[] colNames = new String[COLS];
				for (int i = 0; i < colNames.length; i++) {
					colNames[i] = "col" + i;
				}
				for (int i = 0; i < TABLES; i++) {
					Table t = metadataFactory.addTable("x" + i);
					for (int j = 0; j < COLS; j++) {
						metadataFactory.addColumn(colNames[j], "string", t);
					}
				}
			}
			
			@Override
			public boolean isSourceRequiredForMetadata() {
				return false;
			}
			
			@Override
			public ResultSetExecution createResultSetExecution(
					QueryExpression command, ExecutionContext executionContext,
					RuntimeMetadata metadata, Object connection)
					throws TranslatorException {
				return new ResultSetExecution() {
					
					int i = 0;
					
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
						if (i++ < ROWS) {
							ArrayList<Object> result = new ArrayList<Object>();
							for (int j = 0; j < COLS; j++) {
								if (j == 0) {
									result.add(String.valueOf(r.nextInt()));
								} else {
									result.add(i + "abcdefghijklmnop" + j);
								}
							}
							return result;
						}
						return null;
					}
				};
			}
		};
		es.addTranslator("hc", hardCodedExecutionFactory);
		es.deployVDB("test", mmm);
	}
	
	@After public void teardown() {
		es.stop();
	}
	
	@Test public void testSomething() throws Exception {
		Connection c = es.getDriver().connect("jdbc:teiid:test", null);
		long start = System.currentTimeMillis();
		ResultSet rs = c.createStatement().executeQuery("select * from x0 order by col0");
		int i = 0;
		while(rs.next()) {
			i++;
			/*if (i%1000==0) {
				System.out.println(i);
			}*/
		}
		System.out.println(System.currentTimeMillis() - start);
	}
	
	@Test public void testSomething1() throws Exception {
		Connection c = es.getDriver().connect("jdbc:teiid:test", null);
		long start = System.currentTimeMillis();
		Statement s = c.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
		ResultSet rs = s.executeQuery("select * from x0");
		int i = 0;
		while(rs.next()) {
			i++;
			/*if (i%1000==0) {
				System.out.println(i);
			}*/
		}
		System.out.println(System.currentTimeMillis() - start);
	}
	
}
