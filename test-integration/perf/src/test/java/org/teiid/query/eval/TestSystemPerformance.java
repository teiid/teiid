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

package org.teiid.query.eval;

import java.sql.Connection;
import java.sql.DatabaseMetaData;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.jdbc.AbstractQueryTest;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.HardCodedExecutionFactory;
import org.teiid.translator.TranslatorException;

@SuppressWarnings({"nls"})
public class TestSystemPerformance extends AbstractQueryTest {
	
	private static final int TABLES = 2000;
	private static final int COLS = 16;
	EmbeddedServer es;
	
	@Before public void setup() throws VirtualDatabaseException, ConnectorManagerException, TranslatorException {
		es = new EmbeddedServer();
		es.start(new EmbeddedConfiguration());
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
		};
		es.addTranslator("hc", hardCodedExecutionFactory);
		es.deployVDB("test", mmm);
	}
	
	@After public void teardown() {
		es.stop();
	}
	
	@Test public void testColumnPerformance() throws Exception {
		Connection c = es.getDriver().connect("jdbc:teiid:test", null);
		setConnection(c);
		DatabaseMetaData metadata = c.getMetaData();
		for (int i = 0; i < TABLES; i++) {
			internalResultSet = metadata.getColumns(null, "test", "x" + i, null);
			assertRowCount(COLS);
			internalResultSet.close();
		}
	}
	
}
