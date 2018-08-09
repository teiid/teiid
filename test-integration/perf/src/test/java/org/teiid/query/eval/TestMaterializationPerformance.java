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

import static org.junit.Assert.*;

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractQueryTest;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.HardCodedExecutionFactory;

@SuppressWarnings({"nls"})
public class TestMaterializationPerformance extends AbstractQueryTest {
	
	EmbeddedServer es;
	
	@Before public void setup() {
		es = new EmbeddedServer();
		es.start(new EmbeddedConfiguration());
	}
	
	@After public void teardown() {
		es.stop();
	}
	
	@Test public void testIndexPerformance() throws Exception {
		ModelMetaData mmm = new ModelMetaData();
		mmm.setName("test");
		mmm.setSchemaSourceType("ddl");
		mmm.setSchemaText("create foreign table x (col1 integer, col2 string); " +
				"create view matx (col1 integer, col2 string, constraint idx index (col2)) options (materialized true) as select * from x;");
		mmm.addSourceMapping("x", "hc", null);
		HardCodedExecutionFactory hardCodedExecutionFactory = new HardCodedExecutionFactory();
		hardCodedExecutionFactory.addData("SELECT x.col1, x.col2 FROM x", Arrays.asList(TestEnginePerformance.sampleData(10000)));
		es.addTranslator("hc", hardCodedExecutionFactory);
		es.deployVDB("test", mmm);
		setConnection(es.getDriver().connect("jdbc:teiid:test", null));
		for (int i = 0; i < 10000; i++) {
			execute("SELECT * from matx where col2 = ?", new Object[] {String.valueOf(i)});
			assertEquals(String.valueOf(i), getRowCount(), 1);
		}
	}
	
	@Test public void testFunctionBasedIndexPerformance() throws Exception {
		ModelMetaData mmm = new ModelMetaData();
		mmm.setName("test");
		mmm.setSchemaSourceType("ddl");
		mmm.setSchemaText("create foreign table x (col1 integer, col2 string); " +
				"create view matx (col1 integer, col2 string, constraint idx index (upper(col2))) options (materialized true) as select * from x;");
		mmm.addSourceMapping("x", "hc", null);
		HardCodedExecutionFactory hardCodedExecutionFactory = new HardCodedExecutionFactory();
		hardCodedExecutionFactory.addData("SELECT x.col1, x.col2 FROM x", Arrays.asList(TestEnginePerformance.sampleData(10000)));
		es.addTranslator("hc", hardCodedExecutionFactory);
		es.deployVDB("test", mmm);
		setConnection(es.getDriver().connect("jdbc:teiid:test", null));
		for (int i = 0; i < 10000; i++) {
			execute("SELECT * from matx where upper(col2) = ?", new Object[] {String.valueOf(i)});
			assertEquals(String.valueOf(i), getRowCount(), 1);
		}
	}
	
	@Test public void testLargeWithoutKeys() throws Exception {
        ModelMetaData mmm = new ModelMetaData();
        mmm.setName("test");
        mmm.addSourceMetadata("ddl", ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("large_mat.ddl")));
        mmm.addSourceMapping("x", "hc", null);
        AutoGenExecutionFactory agef = new AutoGenExecutionFactory();
        agef.addRowCount("tbl_f", 84717);
        agef.addRowCount("tbl_y", 85248);
        agef.addRowCount("tbl_u", 327955);
        es.addTranslator("hc", agef);
        es.deployVDB("test", mmm);
        setConnection(es.getDriver().connect("jdbc:teiid:test", null));
        for (int i = 0; i < 15; i++) {
            execute("SELECT y.iamak, u.penog, y.bibdd, SUM((y.odpdb * f.apcmd)) FROM /*+ PRESERVE */ ((v_y AS y LEFT OUTER JOIN v_f AS f ON f.lbjaa = y.bggnl) INNER JOIN (SELECT DISTINCT hjobj, penog FROM /*+ MAKENOTDEP */ v_u) AS u ON u.hjobj = y.jmafi) WHERE icfbj BETWEEN {ts'2017-01-01 00:00:00.0'} AND {ts'2018-01-01 00:00:00.0'} GROUP BY y.iamak, u.penog, y.bibdd ORDER BY y.iamak, u.penog, y.bibdd");
            assertEquals(28416, getRowCount());
        }
    }
	
}
