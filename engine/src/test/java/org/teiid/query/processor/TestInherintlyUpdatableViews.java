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

package org.teiid.query.processor;

import static org.junit.Assert.*;
import static org.teiid.query.processor.TestProcessor.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.cache.DefaultCacheFactory;
import org.teiid.dqp.internal.process.PreparedPlan;
import org.teiid.dqp.internal.process.SessionAwareCache;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.rewriter.TestQueryRewriter;
import org.teiid.query.sql.lang.BatchedUpdateCommand;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.query.validator.TestUpdateValidator;
import org.teiid.translator.SourceSystemFunctions;

@SuppressWarnings("nls")
public class TestInherintlyUpdatableViews {

	@Test public void testUpdatePassThrough() throws Exception {
		String userSql = "update vm1.gx set e1 = e2"; //$NON-NLS-1$
    	String viewSql = "select * from pm1.g1 where e3 < 5";
    	String expectedSql = "UPDATE pm1.g1 SET e1 = convert(pm1.g1.e2, string) WHERE convert(e3, integer) < 5";
        helpTest(userSql, viewSql, expectedSql, null);	
	}

	private Command helpTest(String userSql, String viewSql, String expectedSql, ProcessorDataManager dm)
			throws Exception {
		TransformationMetadata metadata = TestUpdateValidator.example1();
        TestUpdateValidator.createView(viewSql, metadata, "gx");
        Command command = TestQueryRewriter.helpTestRewriteCommand(userSql, expectedSql, metadata);

        if (dm != null) {
        	CommandContext context = createCommandContext();
        	SessionAwareCache<PreparedPlan> planCache = new SessionAwareCache<PreparedPlan>("preparedplan", DefaultCacheFactory.INSTANCE, SessionAwareCache.Type.PREPAREDPLAN, 0);
        	context.setPreparedPlanCache(planCache); //$NON-NLS-1$
	        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
	        caps.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
	        ProcessorPlan plan = helpGetPlan(helpParse(userSql), metadata, new DefaultCapabilitiesFinder(caps), context);
	        List<?>[] expected = new List[] {Arrays.asList(1)};
        	helpProcess(plan, context, dm, expected);
        	assertEquals(0, planCache.getTotalCacheEntries());
        }
        
        return command;
	}
	
	@Test public void testUpdatePassThroughWithAlias() throws Exception {
		String userSql = "update vm1.gx set e1 = e2"; //$NON-NLS-1$
    	String viewSql = "select * from pm1.g1 as x where e3 < 5";
    	String expectedSql = "UPDATE pm1.g1 SET e1 = convert(pm1.g1.e2, string) WHERE convert(e3, integer) < 5";
        helpTest(userSql, viewSql, expectedSql, null);	
	}
	
	@Test public void testDeletePassThrough() throws Exception {
		String userSql = "delete from vm1.gx where e1 = e2"; //$NON-NLS-1$
    	String viewSql = "select * from pm1.g1 where e3 < 5";
        String expectedSql = "DELETE FROM pm1.g1 WHERE (pm1.g1.e1 = convert(pm1.g1.e2, string)) AND (convert(e3, integer) < 5)";
        helpTest(userSql, viewSql, expectedSql, null);
	}
	
	@Test public void testInsertPassThrough() throws Exception {
		String userSql = "insert into vm1.gx (e1) values (1)"; //$NON-NLS-1$
    	String viewSql = "select * from pm1.g1 where e3 < 5";
        String expectedSql = "INSERT INTO pm1.g1 (e1) VALUES ('1')";
        helpTest(userSql, viewSql, expectedSql, null);
	}
	
	@Test public void testDeleteUnion() throws Exception {
		String userSql = "delete from vm1.gx where e4 is null"; //$NON-NLS-1$
    	String viewSql = "select * from pm1.g1 where e3 < 5 union all select * from pm1.g2 where e1 > 1";
        String expectedSql = "BatchedUpdate{D,D}";
        BatchedUpdateCommand buc = (BatchedUpdateCommand)helpTest(userSql, viewSql, expectedSql, null);
        assertEquals("DELETE FROM pm1.g2 WHERE (pm1.g2.e4 IS NULL) AND (e1 > '1')", buc.getUpdateCommands().get(1).toString());
	}
	
	/**
	 * Here we should be able to figure out that we can pass through the join
	 * @throws Exception
	 */
	@Test public void testInsertPassThrough1() throws Exception {
		String userSql = "insert into vm1.gx (e1) values (1)"; //$NON-NLS-1$
    	String viewSql = "select g2.* from pm1.g1 inner join pm1.g2 on g1.e1 = g2.e1";
        String expectedSql = "INSERT INTO pm1.g2 (e1) VALUES ('1')";
        helpTest(userSql, viewSql, expectedSql, null);	
	}
	
	@Test public void testUpdateComplex() throws Exception {
		String userSql = "update vm1.gx set e1 = e2 where e3 is null"; //$NON-NLS-1$
		String viewSql = "select g2.* from pm1.g1 inner join pm1.g2 on g1.e1 = g2.e1";
		
		HardcodedDataManager dm = new HardcodedDataManager();
        dm.addData("SELECT convert(g_1.e2, string), g_1.e2 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_1.e3 IS NULL)", new List[] {Arrays.asList("1", 1)});
        dm.addData("UPDATE pm1.g2 SET e1 = '1' WHERE pm1.g2.e2 = 1", new List[] {Arrays.asList(1)});
        
		helpTest(userSql, viewSql, "CREATE VIRTUAL PROCEDURE\nBEGIN ATOMIC\nDECLARE integer VARIABLES.ROWS_UPDATED = 0;\nLOOP ON (SELECT convert(pm1.g2.e2, string) AS s_0, pm1.g2.e2 AS s_1 FROM pm1.g1 INNER JOIN pm1.g2 ON g1.e1 = g2.e1 WHERE pm1.g2.e3 IS NULL) AS X\nBEGIN\nUPDATE pm1.g2 SET e1 = X.s_0 WHERE pm1.g2.e2 = X.s_1;\nVARIABLES.ROWS_UPDATED = (VARIABLES.ROWS_UPDATED + 1);\nEND\nSELECT VARIABLES.ROWS_UPDATED;\nEND",
				dm);
	}
	
	@Test public void testDeleteComplex() throws Exception {
		String userSql = "delete from vm1.gx where e2 < 10"; //$NON-NLS-1$
		String viewSql = "select g2.* from pm1.g1 inner join pm1.g2 on g1.e1 = g2.e1";
		
		HardcodedDataManager dm = new HardcodedDataManager();
        dm.addData("SELECT g_1.e2 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_1.e2 < 10)", new List[] {Arrays.asList(2)});
        dm.addData("DELETE FROM pm1.g2 WHERE pm1.g2.e2 = 2", new List[] {Arrays.asList(1)});
        
		helpTest(userSql, viewSql, "CREATE VIRTUAL PROCEDURE\nBEGIN ATOMIC\nDECLARE integer VARIABLES.ROWS_UPDATED = 0;\nLOOP ON (SELECT pm1.g2.e2 AS s_0 FROM pm1.g1 INNER JOIN pm1.g2 ON g1.e1 = g2.e1 WHERE pm1.g2.e2 < 10) AS X\nBEGIN\nDELETE FROM pm1.g2 WHERE pm1.g2.e2 = X.s_0;\nVARIABLES.ROWS_UPDATED = (VARIABLES.ROWS_UPDATED + 1);\nEND\nSELECT VARIABLES.ROWS_UPDATED;\nEND",
				dm);
	}
	
	/**
	 * Here we should use the partitioning
	 * @throws Exception
	 */
	@Test public void testInsertPartitionedUnion() throws Exception {
		String userSql = "insert into vm1.gx (e1, e2) values (1, 2)"; //$NON-NLS-1$
    	String viewSql = "select 1 as e1, e2 from pm1.g1 union all select 2 as e1, e2 from pm1.g2";
        String expectedSql = "INSERT INTO pm1.g1 (e2) VALUES (2)";
        helpTest(userSql, viewSql, expectedSql, null);	
	}
	
	@Test public void testWherePartitioningUpdates() throws Exception {
		TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table b (custid integer, field1 varchar) options (updatable true); " +
				"create view finnish_customers options (updatable true) as select custid, field1 as name from b where custid = 1; " +
				"create view other_customers options (updatable true) as select custid, field1 as name from b where custid = 2; " +
				"create view customers options (updatable true) as select * from finnish_customers where custid = 1 union all select * from other_customers where custid = 2;", "x", "y");
		
		ProcessorPlan plan = TestProcessor.helpGetPlan("insert into customers (custid, name) values (1, 'a')", metadata);
		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("INSERT INTO b (custid, field1) VALUES (1, 'a')", new List<?>[] {Arrays.asList(1)});
		TestProcessor.helpProcess(plan, dataManager, new List<?>[] {Arrays.asList(1)});

		//ensure that update works as expected - TODO: eventually we should support a check option to not allow updates such as this
		plan = TestProcessor.helpGetPlan("update customers set custid = 3, name = 'a'", metadata, TestOptimizer.getGenericFinder());
		dataManager = new HardcodedDataManager();
		dataManager.addData("UPDATE b SET custid = 3, field1 = 'a' WHERE custid = 1", new List<?>[] {Arrays.asList(1)});
		dataManager.addData("UPDATE b SET custid = 3, field1 = 'a' WHERE custid = 2", new List<?>[] {Arrays.asList(1)});
		TestProcessor.helpProcess(plan, dataManager, new List<?>[] {Arrays.asList(2)});
	}

}
