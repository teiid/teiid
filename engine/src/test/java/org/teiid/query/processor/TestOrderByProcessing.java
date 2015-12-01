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

import static org.teiid.query.processor.TestProcessor.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.api.exception.query.QueryValidatorException;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.core.TeiidException;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.Options;
import org.teiid.query.validator.TestValidator;
import org.teiid.translator.ExecutionFactory.NullOrder;

@SuppressWarnings({"nls", "unchecked", "rawtypes"})
public class TestOrderByProcessing {

	@Test public void testOrderByDescAll() {
	    String sql = "SELECT distinct e1 from pm1.g2 order by e1 desc limit 1"; //$NON-NLS-1$
	    
	    List[] expected = new List[] { 
	        Arrays.asList("c"),
	    };    
	
	    FakeDataManager dataManager = new FakeDataManager();
	    sampleData1(dataManager);
	    
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
	    
	    helpProcess(plan, dataManager, expected);
	}

	@Test public void testOrderByOutsideOfSelect() {
	    // Create query 
	    String sql = "SELECT e1 FROM (select e1, e2 || e3 as e2 from pm1.g2) x order by e2"; //$NON-NLS-1$
	    
	    //a, a, null, c, b, a
	    // Create expected results
	    List[] expected = new List[] { 
	        Arrays.asList("a"),
	        Arrays.asList("a"),
	        Arrays.asList((String)null),
	        Arrays.asList("c"),
	        Arrays.asList("b"),
	        Arrays.asList("a"),
	    };    
	
	    // Construct data manager with data
	    FakeDataManager dataManager = new FakeDataManager();
	    sampleData1(dataManager);
	    
	    // Plan query
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
	    
	    // Run query
	    helpProcess(plan, dataManager, expected);
	}

	@Test public void testOrderByUnrelatedExpression() {
	    String sql = "SELECT e1, e2 + 1 from pm1.g2 order by e3 || e2 limit 1"; //$NON-NLS-1$
	    
	    List[] expected = new List[] { 
	        Arrays.asList("a", 1),
	    };    
	
	    FakeDataManager dataManager = new FakeDataManager();
	    sampleData1(dataManager);
	    
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
	    
	    helpProcess(plan, dataManager, expected);
	}

	/**
	 * A control test to ensure that y will still exist for sorting
	 */
	@Test public void testOrderByWithDuplicateExpressions() throws Exception {
	    String sql = "select e1 as x, e1 as y from pm1.g1 order by y ASC"; //$NON-NLS-1$
	    
	    QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
	    
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata);
	    
	    List[] expected = new List[] { 
	        Arrays.asList(null, null),
	        Arrays.asList("a", "a"), //$NON-NLS-1$ //$NON-NLS-2$
	        Arrays.asList("a", "a"), //$NON-NLS-1$ //$NON-NLS-2$
	        Arrays.asList("a", "a"), //$NON-NLS-1$ //$NON-NLS-2$
	        Arrays.asList("b", "b"), //$NON-NLS-1$ //$NON-NLS-2$
	        Arrays.asList("c", "c"), //$NON-NLS-1$ //$NON-NLS-2$
	    };
	
	    FakeDataManager manager = new FakeDataManager();
	    sampleData1(manager);
	    helpProcess(plan, manager, expected);
	}
	
	@Test public void testExplicitNullOrdering() throws Exception {
		String sql = "select e1, case when e4 = 2.0 then null else e4 end as x from pm1.g1 order by e1 ASC NULLS LAST, x DESC NULLS FIRST"; //$NON-NLS-1$

		QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
		ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata);

		List[] expected = new List[] { Arrays.asList("a", null),
				Arrays.asList("a", null), //$NON-NLS-1$ 
				Arrays.asList("a", 7.0), //$NON-NLS-1$ 
				Arrays.asList("b", 0.0), //$NON-NLS-1$ 
				Arrays.asList("c", null), //$NON-NLS-1$ 
				Arrays.asList(null, 1.0), 
		};

		FakeDataManager manager = new FakeDataManager();
		sampleData1(manager);
		helpProcess(plan, manager, expected);
	}
	
	@Test public void testNullOrdering() throws Exception { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY_NULL_ORDERING, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select e1 from pm1.g1 order by e1 desc, e2 asc", //$NON-NLS-1$ 
        		RealMetadataFactory.example1Cached(), null, capFinder, 
    			new String[] { "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0 DESC, g_0.e2"},  //$NON-NLS-1$
    			TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);
        
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }
	
	@Test public void testNullOrdering2() throws Exception { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY_NULL_ORDERING, true);
        caps.setSourceProperty(Capability.QUERY_ORDERBY_DEFAULT_NULL_ORDER, NullOrder.FIRST);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().pushdownDefaultNullOrder(true));
        ProcessorPlan plan = TestOptimizer.getPlan(TestOptimizer.helpGetCommand("select e1 from pm1.g1 order by e1 desc, e2 asc NULLS LAST", metadata, null), metadata, capFinder, null, true, cc);
        TestOptimizer.checkAtomicQueries(new String[] { "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0 DESC NULLS LAST, g_0.e2 NULLS LAST"}, plan);  //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }
	
	/**
	 * The engine will remove the null ordering if it's not needed
	 * @throws Exception
	 */
	@Test public void testNullOrdering3() throws Exception { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setSourceProperty(Capability.QUERY_ORDERBY_DEFAULT_NULL_ORDER, NullOrder.HIGH);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().pushdownDefaultNullOrder(true));
        ProcessorPlan plan = TestOptimizer.getPlan(TestOptimizer.helpGetCommand("select e1 from pm1.g1 order by e1 desc, e2 asc NULLS LAST", metadata, null), metadata, capFinder, null, true, cc);
        TestOptimizer.checkAtomicQueries(new String[] { "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0 DESC, g_0.e2"}, plan);  //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }
	
	/**
	 * turns on virtualization
	 * @throws Exception
	 */
	@Test public void testNullOrdering4() throws Exception { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY_NULL_ORDERING, true);
        caps.setSourceProperty(Capability.QUERY_ORDERBY_DEFAULT_NULL_ORDER, NullOrder.UNKNOWN);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().pushdownDefaultNullOrder(true));
        ProcessorPlan plan = TestOptimizer.getPlan(TestOptimizer.helpGetCommand("select e1 from pm1.g1 order by e1 desc, e2 asc", metadata, null), metadata, capFinder, null, true, cc);
        TestOptimizer.checkAtomicQueries(new String[] { "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0 DESC NULLS LAST, g_0.e2 NULLS FIRST"}, plan);  //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }
    
    @Test public void testSortFunctionOverView() {
    	String sql = "select * from (select * from pm1.g1) as x order by cast(e2 as string) limit 1"; //$NON-NLS-1$

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        FakeDataManager fdm = new FakeDataManager();
        sampleData1(fdm);
        helpProcess(plan, fdm, new List[] {Arrays.asList("a", 0, false, 2.0d)});
    }
    
    @Test public void testSortFunctionOverView1() {
    	String sql = "select e1 from (select * from pm1.g1) as x order by cast(e3 as string) desc, cast(e2 as string) limit 1"; //$NON-NLS-1$

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        FakeDataManager fdm = new FakeDataManager();
        sampleData1(fdm);
        helpProcess(plan, fdm, new List[] {Arrays.asList("c")});
    }
    
	@Test public void testOrderByAggWithoutSelectExpression() throws Exception { 
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        TestOptimizer.helpPlan("select sum (e2) as \"sum\" from pm1.g1 group by e1 order by \"sum\"", metadata, new String[] {"SELECT SUM(g_0.e2) FROM pm1.g1 AS g_0 GROUP BY g_0.e1 ORDER BY SUM(g_0.e2)"}, capFinder, ComparisonMode.EXACT_COMMAND_STRING);
    }
	
	@Test public void testNestedLimit() throws Exception {
		String sql = "SELECT count(*) FROM (select intkey, stringkey as x from BQT1.SmallA ORDER BY x limit 10) x where intkey = 1"; //$NON-NLS-1$

		QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
		ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, new DefaultCapabilitiesFinder(TestOptimizer.getTypicalCapabilities()));

		List[] expected = new List[] { Arrays.asList(1), 
		};

		HardcodedDataManager manager = new HardcodedDataManager(metadata);
		manager.addData("SELECT g_0.IntKey AS c_0 FROM SmallA AS g_0 ORDER BY g_0.StringKey", new List[] {Arrays.asList(1)});
		helpProcess(plan, manager, expected);
	}
	
	@Test public void testSubqueryOrderNotPushdown() throws Exception {
		String sql = "SELECT (SELECT SUM(pm1.g1.e2) AS subField FROM pm1.g1 WHERE (pm1.g2.e1 = pm1.g1.e1)) AS sumField FROM pm1.g2 ORDER BY sumField DESC"; //$NON-NLS-1$

		QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
		BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, new DefaultCapabilitiesFinder(bsc));

		List[] expected = new List[] { Arrays.asList(Long.valueOf(2))	};

		HardcodedDataManager manager = new HardcodedDataManager(metadata);
		manager.addData("SELECT g_0.e1 FROM g2 AS g_0", new List[] {Arrays.asList("1")});
		manager.addData("SELECT g_0.e2 FROM g1 AS g_0 WHERE g_0.e1 = '1'", new List[] {Arrays.asList(2)});
		helpProcess(plan, manager, expected);
	}
	
	@Test public void testSubqueryOrderPushdown() throws Exception {
		String sql = "SELECT (SELECT SUM(pm1.g1.e2) AS subField FROM pm1.g1 WHERE (pm1.g2.e1 = pm1.g1.e1)) AS sumField FROM pm1.g2 ORDER BY sumField DESC"; //$NON-NLS-1$

		QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
		BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
		bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
		bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES_SUM, true);
		bsc.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
		bsc.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR_PROJECTION, true);
		ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, new DefaultCapabilitiesFinder(bsc));

		List[] expected = new List[] { Arrays.asList(1), 
		};

		HardcodedDataManager manager = new HardcodedDataManager(metadata);
		manager.addData("SELECT (SELECT SUM(g_1.e2) FROM g1 AS g_1 WHERE g_1.e1 = g_0.e1) AS c_0 FROM g2 AS g_0 ORDER BY c_0 DESC", new List[] {Arrays.asList(1)});
		helpProcess(plan, manager, expected);
	}
	
	//currently we prevent
	@Test(expected=QueryValidatorException.class) public void testSubqueryOrderByUnrelated() throws Exception {
		String sql = "SELECT * FROM pm1.g1 ORDER BY (select count(*) from pm1.g2 where e1 = pm1.g1.e1)"; //$NON-NLS-1$

		QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
		BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		helpGetPlan(helpParse(sql), metadata, new DefaultCapabilitiesFinder(bsc), createCommandContext());
	}
	
	@Test public void testSubqueryOrderByRelated() throws Exception {
		String sql = "SELECT pm1.g1.*, (select count(*) from pm1.g2 where e1 = pm1.g1.e1) FROM pm1.g1 ORDER BY (select count(*) from pm1.g2 where e1 = pm1.g1.e1)"; //$NON-NLS-1$

		QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
		BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
		ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata, new DefaultCapabilitiesFinder(bsc), createCommandContext());
		
		List[] expected = new List[] { Arrays.asList("a", 1, true, 1.0, 1) };
		
		HardcodedDataManager manager = new HardcodedDataManager(metadata);
		manager.addData("SELECT g_0.e1, g_0.e2, g_0.e3, g_0.e4 FROM g1 AS g_0", new List[] {Arrays.asList("a", 1, true, 1.0)});
		manager.addData("SELECT 1 FROM g2 AS g_0 WHERE g_0.e1 = 'a'", new List[] {Arrays.asList(1)});
		helpProcess(plan, manager, expected);
	}
	
	 @Test public void testSortCollationInhibitsPush() throws TeiidException { 
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setSourceProperty(Capability.COLLATION_LOCALE, "foo");

        // Create query 
        String sql = "select e1, e2 from pm1.g1 order by e2"; //$NON-NLS-1$

        CommandContext cc = new CommandContext();
        cc.setOptions(new Options().requireTeiidCollation(true));
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(caps), cc); //$NON-NLS-1$

		List[] expected = new List[] { Arrays.asList("a", 0), Arrays.asList("a", 1) };

		HardcodedDataManager manager = new HardcodedDataManager();
		manager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_1", new List[] {Arrays.asList("a", 0), Arrays.asList("a", 1)});
		helpProcess(plan, manager, expected);
		
		sql = "select e1, e2 from pm1.g1 order by e1"; //$NON-NLS-1$

        plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(caps), cc); //$NON-NLS-1$

		expected = new List[] { Arrays.asList("a", 0), Arrays.asList("b", 1) };

		manager = new HardcodedDataManager();
		manager.addData("SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0", new List[] {Arrays.asList("b", 1), Arrays.asList("a", 0)});
		helpProcess(plan, manager, expected);
    }
	 
	@Test public void testDefaultNullOrdering() throws Exception {
		String sql = "select e1 from pm1.g1 order by e1"; //$NON-NLS-1$

		QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
		CommandContext cc = createCommandContext();
		BufferManagerImpl bm = BufferManagerFactory.createBufferManager();
		bm.setOptions(new Options().defaultNullOrder(NullOrder.FIRST));
		cc.setBufferManager(bm);
		ProcessorPlan plan = helpGetPlan(helpParse(sql), metadata,
				new DefaultCapabilitiesFinder(), cc);

		HardcodedDataManager manager = new HardcodedDataManager(metadata);
		manager.addData("SELECT g1.e1 FROM g1",
				new List[] { Arrays.asList("a"), Arrays.asList("b"), Arrays.asList((String)null) });
		helpProcess(plan, cc, manager, new List[] { Arrays.asList((String)null), Arrays.asList("a"), Arrays.asList("b") });

		plan = helpGetPlan(helpParse("select e1 from pm1.g1 order by e1 desc"), metadata,
				new DefaultCapabilitiesFinder(), cc);

		helpProcess(plan, cc, manager, new List[] { Arrays.asList((String)null), Arrays.asList("b"), Arrays.asList("a") });
		
		bm.getOptions().setDefaultNullOrder(NullOrder.LAST);
		
		plan = helpGetPlan(helpParse("select e1 from pm1.g1 order by e1 desc"), metadata,
				new DefaultCapabilitiesFinder(), cc);

		helpProcess(plan, cc, manager, new List[] { Arrays.asList("b"), Arrays.asList("a"), Arrays.asList((String)null) });
		
		bm.getOptions().setDefaultNullOrder(NullOrder.HIGH);
		
		plan = helpGetPlan(helpParse("select e1 from pm1.g1 order by e1"), metadata,
				new DefaultCapabilitiesFinder(), cc);

		helpProcess(plan, cc, manager, new List[] { Arrays.asList("a"), Arrays.asList("b"), Arrays.asList((String)null) });
		
	}
	
	@Test public void testGroupByUnrelated() {
	    String sql = "SELECT sum(e2) from pm1.g2 group by e1 order by e1"; //$NON-NLS-1$
	    
	    List[] expected = new List[] { 
	    	Arrays.asList(Long.valueOf(1)),
		    Arrays.asList(Long.valueOf(3)),
		    Arrays.asList(Long.valueOf(2)),
		    Arrays.asList(Long.valueOf(1)),
	    };    
	
	    FakeDataManager dataManager = new FakeDataManager();
	    sampleData1(dataManager);
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder());
	    helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testGroupByUnrelated1() {
	    String sql = "SELECT sum(e2), e1 from pm1.g2 group by e1 order by case when e1 is null then 1 else 0 end"; //$NON-NLS-1$
	    
	    List[] expected = new List[] { 
	        Arrays.asList(Long.valueOf(3), "a"),
	        Arrays.asList(Long.valueOf(2), "b"),
	        Arrays.asList(Long.valueOf(1), "c"),
	        Arrays.asList(Long.valueOf(1), null),
	    };    
	
	    FakeDataManager dataManager = new FakeDataManager();
	    sampleData1(dataManager);
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder());
	    helpProcess(plan, dataManager, expected);
	}
	
	@Test(expected=QueryValidatorException.class) public void testGroupByUnrelated2() throws TeiidException {
	    String sql = "SELECT sum(e2) from pm1.g2 group by e1 || 'a' order by case when e1 is null then 0 else 1 end"; //$NON-NLS-1$
	    
	    //should not validate
	    CommandContext context = createCommandContext();
		helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), DefaultCapabilitiesFinder.INSTANCE, context);
	}
	
	@Test public void testGroupByUnrelated3() {
	    String sql = "SELECT sum(e2) from pm1.g2 group by e1 || 'a' order by case when e1 || 'a' is null then 0 else 1 end"; //$NON-NLS-1$
	    
	    List[] expected = new List[] { 
	        Arrays.asList(Long.valueOf(1)),
	        Arrays.asList(Long.valueOf(3)),
	        Arrays.asList(Long.valueOf(2)),
	        Arrays.asList(Long.valueOf(1)),
	    };    
	
	    FakeDataManager dataManager = new FakeDataManager();
	    sampleData1(dataManager);
	    ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder());
	    helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testUnaliasedOrderByFails() {
		TestValidator.helpValidate("SELECT pm1.g1.e1 e2 FROM pm1.g1 group by pm1.g1.e1 ORDER BY pm1.g1.e2", new String[] {"pm1.g1.e2"}, RealMetadataFactory.example1Cached());
    }
    
    @Test public void testUnaliasedOrderByFails1() {
    	TestValidator.helpValidate("SELECT pm1.g1.e1 e2 FROM pm1.g1 group by pm1.g1.e1 ORDER BY pm1.g1.e2 + 1", new String[] {"pm1.g1.e2"}, RealMetadataFactory.example1Cached());
    }
    
    @Test public void testOrderByUnrelated2() {
    	TestValidator.helpValidate("SELECT max(e2) FROM pm1.g1 group by e1 ORDER BY e4", new String[] {"e4"}, RealMetadataFactory.example1Cached());
    }
	
}
