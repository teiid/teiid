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
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.query.util.Options; 
import org.teiid.translator.ExecutionFactory.NullOrder;

@SuppressWarnings({"nls", "unchecked"})
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
        		new String[] { "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0 DESC NULLS LAST, g_0.e2 NULLS FIRST"},  //$NON-NLS-1$
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
	         //cc.setOptions(new Options().pushdownDefaultNullOrder(true));
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

}
