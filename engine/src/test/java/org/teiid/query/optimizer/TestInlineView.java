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

package org.teiid.query.optimizer;

import static org.teiid.query.optimizer.TestOptimizer.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.unittest.RealMetadataFactory;


public class TestInlineView  {
    
    public static FakeCapabilitiesFinder getInliveViewCapabilitiesFinder() {
    	FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_DISTINCT, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, true);
        caps.setCapabilitySupport(Capability.QUERY_UNION, true);
        caps.setCapabilitySupport(Capability.QUERY_SET_ORDER_BY, true);
        caps.setCapabilitySupport(Capability.QUERY_SEARCHED_CASE, true);
        caps.setFunctionSupport("concat", true); //$NON-NLS-1$
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        caps.setFunctionSupport("case", true); //$NON-NLS-1$
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        return capFinder;
    }

	@Test public void testANSIJoinInlineView()  throws Exception {
		runTest(createANSIJoinInlineView());
	}	
	
	@Test public void testInlineView()  throws Exception {
		runTest(createInlineView());
	}
	
	@Test public void testInlineViewWithDistinctAndOrderBy() throws Exception {
		runTest(createInlineViewWithDistinctAndOrderBy());
	}	
	
	@Test public void testInlineViewOfVirtual() throws Exception{
		runTest(createInlineViewOfVirtual());
	}
	
	@Test public void testInlineViewWithOuterOrderAndGroup() throws Exception {
		runTest(createInlineViewWithOuterOrderAndGroup());
	}
	
	@Test public void testInlineViewsInUnions() throws Exception {
		runTest(crateInlineViewsInUnions());
	}
	
	@Test public void testUnionInInlineView() throws Exception{
		runTest(createUnionInInlineView());
	}	
	
	public static InlineViewCase createANSIJoinInlineView()  throws Exception {
		String userQuery = "select q1.a from (select count(bqt1.smalla.intkey) as a, bqt1.smalla.intkey from bqt1.smalla group by bqt1.smalla.intkey) q1 left outer join bqt1.smallb on q1.a = bqt1.smallb.intkey where q1.intkey = 1"; //$NON-NLS-1$
		String optimizedQuery = "SELECT v_0.c_0 FROM (SELECT COUNT(g_0.intkey) AS c_0 FROM bqt1.smalla AS g_0 WHERE g_0.intkey = 1 GROUP BY g_0.intkey) AS v_0 LEFT OUTER JOIN bqt1.smallb AS g_1 ON v_0.c_0 = g_1.intkey"; //$NON-NLS-1$
		
		List expectedResults = new ArrayList();
		List row1 = new ArrayList();
        row1.add(new Integer(1)); 
		expectedResults.add(row1);
				
		Set<String> sourceQueries = new HashSet<String>();
        sourceQueries.add("oracle"); //$NON-NLS-1$ 
        sourceQueries.add("db2"); //$NON-NLS-1$ 
        sourceQueries.add("sybase"); //$NON-NLS-1$ 
        sourceQueries.add("sqlserver"); //$NON-NLS-1$ 
        sourceQueries.add("mysql"); //$NON-NLS-1$
        sourceQueries.add("postgres"); //$NON-NLS-1$
        return new InlineViewCase("testANSIJoinInlineView", userQuery, optimizedQuery, //$NON-NLS-1$
				sourceQueries, expectedResults);
        
	}
	
	public static InlineViewCase createInlineView()  throws Exception {
		String userQuery = "select bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa from (select count(bqt1.smalla.intkey) as aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa, bqt1.smalla.intkey from bqt1.smalla group by bqt1.smalla.intkey) bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb, bqt1.smallb " + //$NON-NLS-1$
				"where bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.intkey = 1 and bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa = bqt1.smallb.intkey"; //$NON-NLS-1$
		String optimizedQuery = "SELECT v_0.c_0 FROM (SELECT COUNT(g_0.intkey) AS c_0 FROM bqt1.smalla AS g_0 WHERE g_0.intkey = 1 GROUP BY g_0.intkey) AS v_0, bqt1.smallb AS g_1 WHERE v_0.c_0 = g_1.intkey"; //$NON-NLS-1$
		
		List expectedResults = new ArrayList();
		List row1 = new ArrayList();
        row1.add(new Integer(1)); 
		expectedResults.add(row1);
				
		Set<String> sourceQueries = new HashSet<String>();
        sourceQueries.add("oracle"); //$NON-NLS-1$ 
        sourceQueries.add("db2"); //$NON-NLS-1$ 
        sourceQueries.add("sybase"); //$NON-NLS-1$ 
        sourceQueries.add("sqlserver"); //$NON-NLS-1$ 
        sourceQueries.add("mysql"); //$NON-NLS-1$
        sourceQueries.add("postgres"); //$NON-NLS-1$
        return new InlineViewCase("testInlineView", userQuery, optimizedQuery,  //$NON-NLS-1$
				sourceQueries, expectedResults);        
	}	
	
	public static InlineViewCase createInlineViewWithDistinctAndOrderBy() throws Exception {
		String userQuery = "select Q1.a from (select distinct count(bqt1.smalla.intkey) as a, bqt1.smalla.intkey from bqt1.smalla group by bqt1.smalla.intkey order by bqt1.smalla.intkey) q1 inner join bqt1.smallb as q2 on q1.intkey = q2.intkey where q1.a = 1 and q1.a + q1.intkey = 2"; //$NON-NLS-1$
        String optimizedQuery = "SELECT v_0.c_1 FROM (SELECT g_0.intkey AS c_0, COUNT(g_0.intkey) AS c_1 FROM bqt1.smalla AS g_0 GROUP BY g_0.intkey HAVING ((COUNT(g_0.intkey) + g_0.intkey) = 2) AND (COUNT(g_0.intkey) = 1)) AS v_0, bqt1.smallb AS g_1 WHERE v_0.c_0 = g_1.intkey"; //$NON-NLS-1$

		List expectedResults = new ArrayList();
		List row1 = new ArrayList();
        row1.add(new Integer(1)); 
		expectedResults.add(row1);
				
        Set<String> sourceQueries = new HashSet<String>();
        sourceQueries.add("oracle"); //$NON-NLS-1$ 
        sourceQueries.add("db2"); //$NON-NLS-1$ 
        sourceQueries.add("sybase"); //$NON-NLS-1$ 
        sourceQueries.add("sqlserver"); //$NON-NLS-1$ 
        sourceQueries.add("mysql"); //$NON-NLS-1$
        sourceQueries.add("postgres"); //$NON-NLS-1$
        return new InlineViewCase("testInlineViewWithDistinctAndOrderBy", userQuery, optimizedQuery, //$NON-NLS-1$
				sourceQueries, expectedResults);
        
	}	
	
	public static InlineViewCase createInlineViewOfVirtual() throws Exception{
		String userQuery = "select q1.A from (select count(intkey) as a, intkey, stringkey from vqt.smalla group by intkey, stringkey) q1 inner join vqt.smallb as q2 on q1.intkey = q2.a12345 where q1.a = 2"; //$NON-NLS-1$
		String optimizedQuery = "SELECT v_0.c_1 FROM (SELECT g_0.IntKey AS c_0, COUNT(g_0.IntKey) AS c_1 FROM BQT1.SmallA AS g_0 GROUP BY g_0.IntKey, g_0.StringKey HAVING COUNT(g_0.IntKey) = 2) AS v_0, BQT1.SmallA AS g_1 WHERE v_0.c_0 = Concat(g_1.StringKey, g_1.StringNum)"; //$NON-NLS-1$

		List expectedResults = new ArrayList();
				
        Set<String> sourceQueries = new HashSet<String>();
        sourceQueries.add("oracle"); //$NON-NLS-1$ 
        sourceQueries.add("db2"); //$NON-NLS-1$ 
        sourceQueries.add("sybase"); //$NON-NLS-1$ 
        sourceQueries.add("sqlserver"); //$NON-NLS-1$ 
        sourceQueries.add("mysql"); //$NON-NLS-1$
        sourceQueries.add("postgres"); //$NON-NLS-1$
        return new InlineViewCase("testInlineViewOfVirtual", userQuery, optimizedQuery, //$NON-NLS-1$
				sourceQueries, expectedResults);        
	}	
	
	public static InlineViewCase createInlineViewWithOuterOrderAndGroup() throws Exception {
		String userQuery = "select count(Q1.a) b from (select distinct count(bqt1.smalla.intkey) as a, bqt1.smalla.intkey from bqt1.smalla group by bqt1.smalla.intkey order by bqt1.smalla.intkey) q1 inner join bqt1.smallb as q2 on q1.intkey = q2.intkey where q1.a = 1 and q1.a + q1.intkey = 2 group by Q1.a order by b"; //$NON-NLS-1$
		String optimizedQuery = "SELECT COUNT(v_0.c_1) AS c_0 FROM (SELECT g_0.intkey AS c_0, COUNT(g_0.intkey) AS c_1 FROM bqt1.smalla AS g_0 GROUP BY g_0.intkey HAVING ((COUNT(g_0.intkey) + g_0.intkey) = 2) AND (COUNT(g_0.intkey) = 1)) AS v_0, bqt1.smallb AS g_1 WHERE v_0.c_0 = g_1.intkey GROUP BY v_0.c_1 ORDER BY c_0"; //$NON-NLS-1$

		List expectedResults = new ArrayList();
		List row1 = new ArrayList();
        row1.add(new Integer(1));
		expectedResults.add(row1);
				
        Set<String> sourceQueries = new HashSet<String>();
        sourceQueries.add("oracle"); //$NON-NLS-1$ 
        sourceQueries.add("db2"); //$NON-NLS-1$ 
        sourceQueries.add("sybase"); //$NON-NLS-1$ 
        sourceQueries.add("sqlserver"); //$NON-NLS-1$ 
        sourceQueries.add("mysql"); //$NON-NLS-1$
        sourceQueries.add("postgres"); //$NON-NLS-1$
        return new InlineViewCase("testInlineViewWithOuterOrderAndGroup", userQuery, optimizedQuery, //$NON-NLS-1$
				sourceQueries, expectedResults);        
	}
	
	public static InlineViewCase crateInlineViewsInUnions() throws Exception {
		String userQuery = "select q1.a from (select count(bqt1.smalla.intkey) as a, bqt1.smalla.intkey from bqt1.smalla group by bqt1.smalla.intkey) q1 left outer join bqt1.smallb on q1.a = bqt1.smallb.intkey where q1.intkey = 1 union all (select count(Q1.a) b from (select distinct count(bqt1.smalla.intkey) as a, bqt1.smalla.intkey from bqt1.smalla group by bqt1.smalla.intkey order by bqt1.smalla.intkey) q1 inner join bqt1.smallb as q2 on q1.intkey = q2.intkey where q1.a = 1 and q1.a + q1.intkey = 2 group by Q1.a order by b)"; //$NON-NLS-1$
		String optimizedQuery = "SELECT v_1.c_0 FROM (SELECT COUNT(g_2.intkey) AS c_0 FROM bqt1.smalla AS g_2 WHERE g_2.intkey = 1 GROUP BY g_2.intkey) AS v_1 LEFT OUTER JOIN bqt1.smallb AS g_3 ON v_1.c_0 = g_3.intkey UNION ALL SELECT COUNT(v_0.c_1) AS c_0 FROM (SELECT g_0.IntKey AS c_0, COUNT(g_0.IntKey) AS c_1 FROM bqt1.smalla AS g_0 GROUP BY g_0.IntKey HAVING ((COUNT(g_0.IntKey) + g_0.IntKey) = 2) AND (COUNT(g_0.IntKey) = 1)) AS v_0, bqt1.smallb AS g_1 WHERE v_0.c_0 = g_1.intkey GROUP BY v_0.c_1"; //$NON-NLS-1$

		List expectedResults = new ArrayList();
		List row1 = new ArrayList();
        row1.add(new Integer(1)); 
		expectedResults.add(row1);
		List row2 = new ArrayList();
        row2.add(new Integer(1)); 
        expectedResults.add(row2);
				
        Set<String> sourceQueries = new HashSet<String>();
        sourceQueries.add("oracle"); //$NON-NLS-1$ 
        sourceQueries.add("db2"); //$NON-NLS-1$ 
        sourceQueries.add("sybase"); //$NON-NLS-1$ 
        sourceQueries.add("sqlserver"); //$NON-NLS-1$ 
        sourceQueries.add("mysql"); //$NON-NLS-1$
        sourceQueries.add("postgres"); //$NON-NLS-1$
		return new InlineViewCase("testInlineViewsInUnions", userQuery, optimizedQuery, //$NON-NLS-1$
				sourceQueries, expectedResults);
		
	}
	
	public static InlineViewCase createUnionInInlineView() throws Exception{
		
	    String userQuery = "select t1.intkey from (select case when q1.a=1 then 2 else 1 end as a from (select count(bqt1.smalla.intkey) as a, bqt1.smalla.intkey from bqt1.smalla group by bqt1.smalla.intkey) q1 left outer join bqt1.smallb on q1.a = bqt1.smallb.intkey where q1.intkey = 1 union all (select count(Q1.a) b from (select distinct count(bqt1.smalla.intkey) as a, bqt1.smalla.intkey from bqt1.smalla group by bqt1.smalla.intkey order by bqt1.smalla.intkey) q1 inner join bqt1.smallb as q2 on q1.intkey = q2.intkey where q1.a = 1 and q1.a + q1.intkey = 2 group by Q1.a order by b)) as q3, bqt1.smallb as t1 where q3.a = t1.intkey order by t1.intkey"; //$NON-NLS-1$
		String optimizedQuery = "SELECT g_4.intkey AS c_0 FROM (SELECT CASE WHEN v_1.c_0 = 1 THEN 2 ELSE 1 END AS c_0 FROM (SELECT COUNT(g_2.intkey) AS c_0 FROM bqt1.smalla AS g_2 WHERE g_2.intkey = 1 GROUP BY g_2.intkey) AS v_1 LEFT OUTER JOIN bqt1.smallb AS g_3 ON v_1.c_0 = g_3.intkey UNION ALL SELECT COUNT(v_0.c_1) AS c_0 FROM (SELECT g_0.IntKey AS c_0, COUNT(g_0.IntKey) AS c_1 FROM bqt1.smalla AS g_0 GROUP BY g_0.IntKey HAVING ((COUNT(g_0.IntKey) + g_0.IntKey) = 2) AND (COUNT(g_0.IntKey) = 1)) AS v_0, bqt1.smallb AS g_1 WHERE v_0.c_0 = g_1.intkey GROUP BY v_0.c_1) AS v_2, bqt1.smallb AS g_4 WHERE v_2.c_0 = g_4.intkey ORDER BY c_0"; //$NON-NLS-1$

		List expectedResults = new ArrayList();
		List row1 = new ArrayList();
        row1.add(new Integer(1)); 
		expectedResults.add(row1);
		List row2 = new ArrayList();
        row2.add(new Integer(2)); 
        expectedResults.add(row2);
				
        Set<String> sourceQueries = new HashSet<String>();
        sourceQueries.add("oracle"); //$NON-NLS-1$ 
        /*
         * fails in db2 since the intkey column is in the database as a decimal
         */
        //sourceQueries.add("db2"); //$NON-NLS-1$ 
        sourceQueries.add("sybase"); //$NON-NLS-1$ 
        sourceQueries.add("sqlserver"); //$NON-NLS-1$ 
        sourceQueries.add("mysql"); //$NON-NLS-1$
        sourceQueries.add("postgres"); //$NON-NLS-1$
		return new InlineViewCase("testUnionInInlineView", userQuery, optimizedQuery, //$NON-NLS-1$
				sourceQueries, expectedResults);
		
	}	
	
	protected void runTest(InlineViewCase testCase) throws Exception {
		FakeCapabilitiesFinder capFinder = getInliveViewCapabilitiesFinder();
    	QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
    	
		ProcessorPlan plan = TestOptimizer.helpPlan(testCase.userQuery, metadata, null, capFinder, new String[] {testCase.optimizedQuery}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); 

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);    
	}	
	
    @Test public void testAliasCreationWithInlineView() {
        FakeCapabilitiesFinder capFinder = getInliveViewCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        ProcessorPlan plan = helpPlan("select a, b from (select distinct count(intNum) a, count(stringKey), bqt1.smalla.intkey as b from bqt1.smalla group by bqt1.smalla.intkey) q1 order by q1.a", //$NON-NLS-1$
                metadata, null, capFinder, new String[] {"SELECT COUNT(g_0.intNum) AS c_0, g_0.intkey AS c_1 FROM bqt1.smalla AS g_0 GROUP BY g_0.intkey ORDER BY c_0"}, true); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);    
    }
    
    @Test public void testAliasPreservationWithInlineView() {
        FakeCapabilitiesFinder capFinder = getInliveViewCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        ProcessorPlan plan = helpPlan("select q1.a + 1, q1.b from (select count(bqt1.smalla.intNum) as a, bqt1.smalla.intkey as b from bqt1.smalla group by bqt1.smalla.intNum, bqt1.smalla.intkey order by b) q1 where q1.a = 1", //$NON-NLS-1$
                metadata, null, capFinder, new String[] {"SELECT (q1.a + 1), q1.b FROM (SELECT COUNT(bqt1.smalla.intNum) AS a, bqt1.smalla.intkey AS b FROM bqt1.smalla GROUP BY bqt1.smalla.intNum, bqt1.smalla.intkey HAVING COUNT(bqt1.smalla.intNum) = 1) AS q1"}, true); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);    
    }
    
    /**
     * Order by's will be added to the atomic queries
     */
    @Test public void testCrossSourceInlineView() throws Exception {
        FakeCapabilitiesFinder capFinder = getInliveViewCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        
        ProcessorPlan plan = helpPlan("select * from (select count(bqt1.smalla.intkey) as a, bqt1.smalla.intkey from bqt1.smalla group by bqt1.smalla.intkey) q1 inner join (select count(bqt2.smallb.intkey) as a, bqt2.smallb.intkey from bqt2.smallb group by bqt2.smallb.intkey) as q2 on q1.intkey = q2.intkey where q1.a = 1", //$NON-NLS-1$
                metadata, null, capFinder, new String[] {"SELECT v_0.c_0, v_0.c_1 FROM (SELECT g_0.intkey AS c_0, COUNT(g_0.intkey) AS c_1 FROM bqt2.smallb AS g_0 GROUP BY g_0.intkey) AS v_0 ORDER BY c_0", //$NON-NLS-1$
                                                         "SELECT v_0.c_0, v_0.c_1 FROM (SELECT g_0.intkey AS c_0, COUNT(g_0.intkey) AS c_1 FROM bqt1.smalla AS g_0 GROUP BY g_0.intkey HAVING COUNT(g_0.intkey) = 1) AS v_0 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                0,      // Join
                1,      // MergeJoin
                0,      // Null
                0,      // PlanExecution
                1,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
            });    
    }

}
