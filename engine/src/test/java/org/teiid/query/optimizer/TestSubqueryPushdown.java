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

import org.junit.After;
import org.junit.Test;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.TestOptimizer.AntiSemiJoin;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.TestOptimizer.SemiJoin;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.optimizer.relational.rules.RuleMergeCriteria;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.rewriter.TestQueryRewriter;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.SourceSystemFunctions;

@SuppressWarnings("nls")
public class TestSubqueryPushdown {

	@Test public void testPushSubqueryBelowVirtual() throws Exception {
		String sql = "select g3.e1 from (select e1, max(e2) y from pm1.g1 group by e1) x, pm1.g3 where exists (select e1 from pm1.g2 where x.e1 = e1)"; //$NON-NLS-1$

	    // Create capabilities
	    FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
	    BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
	    caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
	    caps.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);
	    capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
	    
	    QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
	
	    // Plan query
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata, 
	        null, capFinder,
	        new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE EXISTS (SELECT g_1.e1 FROM pm1.g2 AS g_1 WHERE g_1.e1 = g_0.e1)", //$NON-NLS-1$
	    		"SELECT g_0.e1 FROM pm1.g3 AS g_0" },  //$NON-NLS-1$
	        TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                1,      // Grouping
                1,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                2,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
            });
	}
	
	/**
	 * Same as above, but using a correlated variable based on an aggregate
	 * @throws Exception
	 */
	@Test public void testDontPushSubqueryBelowVirtual() throws Exception {
		String sql = "select g3.e1 from (select e1, max(e2) y from pm1.g1 group by e1) x, pm1.g3 where exists (select e1 from pm1.g2 where x.y = e1)"; //$NON-NLS-1$

	    // Create capabilities
	    FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
	    BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
	    caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
	    caps.setCapabilitySupport(Capability.CRITERIA_EXISTS, true);
	    capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
	    
	    QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
	
	    // Plan query
	    ProcessorPlan plan = TestOptimizer.helpPlan(sql, metadata, 
	        null, capFinder,
	        new String[] { "SELECT g_0.e1 FROM pm1.g3 AS g_0", //$NON-NLS-1$
	    		"SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0" },  //$NON-NLS-1$
	        TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING);
        TestOptimizer.checkNodeTypes(plan, new int[] {
                2,      // Access
                0,      // DependentAccess
                1,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                1,      // Grouping
                1,      // NestedLoopJoinStrategy
                0,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                2,      // Project
                0,      // Select
                0,      // Sort
                0       // UnionAll
            });
	}
	
	@Test public void testPushCorrelatedSubquery1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_ALL, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT intkey FROM bqt1.smalla AS n WHERE intkey = (SELECT MAX(intkey) FROM bqt1.smallb AS s WHERE s.stringkey = n.stringkey )", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT intkey FROM bqt1.smalla AS n WHERE intkey = (SELECT MAX(intkey) FROM bqt1.smallb AS s WHERE s.stringkey = n.stringkey)" }, SHOULD_SUCCEED); //$NON-NLS-1$ 
        checkNodeTypes(plan, FULL_PUSHDOWN); 
    }   

    @Test public void testPushCorrelatedSubquery2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_ALL, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setFunctionSupport(SourceSystemFunctions.CONCAT, true);
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        String sqlIn = 
            "SELECT c37n.intkey " + //$NON-NLS-1$
            "FROM bqt1.mediuma AS c37n, bqt1.smallb AS m37n " + //$NON-NLS-1$
            "WHERE (m37n.stringkey LIKE '%0') AND " + //$NON-NLS-1$
            "(c37n.stringkey = ('1' || (m37n.intkey || '0'))) AND " + //$NON-NLS-1$
            "(c37n.datevalue = (" + //$NON-NLS-1$
            "SELECT MAX(c37s.datevalue) " + //$NON-NLS-1$
            "FROM bqt1.mediuma AS c37s, bqt1.smallb AS m37s " + //$NON-NLS-1$
            "WHERE (m37s.stringkey LIKE '%0') AND " + //$NON-NLS-1$
            "(c37s.stringkey = ('1' || (m37s.intkey || '0'))) AND " + //$NON-NLS-1$
            "(m37s.stringkey = m37n.stringkey) ))"; //$NON-NLS-1$

        String sqlOut = "SELECT g_0.intkey FROM bqt1.mediuma AS g_0, bqt1.smallb AS g_1 WHERE (g_0.stringkey = concat('1', concat(g_1.intkey, '0'))) AND (g_0.datevalue = (SELECT MAX(g_2.datevalue) FROM bqt1.mediuma AS g_2, bqt1.smallb AS g_3 WHERE (g_2.stringkey = concat('1', concat(g_3.intkey, '0'))) AND (g_3.stringkey LIKE '%0') AND (g_3.stringkey = g_1.stringkey))) AND (g_1.stringkey LIKE '%0')"; //$NON-NLS-1$

        ProcessorPlan plan = helpPlan(sqlIn, RealMetadataFactory.exampleBQTCached(),  
            null, capFinder,
            new String[] { sqlOut }, SHOULD_SUCCEED); 
        checkNodeTypes(plan, FULL_PUSHDOWN); 
    }   

    @Test public void testPushCorrelatedSubquery3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_LIKE, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_ALL, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_SELFJOIN, true);
        caps.setFunctionSupport("||", true); //$NON-NLS-1$
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        String sqlIn = 
            "SELECT intkey " + //$NON-NLS-1$
            "FROM vqt.smalla AS e " + //$NON-NLS-1$
            "WHERE (stringkey = 'VOD.L') AND " + //$NON-NLS-1$
            "(datevalue = (" + //$NON-NLS-1$
            "SELECT MAX(datevalue) " + //$NON-NLS-1$
            "FROM vqt.smalla " + //$NON-NLS-1$
            "WHERE (stringkey = e.stringkey) ))"; //$NON-NLS-1$

        String sqlOut = 
            "SELECT SmallA__1.IntKey FROM BQT1.SmallA AS SmallA__1 WHERE (SmallA__1.StringKey = 'VOD.L') AND (SmallA__1.DateValue = (SELECT MAX(BQT1.SmallA.DateValue) FROM BQT1.SmallA WHERE BQT1.SmallA.StringKey = SmallA__1.StringKey))"; //$NON-NLS-1$
        
        ProcessorPlan plan = helpPlan(sqlIn, RealMetadataFactory.exampleBQTCached(),  
            null, capFinder,
            new String[] { sqlOut }, SHOULD_SUCCEED); 
        checkNodeTypes(plan, FULL_PUSHDOWN); 
    }  

    /**
     * Check that scalar subquery in select is pushed 
     */
    public void testPushSubqueryInSelectClause1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT stringkey, (SELECT intkey FROM BQT1.SmallA AS b WHERE Intnum = 22) FROM BQT1.SmallA", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT stringkey, (SELECT intkey FROM BQT1.SmallA AS b WHERE Intnum = 22) FROM BQT1.SmallA" }, SHOULD_SUCCEED); //$NON-NLS-1$ 
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }   
    
    @Test public void testCorrelatedSubquery1() {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in (select e1 FROM pm2.g1 WHERE pm1.g1.e2 = pm2.g1.e2)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            new String[] { "SELECT e1, pm1.g1.e2 FROM pm1.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }

    @Test public void testCorrelatedSubquery2() {
        ProcessorPlan plan = helpPlan("Select e1, (select e1 FROM pm2.g1 WHERE pm1.g1.e2 = pm2.g1.e2) from pm1.g1", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            new String[] { "SELECT e1, pm1.g1.e2 FROM pm1.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            1,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }

    @Test public void testCorrelatedSubqueryVirtualLayer1() {
        ProcessorPlan plan = helpPlan("Select e1 from vm1.g6 where e1 in (select e1 FROM pm2.g1 WHERE vm1.g6.e3 = pm2.g1.e2)", example1(),  //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }

    @Test public void testCorrelatedSubqueryVirtualLayer2() {
        ProcessorPlan plan = helpPlan("Select e1 from vm1.g6 where e1 in (select e1 FROM pm2.g1 WHERE vm1.g6.e4 = pm2.g1.e4)", example1(),  //$NON-NLS-1$
            new String[] { "SELECT e1, e2, e4 FROM pm1.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }

    @Test public void testCorrelatedSubqueryVirtualLayer3() {
        ProcessorPlan plan = helpPlan("Select e1, (select e1 FROM pm2.g1 WHERE vm1.g6.e4 = pm2.g1.e4) from vm1.g6", example1(),  //$NON-NLS-1$
            new String[] { "SELECT e1, e2, e4 FROM pm1.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            1,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }

    @Test public void testCorrelatedSubqueryInTransformation2() {
        String sql = "Select * from vm1.g20"; //$NON-NLS-1$
        ProcessorPlan plan = helpPlan(sql, RealMetadataFactory.example1Cached(), 
            new String[] { "SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3, pm1.g1.e4 FROM pm1.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });         
    }
    
    /**
     * Check that subquery is not pushed if the subquery cannot all be pushed to the source.
     */
    @Test public void testNoPushSubqueryInWhereClause1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in (select max(e1) FROM pm1.g2)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$ 
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

    /**
     * Check that subquery is not pushed if the subquery is from a different model
     * than the outer query.
     */
    @Test public void testNoPushSubqueryInWhereClause2() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", getTypicalCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in (select e1 FROM pm2.g1)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$ 
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }
    
    /**
     * Do not support XML query as subquery
     * Check that subquery is not pushed if the subquery is not relational.
     */
    public void defer_testNoPushSubqueryInWhereClause3() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", new BasicSourceCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in (select * from xmltest.doc1)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$ 
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }

    /**
     * Check that subquery is not pushed if the subquery has a function that can't be pushed 
     * in the SELECT clause
     */
    @Test public void testNoPushSubqueryInWhereClause4() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", new BasicSourceCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in (SELECT ltrim(e1) FROM pm1.g2)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$ 
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }
    
    /**
     * Check that subquery is not pushed if the subquery selects a constant value
     */
    @Test public void testNoPushSubqueryInWhereClause5() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", new BasicSourceCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in (SELECT 'xyz' FROM pm1.g2)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$ 
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }
    
    /**
     * Check that subquery is not pushed if the subquery does ORDER BY
     */
    @Test public void testNoPushSubqueryInWhereClause6() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", new BasicSourceCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in (SELECT e1 FROM pm1.g2 ORDER BY e1 limit 2)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$ 
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }
    
    /**
     * Check that subquery is not pushed if the subquery has a function that can't be pushed 
     * in the SELECT clause
     */
    @Test public void testNoPushSubqueryInWhereClause7() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setFunctionSupport("ltrim", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", new BasicSourceCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in (SELECT rtrim(ltrim(e1)) FROM pm1.g2)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$ 
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }

    /**
     * Check that subquery is not pushed if the subquery holds non-query access node.
     */
    @Test public void testNoPushSubqueryInWhereClause8() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", new BasicSourceCapabilities()); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in (EXEC pm1.sqsp1())", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$ 
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }

    /**
     * Check that subquery is not pushed if the subquery is correlated and correlated not supported
     */
    @Test public void testNoPushSubqueryInWhereClause9() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, false);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in (SELECT pm1.g2.e1 FROM pm1.g2 WHERE pm1.g2.e1 = pm1.g1.e1)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT e1 FROM pm1.g1" }, SHOULD_SUCCEED); //$NON-NLS-1$ 
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }       
    
	@Test public void testPushMultipleCorrelatedSubquery1() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_OR, true);
        caps.setCapabilitySupport(Capability.CRITERIA_COMPARE_EQ, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_ALL, true);
        caps.setCapabilitySupport(Capability.CRITERIA_QUANTIFIED_SOME, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.CRITERIA_IN_SUBQUERY, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SELECT_EXPRESSION, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MIN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        ProcessorPlan plan = helpPlan("SELECT intkey FROM bqt1.smalla AS n WHERE intkey = (SELECT MAX(intkey) FROM bqt1.smallb AS s WHERE s.stringkey = n.stringkey ) or intkey = (SELECT MIN(intkey) FROM bqt1.smallb AS s WHERE s.stringkey = n.stringkey )", RealMetadataFactory.exampleBQTCached(),  //$NON-NLS-1$
            null, capFinder,
            new String[] { "SELECT g_0.intkey FROM bqt1.smalla AS g_0 WHERE (g_0.intkey = (SELECT MAX(g_1.intkey) FROM bqt1.smallb AS g_1 WHERE g_1.stringkey = g_0.stringkey)) OR (g_0.intkey = (SELECT MIN(g_2.IntKey) FROM bqt1.smallb AS g_2 WHERE g_2.StringKey = g_0.stringkey))" }, SHOULD_SUCCEED); //$NON-NLS-1$ 
        checkNodeTypes(plan, FULL_PUSHDOWN); 
    }
	
    /*
     * Expressions containing subqueries can be pushed down
     */
    @Test public void testProjectSubqueryPushdown() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
        
        BasicSourceCapabilities caps = getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        caps.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);
        caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        caps.setFunctionSupport("+", true); //$NON-NLS-1$
        caps.setFunctionSupport("convert", true); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        
        ProcessorPlan plan = helpPlan("select pm1.g1.e1, convert((select max(vm1.g1.e1) from vm1.g1), integer) + 1 from pm1.g1", metadata,  //$NON-NLS-1$
                                      null, capFinder,
            new String[] { "SELECT g_0.e1, (convert((SELECT MAX(g_1.e1) FROM pm1.g1 AS g_1), integer) + 1) FROM pm1.g1 AS g_0" }, SHOULD_SUCCEED); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN); 
    }
    
    @Test public void testScalarSubquery2() {
        ProcessorPlan plan = helpPlan("Select e1, (select e1 FROM pm2.g1 where pm1.g1.e1 = 'x') as X from pm1.g1", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            1,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    } 
    
    /**
     * Technically this is not a full push-down, but the subquery will be evaluated prior to pushdown
     */
    @Test public void testCompareSubquery4() throws TeiidComponentException, TeiidProcessingException {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 > (select e1 FROM pm2.g1 where e2 = 13)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 > (SELECT g_0.e1 FROM pm2.g1 AS g_0 WHERE g_0.e2 = 13)" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN); 
    }
    
    @Test public void testScalarSubquery1() throws TeiidComponentException, TeiidProcessingException {
        ProcessorPlan plan = helpPlan("Select e1, (select e1 FROM pm2.g1 where e1 = 'x') from pm1.g1", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e1, (SELECT g_0.e1 FROM pm2.g1 AS g_0 WHERE g_0.e1 = 'x') FROM pm1.g1 AS g_0" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN); 
    }
    
    @Test public void testSubqueryRewriteToJoinDistinct() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select distinct e1 from pm1.g1 as x where exists (select pm1.g1.e1 FROM pm1.g1 where e1 = x.e1)", "SELECT DISTINCT e1 FROM pm1.g1 AS x, (SELECT pm1.g1.e1 FROM pm1.g1) AS X__1 WHERE x.e1 = X__1.e1", RealMetadataFactory.example1Cached());
    }
    
    /**
     * Agg does not depend on cardinality
     */
    @Test public void testSubqueryRewriteToJoinGroupBy() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select max(e1) from pm1.g1 as x where exists (select pm1.g1.e1 FROM pm1.g1 where e1 = x.e1) group by e2", "SELECT MAX(e1) FROM pm1.g1 AS x, (SELECT pm1.g1.e1 FROM pm1.g1) AS X__1 WHERE x.e1 = X__1.e1 GROUP BY e2", RealMetadataFactory.example1Cached());
    }
    
    /**
     * Agg does depend on cardinality
     */
    @Test public void testSubqueryRewriteToJoinGroupBy1() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select avg(e1) from pm1.g1 as x where exists (select pm1.g1.e1 FROM pm1.g1 where e1 = x.e1) group by e2", "SELECT AVG(e1) FROM pm1.g1 AS x WHERE EXISTS (SELECT pm1.g1.e1 FROM pm1.g1 WHERE e1 = x.e1 LIMIT 1) GROUP BY e2", RealMetadataFactory.example1Cached());
    }
    
    @Test public void testSubqueryRewriteToJoin() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where exists (select pm1.g1.e1 FROM pm1.g1 where e1 = pm3.g1.e1)", "SELECT e1 FROM pm3.g1, (SELECT pm1.g1.e1 FROM pm1.g1) AS X__1 WHERE pm3.g1.e1 = X__1.e1", RealMetadataFactory.example4());
    }
    
    @Test public void testSubqueryRewriteToJoin1() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e1 in /*+ mj */ (select pm1.g1.e1 as x FROM pm1.g1)", "SELECT e1 FROM pm3.g1, (SELECT pm1.g1.e1 AS x FROM pm1.g1) AS X__1 WHERE pm3.g1.e1 = X__1.x", RealMetadataFactory.example4());
    }
    
    @Test public void testSubqueryRewriteToJoin2() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e1 in /*+ mj */ (select distinct pm1.g1.e1 || 1 FROM pm1.g1)", "SELECT e1 FROM pm3.g1, (SELECT DISTINCT concat(pm1.g1.e1, '1') AS EXPR FROM pm1.g1) AS X__1 WHERE pm3.g1.e1 = X__1.EXPR", RealMetadataFactory.example4());
    }

    @Test public void testSubqueryRewriteToJoin2a() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e1 in /*+ mj */ (select pm1.g1.e1 || 1 FROM pm1.g1)", "SELECT e1 FROM pm3.g1, (SELECT DISTINCT concat(pm1.g1.e1, '1') AS EXPR FROM pm1.g1) AS X__1 WHERE pm3.g1.e1 = X__1.EXPR", RealMetadataFactory.example4());
    }

    /**
     * Even though this situation is essentially the same as above, we don't yet handle it
     */
    @Test public void testSubqueryRewriteToJoin3() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where exists (select pm1.g1.e2 FROM pm1.g1 WHERE pm3.g1.e1 = pm1.g1.e1 || 1)", "SELECT e1 FROM pm3.g1 WHERE EXISTS (SELECT pm1.g1.e2 FROM pm1.g1 WHERE concat(pm1.g1.e1, '1') = pm3.g1.e1 LIMIT 1)", RealMetadataFactory.example4());
    }
    
    @Test public void testSubqueryRewriteToJoinWithOtherCriteria() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e1 in /*+ mj */ (select pm1.g1.e1 FROM pm1.g1 where e2 < pm3.g1.e2)", "SELECT e1 FROM pm3.g1, (SELECT pm1.g1.e1, e2 FROM pm1.g1) AS X__1 WHERE (X__1.e2 < pm3.g1.e2) AND (pm3.g1.e1 = X__1.e1)", RealMetadataFactory.example4());
    }
    
    @Test public void testDontRewriteToJoinWithOtherCriteria() throws Exception {
    	System.setProperty(RuleMergeCriteria.UNNEST_DEFAULT, Boolean.TRUE.toString());
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e1 in /*+ NO_UNNEST */ (select pm1.g1.e1 FROM pm1.g1 where e2 < pm3.g1.e2)", "SELECT e1 FROM pm3.g1 WHERE pm3.g1.e1 IN /*+ NO_UNNEST */ (SELECT pm1.g1.e1 FROM pm1.g1 WHERE e2 < pm3.g1.e2)", RealMetadataFactory.example4());
    }

    @Test public void testSubqueryRewriteToJoinWithAggregate() throws Exception {
    	System.setProperty(RuleMergeCriteria.UNNEST_DEFAULT, Boolean.TRUE.toString());
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e2 < (select max(e2) FROM pm1.g1 where pm3.g1.e1 = e1)", "SELECT e1 FROM pm3.g1, (SELECT MAX(e2) AS MAX, e1 FROM pm1.g1 GROUP BY e1) AS X__1 WHERE (pm3.g1.e2 < X__1.MAX) AND (pm3.g1.e1 = X__1.e1)", RealMetadataFactory.example4());
    }
    
    /**
     * A join will not be used since the predicate cannot be applied after the grouping
     * @throws Exception
     */
    @Test public void testSubqueryRewriteToJoinWithAggregate1() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e2 < (select max(e2) FROM pm1.g1 where pm3.g1.e1 = e1 and pm3.g1.e3 > e3)", "SELECT e1 FROM pm3.g1 WHERE pm3.g1.e2 < (SELECT MAX(e2) FROM pm1.g1 WHERE (e1 = pm3.g1.e1) AND (e3 < pm3.g1.e3))", RealMetadataFactory.example4());
    }
    
    @Test public void testSubqueryRewriteToJoinWithAggregate2() throws Exception {
    	System.setProperty(RuleMergeCriteria.UNNEST_DEFAULT, Boolean.TRUE.toString());
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e2 < (select max(e2) FROM pm1.g1 WHERE pm3.g1.e1 = e1 HAVING min(e3) < pm3.g1.e3)", "SELECT e1 FROM pm3.g1, (SELECT MAX(e2) AS MAX, e1, MIN(e3) AS MIN FROM pm1.g1 GROUP BY e1) AS X__1 WHERE (X__1.MIN < pm3.g1.e3) AND (pm3.g1.e2 < X__1.MAX) AND (pm3.g1.e1 = X__1.e1)", RealMetadataFactory.example4());
    }
    
    @Test public void testSubqueryRewriteToJoinWithGroupingExpression() throws Exception {
    	System.setProperty(RuleMergeCriteria.UNNEST_DEFAULT, Boolean.TRUE.toString());
        TestQueryRewriter.helpTestRewriteCommand("Select distinct e1 from pm3.g1 where exists (select 1 FROM pm1.g1 group by e4 || 'x' HAVING min(e3) || (e4 || 'x') = pm3.g1.e3)", "SELECT DISTINCT e1 FROM pm3.g1, (SELECT 1 AS EXPR, MIN(e3) AS MIN, concat(e4, 'x') AS EXPR_0, concat(MIN(e3), concat(e4, 'x')) AS EXPR_1 FROM pm1.g1 GROUP BY concat(e4, 'x')) AS X__1 WHERE pm3.g1.e3 = X__1.EXPR_1", RealMetadataFactory.example4());
    }

    /**
     * A join will not be used here because of the not
     * @throws Exception
     */
    @Test public void testSubqueryRewriteNot() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e2 not in (select e2 FROM pm1.g1 where pm3.g1.e1 = e1)", "SELECT e1 FROM pm3.g1 WHERE pm3.g1.e2 NOT IN (SELECT e2 FROM pm1.g1 WHERE e1 = pm3.g1.e1)", RealMetadataFactory.example4());
    }

    /**
     * A join will not be used here because of the all
     * @throws Exception
     */
    @Test public void testSubqueryRewriteAll() throws Exception {
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e2 = all (select e2 FROM pm1.g1 where pm3.g1.e1 = e1)", "SELECT e1 FROM pm3.g1 WHERE pm3.g1.e2 = ALL (SELECT e2 FROM pm1.g1 WHERE e1 = pm3.g1.e1)", RealMetadataFactory.example4());
    }
    
    @Test public void testRewriteSubqueryCompare() throws Exception {
    	TestQueryRewriter.helpTestRewriteCommand("select e1 from pm1.g1 where e1 <> ANY (select e1 from pm1.g1)", "SELECT e1 FROM pm1.g1 WHERE e1 <> SOME (SELECT e1 FROM pm1.g1)", RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteSubqueryCompare1() throws Exception {
    	TestQueryRewriter.helpTestRewriteCommand("select e1 from pm1.g1 where e1 <> ALL (select e1 from pm1.g1)", "SELECT e1 FROM pm1.g1 WHERE e1 NOT IN (SELECT e1 FROM pm1.g1)", RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testRewriteSubqueryCompare2() throws Exception {
    	TestQueryRewriter.helpTestRewriteCommand("select e1 from pm1.g1 where e1 = ANY (select e1 from pm1.g1)", "SELECT e1 FROM pm1.g1 WHERE e1 IN (SELECT e1 FROM pm1.g1)", RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testRewriteSubqueryCompare3() throws Exception {
    	TestQueryRewriter.helpTestRewriteCommand("select e1 from pm1.g1 where e1 = ALL (select e1 from pm1.g1)", "SELECT e1 FROM pm1.g1 WHERE e1 = ALL (SELECT e1 FROM pm1.g1)", RealMetadataFactory.example1Cached()); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testSubqueryExpressionJoin() throws Exception {
    	System.setProperty(RuleMergeCriteria.UNNEST_DEFAULT, Boolean.TRUE.toString());
        TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e2 < (Select max(e2) from pm2.g2 where e1 = pm3.g1.e1 having convert(min(e2), string) > pm3.g1.e1)", "SELECT e1 FROM pm3.g1, (SELECT MAX(e2) AS MAX, e1, MIN(e2) AS MIN FROM pm2.g2 GROUP BY e1) AS X__1 WHERE (convert(X__1.MIN, string) > pm3.g1.e1) AND (pm3.g1.e2 < X__1.MAX) AND (pm3.g1.e1 = X__1.e1)", RealMetadataFactory.example4());
    }

    /**
     * Must be handled as a semi-join, rather than a regular join
     */
    @Test public void testSemiJoin() {
        ProcessorPlan plan = helpPlan("Select e1 from pm2.g2 where e2 in /*+ mj */ (select count(e2) FROM pm1.g2 group by e1 having e1 < pm2.g2.e3)", RealMetadataFactory.example4(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e2 AS c_0, g_0.e3 AS c_1, g_0.e1 AS c_2 FROM pm2.g2 AS g_0 ORDER BY c_0" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            1,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
        checkJoinCounts(plan, 1, 0);
    } 
    
    /**
     * This will not plan as a anti semi-join since the cost seems too high
     */
    @Test public void testNoAntiSemiJoinExistsCosting() {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g2 as o where not exists (select 1 from pm3.g1 where e1 = o.e1 having o.e2 = count(e2))", RealMetadataFactory.example4(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e1, g_0.e2 FROM pm1.g2 AS g_0" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    } 
    
    /**
     * Same as above, but the source is much larger, so a semi-join is favorable
     */
    @Test public void testSemiJoinExistsCosting() {
        ProcessorPlan plan = helpPlan("Select e1 from pm2.g2 as o where not exists (select 1 from pm3.g1 where e1 = o.e1 having o.e2 = count(e2))", RealMetadataFactory.example4(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g2 AS g_0 ORDER BY c_0, c_1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            1,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
        checkJoinCounts(plan, 0, 1);
    }
    
    @Test public void testAntiSemiJoinExistsHint() {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g2 as o where not exists /*+ MJ */ (select 1 from pm3.g1 where e1 = o.e1 having o.e2 = count(e2))", RealMetadataFactory.example4(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g2 AS g_0 ORDER BY c_0, c_1" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            1,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
        checkJoinCounts(plan, 0, 1);
    }
    
    @After public void tearDown() {
    	System.setProperty(RuleMergeCriteria.UNNEST_DEFAULT, Boolean.FALSE.toString());
    }
    
    @Test public void testAntiSemiJoinInHint() {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g2 as o where e2 NOT IN /*+ MJ */ (select count(e2) from pm3.g1 where e1 = o.e1)", RealMetadataFactory.example4(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e2 AS c_0, g_0.e1 AS c_1 FROM pm1.g2 AS g_0 ORDER BY c_1, c_0" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            1,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            1,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
        checkJoinCounts(plan, 0, 1);
    }
    
    void checkJoinCounts(ProcessorPlan plan, int semi, int antiSemi) {
    	checkNodeTypes(plan, new int[] {semi, antiSemi}, new Class[] {SemiJoin.class, AntiSemiJoin.class});
    }
    
    @Test public void testNonSemiJoin() throws Exception {
        ProcessorPlan plan = helpPlan("Select x from xmltable('/a/b' passing convert('<a/>', xml) columns x integer path '@x') as t where x = (select count(e2) FROM pm1.g2)", RealMetadataFactory.example4(),  //$NON-NLS-1$
            new String[] {}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            0,      // Access
            0,      // DependentAccess
            1,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }
    
    /**
     * Test to ensure that we don't create an invalid semijoin query when attempting to convert the subquery to a semijoin
     */
    @Test public void testInvalidGeneratedSemijoinQuery() throws Exception {
    	String sql = "SELECT intkey FROM BQT1.SmallA AS A WHERE convert(shortvalue, integer) = (SELECT MAX(convert(shortvalue, integer)) FROM (select * from BQT1.SmallA) AS B WHERE b.intnum = a.intnum) ORDER BY intkey";
    	BasicSourceCapabilities bsc = getTypicalCapabilities();
    	bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
    	bsc.setCapabilitySupport(Capability.QUERY_GROUP_BY, true);
    	TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), new String[] {"SELECT g_0.shortvalue, g_0.intnum, g_0.intkey FROM BQT1.SmallA AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
    }

    @Test public void testInvalidGeneratedSemijoinQuery1() throws Exception {
    	TestQueryRewriter.helpTestRewriteCommand("Select e1 from pm3.g1 where pm3.g1.e2 IN /*+ mj */ (Select max(e2) from pm2.g2 where e1 = pm3.g1.e1)", "SELECT e1 FROM pm3.g1, (SELECT MAX(e2) AS MAX, e1 FROM pm2.g2 GROUP BY e1) AS X__1 WHERE (pm3.g1.e1 = X__1.e1) AND (pm3.g1.e2 = X__1.MAX)", RealMetadataFactory.example4());
    }
    
    @Test public void testCompareSubquery2() throws Exception {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 <= some (select e1 FROM pm2.g1)", example1(),  //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 <= (SELECT MAX(X.e1) FROM (SELECT e1 FROM pm2.g1) AS X)" }, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN); 
    }
    
    @Test public void testUncorrelatedSet() {
        ProcessorPlan plan = helpPlan("Select e1 from pm1.g1 where e1 in /*+ mj */ (select e1 FROM pm2.g1)", RealMetadataFactory.example1Cached(),  //$NON-NLS-1$
            new String[] { "SELECT DISTINCT g_0.e1 FROM pm2.g1 AS g_0", "SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0" }); //$NON-NLS-1$
        checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            1,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
        checkJoinCounts(plan, 0, 0);
    }

}
