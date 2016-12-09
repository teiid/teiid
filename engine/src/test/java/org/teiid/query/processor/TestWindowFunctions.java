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

import static org.teiid.query.optimizer.TestOptimizer.*;
import static org.teiid.query.processor.TestProcessor.*;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.relational.AccessNode;
import org.teiid.query.processor.relational.ProjectNode;
import org.teiid.query.processor.relational.WindowFunctionProjectNode;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.ExecutionFactory.NullOrder;

@SuppressWarnings({"nls", "unchecked"})
public class TestWindowFunctions {

    @Test public void testViewNotRemoved() throws Exception {
    	BasicSourceCapabilities caps = getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
    	caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT y FROM (select row_number() over (order by e1) as y from pm1.g1) as x where x.y = 10", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT v_0.c_0 FROM (SELECT ROW_NUMBER() OVER (ORDER BY g_0.e1) AS c_0 FROM pm1.g1 AS g_0) AS v_0 WHERE v_0.c_0 = 10"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        checkNodeTypes(plan, FULL_PUSHDOWN);                                    
    }
    
    @Test public void testWindowFunctionPushdown() throws Exception {
    	BasicSourceCapabilities caps = getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
    	caps.setCapabilitySupport(Capability.WINDOW_FUNCTION_ORDER_BY_AGGREGATES, true);
    	caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("select max(e1) over (order by e1) as y from pm1.g1", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT MAX(g_0.e1) OVER (ORDER BY g_0.e1) FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        checkNodeTypes(plan, FULL_PUSHDOWN);                                    
    }

    @Test public void testWindowFunctionPushdown1() throws Exception {
    	BasicSourceCapabilities caps = getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
    	caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("select max(e1) over (order by e1) as y from pm1.g1", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT g_0.e1 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
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
    
    @Test public void testWindowFunctionPushdown2() throws Exception {
    	BasicSourceCapabilities caps = getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
    	caps.setCapabilitySupport(Capability.WINDOW_FUNCTION_ORDER_BY_AGGREGATES, true);
    	caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
    	caps.setSourceProperty(Capability.QUERY_ORDERBY_DEFAULT_NULL_ORDER, NullOrder.UNKNOWN);
        ProcessorPlan plan = TestOptimizer.helpPlan("select max(e1) over (order by e1 nulls first) as y from pm1.g1", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT g_0.e1 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        checkNodeTypes(plan, new int[] {1, 1, 1}, new Class<?>[] {AccessNode.class, WindowFunctionProjectNode.class, ProjectNode.class});                                    
        
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY_NULL_ORDERING, true);
        plan = TestOptimizer.helpPlan("select max(e1) over (order by e1 nulls first) as y from pm1.g1", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                new String[] {
                    "SELECT MAX(g_0.e1) OVER (ORDER BY g_0.e1 NULLS FIRST) FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }
    
    @Test public void testWindowFunctionPushdown3() throws Exception {
    	BasicSourceCapabilities caps = getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
    	caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_COUNT, true);
    	caps.setCapabilitySupport(Capability.QUERY_AGGREGATES_DISTINCT, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("select count(distinct e1) over (partition by e2) as y from pm1.g1", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        checkNodeTypes(plan, new int[] {1, 1, 1}, new Class<?>[] {AccessNode.class, WindowFunctionProjectNode.class, ProjectNode.class});                                    
        
        caps.setCapabilitySupport(Capability.WINDOW_FUNCTION_DISTINCT_AGGREGATES, true);
        plan = TestOptimizer.helpPlan("select count(distinct e1) over (partition by e2) as y from pm1.g1", //$NON-NLS-1$
                RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                new String[] {
                    "SELECT COUNT(DISTINCT g_0.e1) OVER (PARTITION BY g_0.e2) FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    
	@Test public void testRanking() throws Exception {
    	String sql = "select e1, row_number() over (order by e1), rank() over (order by e1), dense_rank() over (order by e1 nulls last) from pm1.g1";
        
    	List<?>[] expected = new List[] {
        		Arrays.asList("a", 2, 2, 1),
        		Arrays.asList(null, 1, 1, 4),
        		Arrays.asList("a", 3, 2, 1),
        		Arrays.asList("c", 6, 6, 3),
        		Arrays.asList("b", 5, 5, 2),
        		Arrays.asList("a", 4, 2, 1),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
	
	@Test public void testRankingView() throws Exception {
    	String sql = "select * from (select e1, row_number() over (order by e1) as rn, rank() over (order by e1) as r, dense_rank() over (order by e1 nulls last) as dr from pm1.g1) as x where e1 = 'a'";
        
    	List<?>[] expected = new List[] {
        		Arrays.asList("a", 2, 2, 1),
        		Arrays.asList("a", 3, 2, 1),
        		Arrays.asList("a", 4, 2, 1),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testPartitionedMax() throws Exception {
    	String sql = "select e2, max(e1) over (partition by e2) as y from pm1.g1";
        
    	List<?>[] expected = new List[] {
        		Arrays.asList(0, "a"),
        		Arrays.asList(1, "c"),
        		Arrays.asList(3, "a"),
        		Arrays.asList(1, "c"),
        		Arrays.asList(2, "b"),
        		Arrays.asList(0, "a"),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testUnrelatedWindowFunctionOrderBy() throws Exception {
    	String sql = "select e2, e1 from pm1.g1 order by count(e1) over (partition by e3), e2";
        
    	List<?>[] expected = new List[] {
    			Arrays.asList(1, "c"),
        		Arrays.asList(3, "a"),
        		Arrays.asList(0, "a"),
        		Arrays.asList(0, "a"),
        		Arrays.asList(1, null),
        		Arrays.asList(2, "b"),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testWindowFunctionOrderBy() throws Exception {
    	String sql = "select e2, e1, count(e1) over (partition by e3) as c from pm1.g1 order by c, e2";
        
    	List<?>[] expected = new List[] {
        		Arrays.asList(1, "c", 2),
        		Arrays.asList(3, "a", 2),
        		Arrays.asList(0, "a", 3),
        		Arrays.asList(0, "a", 3),
        		Arrays.asList(1, null, 3),
        		Arrays.asList(2, "b", 3),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }

    /**
     * Note that we've optimized the ordering to be performed prior to the windowing.
     * If we change the windowing logic to not preserve the incoming row ordering, then this optimization will need to change
     * @throws Exception
     */
    @Test public void testCountDuplicates() throws Exception {
    	String sql = "select e1, count(e1) over (order by e1) as c from pm1.g1 order by e1";
        
    	List<?>[] expected = new List[] {
        		Arrays.asList("a", 2),
        		Arrays.asList("a", 2),
        		Arrays.asList("b", 3),
        };
    	
    	HardcodedDataManager dataManager = new HardcodedDataManager();
    	dataManager.addData("SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0", new List[] {Arrays.asList("a"), Arrays.asList("a"), Arrays.asList("b")});
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testEmptyOver() throws Exception {
    	String sql = "select e1, max(e1) over () as c from pm1.g1";
        
    	List<?>[] expected = new List[] {
        		Arrays.asList("a", "c"),
        		Arrays.asList(null, "c"),
        		Arrays.asList("a", "c"),
        		Arrays.asList("c", "c"),
        		Arrays.asList("b", "c"),
        		Arrays.asList("a", "c"),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testRowNumberMedian() throws Exception {
    	String sql = "select e1, r, c from (select e1, row_number() over (order by e1) as r, count(*) over () c from pm1.g1) x where r = ceiling(c/2)";
        
    	List<?>[] expected = new List[] {
        		Arrays.asList("a", 3, 6),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testPartitionedRowNumber() throws Exception {
    	String sql = "select e1, e3, row_number() over (partition by e3 order by e1) as r from pm1.g1 order by r limit 2";
        
    	List<?>[] expected = new List[] {
        		Arrays.asList(null, Boolean.FALSE, 1),
        		Arrays.asList("a", Boolean.TRUE, 1),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached(), TestOptimizer.getGenericFinder());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testPartitionedDistinctCount() throws Exception {
    	String sql = "select e1, e3, count(distinct e1) over (partition by e3) as r from pm1.g1 order by e1, e3";
        
    	List<?>[] expected = new List[] {
    			Arrays.asList(null, Boolean.FALSE, 2),
        		Arrays.asList("a", Boolean.FALSE, 2),
        		Arrays.asList("a", Boolean.FALSE, 2),
        		Arrays.asList("a", Boolean.TRUE, 2),
        		Arrays.asList("b", Boolean.FALSE, 2),
        		Arrays.asList("c", Boolean.TRUE, 2),
        };
    	
    	FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }

	@Test public void testXMLAggDelimitedConcatFiltered() throws Exception {
		String sql = "SELECT XMLAGG(XMLPARSE(CONTENT (case when rn = 1 then '' else ',' end) || e1 WELLFORMED) ORDER BY rn) FROM (SELECT e1, e2, row_number() FILTER (WHERE e1 IS NOT NULL) over (order by e1) as rn FROM pm1.g1) X"; //$NON-NLS-1$
        
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("a,a,a,b,c"),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
	
    @Test public void testViewCriteria() throws Exception {
    	BasicSourceCapabilities caps = getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT * FROM (select e1, e3, count(distinct e1) over (partition by e3) as r from pm1.g1) as x where x.e1 = 'a'", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT g_0.e1, g_0.e3 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("a", Boolean.FALSE, 2),
        		Arrays.asList("a", Boolean.TRUE, 2),
        		Arrays.asList("a", Boolean.FALSE, 2),
        }; 
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testViewCriteriaPushdown() throws Exception {
    	BasicSourceCapabilities caps = getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT * FROM (select e1, e3, count(distinct e1) over (partition by e3) as r from pm1.g1) as x where x.e3 = false", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT g_0.e1, g_0.e3 FROM pm1.g1 AS g_0 WHERE g_0.e3 = FALSE"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("a", Boolean.FALSE, 2),
        		Arrays.asList(null, Boolean.FALSE, 2),
        		Arrays.asList("b", Boolean.FALSE, 2),
        		Arrays.asList("a", Boolean.FALSE, 2),
        }; 
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testViewCriteriaPushdown1() throws Exception {
    	BasicSourceCapabilities caps = getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT * FROM (select e1, e3, count(e1) over (partition by e3 order by e2) as r from pm1.g1) as x where x.e3 = false", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT g_0.e1, g_0.e3, g_0.e2 FROM pm1.g1 AS g_0 WHERE g_0.e3 = FALSE"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("a", Boolean.FALSE, 2),
        		Arrays.asList(null, Boolean.FALSE, 2),
        		Arrays.asList("b", Boolean.FALSE, 3),
        		Arrays.asList("a", Boolean.FALSE, 2),
        }; 
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testViewLimit() throws Exception {
    	BasicSourceCapabilities caps = getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT * FROM (select e1, e3, count(distinct e1) over (partition by e3) as r from pm1.g1) as x limit 1", //$NON-NLS-1$
                                      RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(caps),
                                      new String[] {
                                          "SELECT g_0.e1, g_0.e3 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    
        FakeDataManager dataManager = new FakeDataManager();
    	sampleData1(dataManager);
        List<?>[] expected = new List<?>[] {
        		Arrays.asList("a", Boolean.FALSE, 2),
        }; 
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testOrderByConstant() throws TeiidComponentException, TeiidProcessingException {
    	String sql = "select 1 as jiraissue_assignee, "
    			+ "row_number() over(order by subquerytable.jiraissue_id desc) as calculatedfield1 "
    			+ "from  pm1.g1 as jiraissue left outer join "
    			+ "(select jiraissue_sub.e1 as jiraissue_assignee, jiraissue_sub.e1 as jiraissue_id from pm2.g2 jiraissue_sub "
    			+ "where (jiraissue_sub.e4 between null and 2)"
    			+ " ) subquerytable on jiraissue.e1 = subquerytable.jiraissue_assignee";
    	
    	BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
    	bsc.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
    	bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES_MAX, true);
    	bsc.setCapabilitySupport(Capability.QUERY_ORDERBY_NULL_ORDERING, true);
    	bsc.setCapabilitySupport(Capability.WINDOW_FUNCTION_ORDER_BY_AGGREGATES, true);
    	 
    	ProcessorPlan plan = TestOptimizer.helpPlan(sql, 
                RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(bsc),
                new String[] {
                    "SELECT 1 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

    	checkNodeTypes(plan, new int[] {1, 1, 1}, new Class<?>[] {AccessNode.class, WindowFunctionProjectNode.class, ProjectNode.class});                                    
    }
    
    @Test public void testPartialProjection() throws TeiidComponentException, TeiidProcessingException {
    	String sql = "SELECT user() AS a, "
        		+ " AVG(e2) OVER ( ) AS b,"
        		+ " MAX(e2) OVER ( ) AS b"
        		+ " FROM pm1.g1";

    	HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT ROUND(convert((g_0.L_DISCOUNT - AVG(g_0.L_DISCOUNT) OVER ()), FLOAT), 0) FROM TPCR_Oracle_9i.LINEITEM AS g_0", Arrays.asList(2.0f), Arrays.asList(2.0f));
    	
    	BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
    	bsc.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
    	bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES_AVG, true);
    	bsc.setCapabilitySupport(Capability.WINDOW_FUNCTION_ORDER_BY_AGGREGATES, true);
    	 
    	ProcessorPlan plan = TestOptimizer.helpPlan(sql, 
                RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(bsc),
                new String[] {
                    "SELECT AVG(g_0.e2) OVER (), g_0.e2 FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

    	checkNodeTypes(plan, new int[] {1, 1, 1}, new Class<?>[] {AccessNode.class, WindowFunctionProjectNode.class, ProjectNode.class});
    	
        List<?>[] expected =
                new List<?>[] {Arrays.asList(null, BigDecimal.valueOf(1.5), 2), Arrays.asList(null, BigDecimal.valueOf(1.5), 2)}; 

        dataMgr.addData("SELECT AVG(g_0.e2) OVER (), g_0.e2 FROM pm1.g1 AS g_0", //$NON-NLS-1$
                        Arrays.asList(1.5, 2), Arrays.asList(1.5, 1));

        helpProcess(plan, dataMgr, expected);
        
        //should completely eliminate the window function node 
        plan = TestOptimizer.helpPlan("SELECT uuid() AS a, AVG(e2) OVER ( ) AS b FROM pm1.g1", 
                RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(bsc),
                new String[] {
                    "SELECT AVG(g_0.e2) OVER () FROM pm1.g1 AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

    	checkNodeTypes(plan, new int[] {1, 1}, new Class<?>[] {AccessNode.class, ProjectNode.class});
    }
    
    @Test public void testSourceWindowFunction() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table team_target (amount integer, team integer, \"year\" integer); "
                + "create foreign function lead (arg string) returns string options (\"teiid_rel:aggregate\" true, \"teiid_rel:analytic\" true)", "x", "y");
        
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        
        String sql = "SELECT LEAD(ALL convert(amount, string)) OVER (PARTITION BY team ORDER BY \"year\") FROM team_target";
        
        TestOptimizer.getPlan(helpGetCommand(sql, metadata, null), 
                metadata, new DefaultCapabilitiesFinder(bsc), 
                null, false, new CommandContext()); //$NON-NLS-1$
    }
    
}
