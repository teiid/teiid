/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.processor;

import static org.teiid.query.optimizer.TestOptimizer.*;
import static org.teiid.query.processor.TestProcessor.*;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
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
    
    @Test public void testPartitionedRowNumber1() throws Exception {
        String sql = "select e1, e3, row_number() over (partition by e3 order by e1) as r from pm1.g1 order by r, e1";
        
        List<?>[] expected = new List[] {
                Arrays.asList(null, Boolean.FALSE, 1),
                Arrays.asList("a", Boolean.TRUE, 1),
                Arrays.asList("a", Boolean.FALSE, 2),
                Arrays.asList("c", Boolean.TRUE, 2),
                Arrays.asList("a", Boolean.FALSE, 3),
                Arrays.asList("b", Boolean.FALSE, 4),
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
        
        String sql = "SELECT y.LEAD(ALL convert(amount, string)) OVER (PARTITION BY team ORDER BY \"year\") FROM team_target";
        
        TestOptimizer.getPlan(helpGetCommand(sql, metadata), 
                metadata, new DefaultCapabilitiesFinder(bsc), 
                null, false, new CommandContext()); //$NON-NLS-1$
    }
    
    @Test public void testFirstLastValue() throws Exception {
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        
        String sql = "SELECT FIRST_VALUE(e1) over (order by e2), LAST_VALUE(e2) over (order by e1) from pm1.g1";
        
        ProcessorPlan plan = TestOptimizer.getPlan(helpGetCommand(sql, RealMetadataFactory.example1Cached()), 
                RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), 
                null, true, new CommandContext()); //$NON-NLS-1$
        
        HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0", Arrays.asList("a", 1), Arrays.asList("b", 2));
        
        List<?>[] expected = new List<?>[] {
                Arrays.asList("a", 1),
                Arrays.asList("a", 2),
        }; 
        
        helpProcess(plan, dataMgr, expected);
        
        bsc.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
        bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT FIRST_VALUE(g_0.e1) OVER (ORDER BY g_0.e2), LAST_VALUE(g_0.e2) OVER (ORDER BY g_0.e1) FROM pm1.g1 AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
    }
    
    @Test public void testFirstLastValueNull() throws Exception {
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        
        String sql = "SELECT FIRST_VALUE(e1) over (order by e2), LAST_VALUE(e1) over (order by e2) from pm1.g1";
        
        ProcessorPlan plan = TestOptimizer.getPlan(helpGetCommand(sql, RealMetadataFactory.example1Cached()), 
                RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), 
                null, true, new CommandContext()); //$NON-NLS-1$
        
        HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0", Arrays.asList(null, 1), Arrays.asList("b", 2), Arrays.asList("c", 3));
        
        List<?>[] expected = new List<?>[] {
                Arrays.asList(null, null),
                Arrays.asList(null, "b"),
                Arrays.asList(null, "c")
        }; 
        
        helpProcess(plan, dataMgr, expected);
    }
    
    @Test public void testLead() throws Exception {
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        
        String sql = "SELECT LEAD(e1) over (order by e2), LEAD(e1, 2, 'c') over (order by e2) from pm1.g1";
        
        ProcessorPlan plan = TestOptimizer.getPlan(helpGetCommand(sql, RealMetadataFactory.example1Cached()), 
                RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), 
                null, true, new CommandContext()); //$NON-NLS-1$
        
        HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0", Arrays.asList("a", 1), Arrays.asList("b", 2));
        
        List<?>[] expected = new List<?>[] {
                Arrays.asList("b", "c"),
                Arrays.asList(null, "c"),
        }; 
        
        helpProcess(plan, dataMgr, expected);
        
        bsc.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
        bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT LEAD(g_0.e1) OVER (ORDER BY g_0.e2), LEAD(g_0.e1, 2, 'c') OVER (ORDER BY g_0.e2) FROM pm1.g1 AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
    }
    
    @Test public void testLeadNullValues() throws Exception {
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        
        String sql = "SELECT LEAD(e1, 1, 'default') over (order by e2) from pm1.g1";
        
        ProcessorPlan plan = TestOptimizer.getPlan(helpGetCommand(sql, RealMetadataFactory.example1Cached()), 
                RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), 
                null, true, new CommandContext()); //$NON-NLS-1$
        
        HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.e1, g_0.e2 FROM pm1.g1 AS g_0", Arrays.asList("a", 1), Arrays.asList(null, 2));
        
        List<?>[] expected = new List<?>[] {
                Collections.singletonList(null),
                Collections.singletonList("default"),
        }; 
        
        helpProcess(plan, dataMgr, expected);
    }
    
    @Test public void testLag() throws Exception {
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        
        String sql = "SELECT e1, e3, e2, LAG(e1, 2, 'd') over (partition by e3 order by e2) from pm1.g1";
        
        ProcessorPlan plan = TestOptimizer.getPlan(helpGetCommand(sql, RealMetadataFactory.example1Cached()), 
                RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), 
                null, true, new CommandContext()); //$NON-NLS-1$
        
        HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.e1, g_0.e3, g_0.e2 FROM pm1.g1 AS g_0", 
                Arrays.asList("a", true, 1), Arrays.asList("b", true, 2), Arrays.asList("c", true, 0), 
                Arrays.asList("a", false, 1), Arrays.asList("b", false, 2), Arrays.asList("c", false, 0));
        
        List<?>[] expected = new List<?>[] {
            Arrays.asList("a", true, 1, "d"),
            Arrays.asList("b", true, 2, "c"),
            Arrays.asList("c", true, 0, "d"),
            Arrays.asList("a", false, 1, "d"),
            Arrays.asList("b", false, 2, "c"),
            Arrays.asList("c", false, 0, "d"),
        }; 
        
        helpProcess(plan, dataMgr, expected);
        
        bsc.setCapabilitySupport(Capability.ELEMENTARY_OLAP, true);
        bsc.setCapabilitySupport(Capability.QUERY_AGGREGATES, true);

        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT g_0.e1, g_0.e3, g_0.e2, LAG(g_0.e1, 2, 'd') OVER (PARTITION BY g_0.e3 ORDER BY g_0.e2) FROM pm1.g1 AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
    }
    
    @Test public void testSimpleLead() throws Exception {
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        
        String sql = "select stringkey, lead(stringkey) over (order by stringkey) l from bqt1.smalla order by l";
        
        ProcessorPlan plan = TestOptimizer.getPlan(helpGetCommand(sql, RealMetadataFactory.exampleBQTCached()), 
                RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), 
                null, true, new CommandContext()); //$NON-NLS-1$
        
        HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.StringKey FROM BQT1.SmallA AS g_0", 
                Arrays.asList("b"), Arrays.asList("a"), Arrays.asList("c"), Arrays.asList("d"));
        
        List<?>[] expected = new List<?>[] {
                Arrays.asList("d", null),
                Arrays.asList("a", "b"),
                Arrays.asList("b", "c"),
                Arrays.asList("c", "d"),
        }; 
        
        helpProcess(plan, dataMgr, expected);
    }
    
    @Test public void testInsertWithView() throws Exception {
        String sql = "insert into bqt1.smalla (intkey)\n" + 
                "select rang \n" + 
                "from (\n" + 
                "    select row_number() over(partition by intnum order by intkey) as rang\n" + 
                "    from bqt1.smallb csr\n" + 
                ") v"; 
        
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        
        ProcessorPlan plan = TestOptimizer.getPlan(helpGetCommand(sql, RealMetadataFactory.exampleBQTCached()), 
                RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), 
                null, true, new CommandContext()); //$NON-NLS-1$
        
        HardcodedDataManager dataMgr = new HardcodedDataManager();
        dataMgr.addData("SELECT g_0.IntNum, g_0.IntKey FROM BQT1.SmallB AS g_0", 
                Arrays.asList(1, 1), Arrays.asList(1, 2), Arrays.asList(2, 3), Arrays.asList(2, 4));
        dataMgr.addData("INSERT INTO bqt1.smalla (intkey) VALUES (1)", Arrays.asList(1));
        dataMgr.addData("INSERT INTO bqt1.smalla (intkey) VALUES (2)", Arrays.asList(1));
        
        List<?>[] expected = new List<?>[] {
                Arrays.asList(4),
        }; 
        
        helpProcess(plan, dataMgr, expected);
        
        sql = "insert into bqt1.smalla (intkey)\n" + 
                "select rang \n" + 
                "from (\n" + 
                "    select row_number() over(partition by intnum order by intkey) as rang\n" + 
                "    from bqt1.smallb csr\n" + 
                ") v group by rang";

        plan = TestOptimizer.getPlan(helpGetCommand(sql, RealMetadataFactory.exampleBQTCached()), 
                RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), 
                null, true, new CommandContext()); //$NON-NLS-1$
        
        expected = new List<?>[] {
            Arrays.asList(2),
        }; 
        
        helpProcess(plan, dataMgr, expected);
        
        sql = "insert into bqt1.smalla (intkey)\n" + 
                "select distinct rang \n" + 
                "from (\n" + 
                "    select row_number() over(partition by intnum order by intkey) as rang\n" + 
                "    from bqt1.smallb csr\n" + 
                ") v"; 
        
        plan = TestOptimizer.getPlan(helpGetCommand(sql, RealMetadataFactory.exampleBQTCached()), 
                RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc), 
                null, true, new CommandContext()); //$NON-NLS-1$
        
        helpProcess(plan, dataMgr, expected);
    }
    
}
