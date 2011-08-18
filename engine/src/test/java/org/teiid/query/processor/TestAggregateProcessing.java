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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.impl.BufferManagerImpl;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.TestAggregatePushdown;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.SourceSystemFunctions;

@SuppressWarnings({"nls", "unchecked"})
public class TestAggregateProcessing {

	static void sampleDataBQT3(FakeDataManager dataMgr) throws Exception {
		QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

		// Group bqt1.smalla

		List[] tuples = new List[20];
		for (int i = 0; i < tuples.length; i++) {
			tuples[i] = new ArrayList(17);
			tuples[i].add(new Integer(i));
			tuples[i].add("" + i); //$NON-NLS-1$
			tuples[i].add(new Integer(i + 1));
			for (int j = 0; j < 14; j++) {
				tuples[i].add(null);
			}
		}

		dataMgr.registerTuples(metadata, "bqt1.smalla", tuples); //$NON-NLS-1$

		tuples = new List[20];
		for (int i = 0; i < tuples.length; i++) {
			tuples[i] = new ArrayList(17);
			tuples[i].add(new Integer(i));
			for (int j = 0; j < 16; j++) {
				tuples[i].add(null);
			}
		}

		dataMgr.registerTuples(metadata, "bqt2.mediumb", tuples); //$NON-NLS-1$
	}

	private void sampleDataBQT_defect9842(FakeDataManager dataMgr) throws Exception {
		QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();

		List[] tuples = new List[5];
		for (int i = 0; i < tuples.length; i++) {
			int k = i + 10;
			tuples[i] = new ArrayList(17);
			if (i < 2) {
				tuples[i].add(new Integer(1)); // need duplicate values
			} else {
				tuples[i].add(new Integer(2)); // need duplicate values
			}
			tuples[i].add("" + k); //$NON-NLS-1$
			tuples[i].add(new Integer(k + 1));
			tuples[i].add("" + (k + 1)); //$NON-NLS-1$
			tuples[i].add(new Float(0.5));
			for (int j = 0; j < 8; j++) {
				tuples[i].add(null);
			}
			tuples[i].add(new Short((short) k));
			tuples[i].add(null);
			tuples[i].add(new BigDecimal("" + k)); //$NON-NLS-1$
			tuples[i].add(null);
		}

		dataMgr.registerTuples(metadata, "bqt1.smalla", tuples); //$NON-NLS-1$
	}

	@Test public void testAggregateOnBQT() throws Exception {
		// Create query
		String sql = "SELECT IntKey, SUM(IntNum) FROM BQT1.SmallA GROUP BY IntKey, IntNum HAVING IntNum > 10 ORDER BY IntKey"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] {
				Arrays.asList(new Object[] { new Integer(10), new Long(11) }),
				Arrays.asList(new Object[] { new Integer(11), new Long(12) }),
				Arrays.asList(new Object[] { new Integer(12), new Long(13) }),
				Arrays.asList(new Object[] { new Integer(13), new Long(14) }),
				Arrays.asList(new Object[] { new Integer(14), new Long(15) }),
				Arrays.asList(new Object[] { new Integer(15), new Long(16) }),
				Arrays.asList(new Object[] { new Integer(16), new Long(17) }),
				Arrays.asList(new Object[] { new Integer(17), new Long(18) }),
				Arrays.asList(new Object[] { new Integer(18), new Long(19) }),
				Arrays.asList(new Object[] { new Integer(19), new Long(20) }) };

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleDataBQT3(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory
				.exampleBQTCached());

		// Run query
		helpProcess(plan, dataManager, expected);
	}

	@Test public void testAggregateOnBQT2() throws Exception {
		// Create query
		String sql = "SELECT IntNum, IsNotNull FROM (SELECT IntNum, LongNum, COUNT(IntNum) AS IsNotNull FROM BQT1.SmallA GROUP BY IntNum, LongNum HAVING LongNum IS NULL ) AS x ORDER BY IntNum, IsNotNull"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] {
				Arrays.asList(new Object[] { new Integer(1), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(2), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(3), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(4), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(5), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(6), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(7), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(8), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(9), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(10), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(11), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(12), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(13), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(14), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(15), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(16), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(17), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(18), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(19), new Integer(1) }),
				Arrays.asList(new Object[] { new Integer(20), new Integer(1) }) };

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleDataBQT3(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory
				.exampleBQTCached());

		// Run query
		helpProcess(plan, dataManager, expected);
	}

	@Test public void testAggregateOnBQT_defect9842() throws Exception {
		// Create query
		String sql = "SELECT IntKey, SUM((BigDecimalValue)*(ShortValue)-(BigDecimalValue)*(ShortValue)*(FloatNum)) " + //$NON-NLS-1$
				"AS MySum FROM BQT1.SmallA GROUP BY IntKey ORDER BY IntKey"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] {
				Arrays.asList(new Object[] { new Integer(1),
						new BigDecimal("110.5000000") }), //$NON-NLS-1$
				Arrays.asList(new Object[] { new Integer(2),
						new BigDecimal("254.5000000") }) //$NON-NLS-1$
		};

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleDataBQT_defect9842(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory
				.exampleBQTCached());

		// Run query
		helpProcess(plan, dataManager, expected);
	}
	
    @Test public void testCase186260() {
        /*
         * This case revealed that an expression like "COUNT( DISTINCT e1 )", where the type of e1 is 
         * anything but integer, was not handled properly.  We tried to use "integer" (the type of the
         * COUNT expression) to work with the e1 tuples.
         */
        // Create query 
        String sql = "SELECT COUNT(DISTINCT pm1.g2.e1), COUNT(DISTINCT pm1.g3.e1) FROM pm1.g2, pm1.g3"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(3), new Integer(3) }),
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
	@Test public void testAggregatePushdown() {
    	Command command = helpParse("select e1, count(e2), max(e2) from (select e1, e2, e3 from pm1.g1 union all select e1, e2, e3 from pm1.g2 union all select e1, e2, e3 from pm2.g1) z group by e1"); //$NON-NLS-1$
    	
    	FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
    	capFinder.addCapabilities("pm1", TestAggregatePushdown.getAggregateCapabilities()); //$NON-NLS-1$
    	capFinder.addCapabilities("pm2", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$
    	HardcodedDataManager dataManager = new HardcodedDataManager();
    	
    	dataManager.addData("SELECT v_0.c_0, COUNT(v_0.c_1), MAX(v_0.c_1) FROM (SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0) AS v_0 GROUP BY v_0.c_0", //$NON-NLS-1$ 
    			new List[] {
    				Arrays.asList("a", Integer.valueOf(2), Integer.valueOf(1)), //$NON-NLS-1$
    			});
    	dataManager.addData("SELECT v_0.c_0, COUNT(v_0.c_1), MAX(v_0.c_1) FROM (SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g2 AS g_0) AS v_0 GROUP BY v_0.c_0", //$NON-NLS-1$ 
    			new List[] {
    				Arrays.asList("a", Integer.valueOf(3), Integer.valueOf(2)), //$NON-NLS-1$
    			});
    	dataManager.addData("SELECT g_0.e1, g_0.e2 FROM pm2.g1 AS g_0", //$NON-NLS-1$ 
    			new List[] {
    				Arrays.asList("a", Integer.valueOf(3)), //$NON-NLS-1$
    				Arrays.asList("xyz", Integer.valueOf(4)), //$NON-NLS-1$
    				Arrays.asList(null, Integer.valueOf(5)),
    			});
    	
    	ProcessorPlan plan = helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);
    	
    	List[] expected = new List[] { 
                Arrays.asList(null, Integer.valueOf(1), Integer.valueOf(5)),
                Arrays.asList("a", Integer.valueOf(6), Integer.valueOf(3)), //$NON-NLS-1$
                Arrays.asList("xyz", Integer.valueOf(1), Integer.valueOf(4)) //$NON-NLS-1$
            };    
    	
    	helpProcess(plan, dataManager, expected);
    }
    
	@Test public void testUnionAggregatePushdown() {
    	Command command = helpParse("select count(*), max(e3) from (select e1, e2, e3 from pm1.g1 union all (select convert(e2, string) as a, e2, e3 from pm2.g2 order by a limit 10)) x group by e1, e2"); //$NON-NLS-1$
    	
    	FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
    	capFinder.addCapabilities("pm1", TestAggregatePushdown.getAggregateCapabilities()); //$NON-NLS-1$
    	BasicSourceCapabilities bac = TestAggregatePushdown.getAggregateCapabilities();
    	bac.setFunctionSupport("convert", true); //$NON-NLS-1$
    	capFinder.addCapabilities("pm2", bac); //$NON-NLS-1$
    	HardcodedDataManager dataManager = new HardcodedDataManager();
    	
    	dataManager.addData("SELECT v_0.c_0, v_0.c_1, COUNT(*), MAX(v_0.c_2) FROM (SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2 FROM pm1.g1 AS g_0) AS v_0 GROUP BY v_0.c_0, v_0.c_1", //$NON-NLS-1$ 
    			new List[] {
    				Arrays.asList("2", Integer.valueOf(2), Integer.valueOf(2), Boolean.FALSE), //$NON-NLS-1$
    				Arrays.asList("1", Integer.valueOf(1), Integer.valueOf(3), Boolean.TRUE), //$NON-NLS-1$
    			});
    	dataManager.addData("SELECT v_0.c_0, v_0.c_1, COUNT(*), MAX(v_0.c_2) FROM (SELECT convert(g_0.e2, string) AS c_0, g_0.e2 AS c_1, g_0.e3 AS c_2 FROM pm2.g2 AS g_0 ORDER BY c_0 LIMIT 10) AS v_0 GROUP BY v_0.c_0, v_0.c_1", //$NON-NLS-1$ 
    			new List[] {
    				Arrays.asList("1", Integer.valueOf(1), Integer.valueOf(4), Boolean.FALSE), //$NON-NLS-1$
    			});
    	
    	ProcessorPlan plan = helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);
    	
    	List[] expected = new List[] { 
                Arrays.asList(Integer.valueOf(7), Boolean.TRUE),
                Arrays.asList(Integer.valueOf(2), Boolean.FALSE),
            };    
    	
    	helpProcess(plan, dataManager, expected);
    }
	
    @Test public void testPushDownOverUnionMixed1() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestAggregatePushdown.getAggregateCapabilities();
        caps.setFunctionSupport(SourceSystemFunctions.POWER, true);
        caps.setFunctionSupport(SourceSystemFunctions.CONVERT, true);
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$
        
        ProcessorPlan plan = helpGetPlan(helpParse("select max(e2), count(*), stddev_pop(e2), var_samp(e2) from (select e1, e2 from pm1.g1 union all select e1, e2 from pm2.g2) z"), RealMetadataFactory.example1Cached(), capFinder); //$NON-NLS-1$
        
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e2 FROM pm1.g1 AS g_0", new List[] {Arrays.asList(1), Arrays.asList(2)});
        dataManager.addData("SELECT MAX(v_0.c_0), COUNT(*), COUNT(v_0.c_0), SUM(power(v_0.c_0, 2)), SUM(v_0.c_0) FROM (SELECT g_0.e2 AS c_0 FROM pm2.g2 AS g_0) AS v_0 HAVING COUNT(*) > 0", new List[] {Arrays.asList(5, 6, 4, BigInteger.valueOf(50l), 10l)});
        
        List[] expected = new List[] {
    		Arrays.asList(5, 8, 2.1147629234082532, 5.366666666666666),
        }; 
        
        helpProcess(plan, dataManager, expected);
    }

    @Test public void testBooleanAgg() {
    	String sql = "select every(e3), any(e3) from pm1.g1"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList(Boolean.FALSE, Boolean.TRUE),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testStatsFunctions() {
    	String sql = "select stddev_pop(e2), var_samp(e2) from pm1.g1"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList(1.0671873729054748, 1.3666666666666667),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testStatsFunctions1() {
    	String sql = "select stddev_samp(e2), var_pop(e2) from (select 2 e2) x"; //$NON-NLS-1$
    	
        List[] expected = new List[] {
        		Arrays.asList(null, 0.0),
        };    
    
        FakeDataManager dataManager = new FakeDataManager();
        sampleData1(dataManager);
        
        ProcessorPlan plan = helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
        
        helpProcess(plan, dataManager, expected);
    }
    
	@Test public void testJira1621() throws Exception {
		// Create query
		String sql = "SELECT sum(t2.e4) as s, max(t1.e1 || t2.e1) FROM pm1.g1 as t1, pm1.g2 as t2, pm1.g3 as t3 WHERE t1.e1 = coalesce(t2.e1, 'b') AND t2.e2 = t3.e2 GROUP BY t2.e2, t2.e3, t3.e2 ORDER BY s"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] {
				Arrays.asList(null, "cc"),
				Arrays.asList(0.0, "bb"),
				Arrays.asList(2.0, null),
				Arrays.asList(21.0, "aa"),
				Arrays.asList(24.0, "aa")
		};

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleData1(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

		// Run query
		helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testArrayAggOrderByPersistence() throws Exception {
		// Create query
		String sql = "SELECT array_agg(e2 order by e1) from pm1.g1 group by e3"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] {
				Arrays.asList((Object)new Integer[] {1, 0, 0, 2}),
				Arrays.asList((Object)new Integer[] {3, 1}),
		};

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleData1(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
		CommandContext cc = TestProcessor.createCommandContext();
		BufferManagerImpl impl = BufferManagerFactory.getTestBufferManager(0, 2);
		impl.setUseWeakReferences(false);
		cc.setBufferManager(impl);
		// Run query
		helpProcess(plan, cc, dataManager, expected);
	}
	
	@Test public void testDupGroupCombination() throws Exception {
        String sql = "select count(e2), e1 from (select distinct e1, e2, e3 from pm1.g1) x group by e1"; //$NON-NLS-1$

        List[] expected = new List[] {
				Arrays.asList(2, "a"),
		};

		HardcodedDataManager dataManager = new HardcodedDataManager();
		dataManager.addData("SELECT pm1.g1.e1, pm1.g1.e2, pm1.g1.e3 FROM pm1.g1", new List[] {
				Arrays.asList("a", 0, Boolean.TRUE),
				Arrays.asList("a", 0, Boolean.FALSE),
		});

		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
		helpProcess(plan, dataManager, expected);
	}
	
	@Test public void testAggFilter() throws Exception {
		// Create query
		String sql = "SELECT e2, count(*) filter (where e3) from pm1.g1 group by e2 order by e2"; //$NON-NLS-1$

		// Create expected results
		List[] expected = new List[] {
				Arrays.asList(0, 0),
				Arrays.asList(1, 1),
				Arrays.asList(2, 0),
				Arrays.asList(3, 1),
		};

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleData1(dataManager);

		ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());
		helpProcess(plan, dataManager, expected);
	}

}
