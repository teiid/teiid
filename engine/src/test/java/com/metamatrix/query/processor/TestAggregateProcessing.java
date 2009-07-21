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

package com.metamatrix.query.processor;

import static com.metamatrix.query.processor.TestProcessor.*;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import com.metamatrix.query.optimizer.TestAggregatePushdown;
import com.metamatrix.query.optimizer.TestOptimizer;
import com.metamatrix.query.optimizer.capabilities.FakeCapabilitiesFinder;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;

public class TestAggregateProcessing {

	static void sampleDataBQT3(FakeDataManager dataMgr) throws Exception {
		FakeMetadataFacade metadata = FakeMetadataFactory.exampleBQTCached();

		// Group bqt1.smalla
		FakeMetadataObject groupID = (FakeMetadataObject) metadata
				.getGroupID("bqt1.smalla"); //$NON-NLS-1$
		List elementIDs = metadata.getElementIDsInGroupID(groupID);
		List elementSymbols = FakeDataStore.createElements(elementIDs);

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

		dataMgr.registerTuples(groupID, elementSymbols, tuples);

		// Group bqt2.mediumb
		groupID = (FakeMetadataObject) metadata.getGroupID("bqt2.mediumb"); //$NON-NLS-1$
		elementIDs = metadata.getElementIDsInGroupID(groupID);
		elementSymbols = FakeDataStore.createElements(elementIDs);

		tuples = new List[20];
		for (int i = 0; i < tuples.length; i++) {
			tuples[i] = new ArrayList(17);
			tuples[i].add(new Integer(i));
			for (int j = 0; j < 16; j++) {
				tuples[i].add(null);
			}
		}

		dataMgr.registerTuples(groupID, elementSymbols, tuples);
	}

	private void sampleDataBQT_defect9842(FakeDataManager dataMgr) throws Exception {
		FakeMetadataFacade metadata = FakeMetadataFactory.exampleBQTCached();

		// Group bqt1.smalla
		FakeMetadataObject groupID = (FakeMetadataObject) metadata
				.getGroupID("bqt1.smalla"); //$NON-NLS-1$
		List elementIDs = metadata.getElementIDsInGroupID(groupID);
		List elementSymbols = FakeDataStore.createElements(elementIDs);

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

		dataMgr.registerTuples(groupID, elementSymbols, tuples);
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
		ProcessorPlan plan = helpGetPlan(sql, FakeMetadataFactory
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
		ProcessorPlan plan = helpGetPlan(sql, FakeMetadataFactory
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
						new BigDecimal("110.5") }), //$NON-NLS-1$
				Arrays.asList(new Object[] { new Integer(2),
						new BigDecimal("254.5") }) //$NON-NLS-1$
		};

		// Construct data manager with data
		FakeDataManager dataManager = new FakeDataManager();
		sampleDataBQT_defect9842(dataManager);

		// Plan query
		ProcessorPlan plan = helpGetPlan(sql, FakeMetadataFactory
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
        ProcessorPlan plan = helpGetPlan(sql, FakeMetadataFactory.example1Cached());
        
        // Run query
        helpProcess(plan, dataManager, expected);
    }
    
    @SuppressWarnings("unchecked")
	@Test public void testAggregatePushdown() {
    	Command command = helpParse("select e1, count(e2), max(e2) from (select e1, e2, e3 from pm1.g1 union all select e1, e2, e3 from pm1.g2 union all select e1, e2, e3 from pm2.g1) z group by e1"); //$NON-NLS-1$
    	
    	FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
    	capFinder.addCapabilities("pm1", TestAggregatePushdown.getAggregateCapabilities()); //$NON-NLS-1$
    	capFinder.addCapabilities("pm2", TestOptimizer.getTypicalCapabilities()); //$NON-NLS-1$
    	HardcodedDataManager dataManager = new HardcodedDataManager();
    	
    	dataManager.addData("SELECT v_0.c_0, COUNT(v_0.c_1), MAX(v_0.c_1) FROM (SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0) AS v_0 GROUP BY v_0.c_0", //$NON-NLS-1$ 
    			new List[] {
    				Arrays.asList("a", Integer.valueOf(2), Boolean.TRUE), //$NON-NLS-1$
    				Arrays.asList("b", null, null) //$NON-NLS-1$
    			});
    	dataManager.addData("SELECT v_0.c_0, COUNT(v_0.c_1), MAX(v_0.c_1) FROM (SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g2 AS g_0) AS v_0 GROUP BY v_0.c_0", //$NON-NLS-1$ 
    			new List[] {
    				Arrays.asList("a", Integer.valueOf(3), Boolean.FALSE), //$NON-NLS-1$
    				Arrays.asList("b", null, null) //$NON-NLS-1$
    			});
    	dataManager.addData("SELECT g_0.e1, g_0.e2 FROM pm2.g1 AS g_0", //$NON-NLS-1$ 
    			new List[] {
    				Arrays.asList("a", Boolean.FALSE), //$NON-NLS-1$
    				Arrays.asList("xyz", Boolean.FALSE), //$NON-NLS-1$
    				Arrays.asList(null, Boolean.TRUE),
    			});
    	
    	ProcessorPlan plan = helpGetPlan(command, FakeMetadataFactory.example1Cached(), capFinder);
    	
    	List[] expected = new List[] { 
                Arrays.asList(null, Integer.valueOf(1), Boolean.TRUE),
                Arrays.asList("a", Integer.valueOf(6), Boolean.TRUE), //$NON-NLS-1$
                Arrays.asList("b", null, null), //$NON-NLS-1$
                Arrays.asList("xyz", Integer.valueOf(1), Boolean.FALSE) //$NON-NLS-1$
            };    
    	
    	helpProcess(plan, dataManager, expected);
    }

}
