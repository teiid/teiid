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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.unittest.RealMetadataFactory;

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

    
}
