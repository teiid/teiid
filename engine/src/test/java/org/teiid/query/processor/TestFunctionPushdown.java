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

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.FakeFunctionMetadataSource;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings({"nls", "unchecked"})
public class TestFunctionPushdown {

	@Test public void testMustPushdownOverMultipleSourcesWithoutSupport() throws Exception {
		QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource()));
		
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        String sql = "select func(x.e1) from pm1.g1 as x, pm2.g1 as y where x.e2 = y.e2"; //$NON-NLS-1$
        
        helpPlan(sql, metadata, null, capFinder, 
                                      new String[] {}, ComparisonMode.FAILED_PLANNING); //$NON-NLS-1$ 
	}

	@Test public void testMustPushdownOverMultipleSources() throws Exception {
		QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource()));
		
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport("misc.namespace.func", true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        String sql = "select func(x.e1) from pm1.g1 as x, pm2.g1 as y where x.e2 = y.e2"; //$NON-NLS-1$
        
        ProcessorPlan plan = helpPlan(sql, metadata, null, capFinder, 
                                      new String[] {"SELECT g_0.e2 AS c_0, func(g_0.e1) AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e2 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ 
        
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e2 AS c_0, func(g_0.e1) AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0", new List[] {Arrays.asList(1, "a")});
        dataManager.addData("SELECT g_0.e2 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0", new List[] {Arrays.asList(1), Arrays.asList(2)});
        
        TestProcessor.helpProcess(plan, dataManager, new List[] {Arrays.asList("a")});
	}

	@Test public void testMustPushdownOverMultipleSourcesWithView() throws Exception {
		QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource()));
		
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport("misc.namespace.func", true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        String sql = "select func(x.e1) from (select x.* from pm1.g1 as x, pm2.g1 as y where x.e2 = y.e2 order by e1 limit 10) as x"; //$NON-NLS-1$
        
        ProcessorPlan plan = helpPlan(sql, metadata, null, capFinder, 
                                      new String[] {"SELECT g_0.e2 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e2 AS c_0, func(g_0.e1) AS c_1, g_0.e1 AS c_2 FROM pm1.g1 AS g_0 ORDER BY c_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ 
        
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.e2 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0", new List[] {Arrays.asList(1)});
        dataManager.addData("SELECT g_0.e2 AS c_0, func(g_0.e1) AS c_1, g_0.e1 AS c_2 FROM pm1.g1 AS g_0 ORDER BY c_0", new List[] {Arrays.asList(1, "aa", "a"), Arrays.asList(2, "bb", "b")});
        
        TestProcessor.helpProcess(plan, dataManager, new List[] {Arrays.asList("aa")});
	}
	
	@Test public void testMustPushdownOverMultipleSourcesWithViewDupRemoval() throws Exception {
		QueryMetadataInterface metadata = RealMetadataFactory.createTransformationMetadata(RealMetadataFactory.example1Cached().getMetadataStore(), "example1", new FunctionTree("foo", new FakeFunctionMetadataSource()));
		
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setFunctionSupport("misc.namespace.func", true);
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        String sql = "select func(x.e1) from (select distinct x.* from pm1.g1 as x, pm2.g1 as y where x.e2 = y.e2 order by e1 limit 10) as x"; //$NON-NLS-1$
        
        helpPlan(sql, metadata, null, capFinder, 
                                      new String[] {}, ComparisonMode.FAILED_PLANNING); //$NON-NLS-1$ 
	}
	
}
