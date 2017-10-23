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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestSetProcessing {
    
    @Test public void testExcept() throws Exception {
        String sql = "select e1, e2 from pm1.g2 except select e1, 1 from pm1.g2"; //$NON-NLS-1$
        
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(), new String[] {"SELECT pm1.g2.e1 FROM pm1.g2", "SELECT pm1.g2.e1, pm1.g2.e2 FROM pm1.g2"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$  //$NON-NLS-2$
        
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] {"a", 0}), //$NON-NLS-1$
            Arrays.asList(new Object[] {"a", 3}), //$NON-NLS-1$
            Arrays.asList(new Object[] {"b", 2}), //$NON-NLS-1$
            };

        FakeDataManager manager = new FakeDataManager();
        TestProcessor.sampleData1(manager);
        TestProcessor.helpProcess(plan, manager, expected);
    }
    
    @Test public void testIntersect() throws Exception {
        String sql = "select e1, e2 from pm1.g2 intersect select e1, 1 from pm1.g2"; //$NON-NLS-1$
        
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), null, new DefaultCapabilitiesFinder(), new String[] {"SELECT pm1.g2.e1 FROM pm1.g2", "SELECT pm1.g2.e1, pm1.g2.e2 FROM pm1.g2"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$  //$NON-NLS-2$
        
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] {null, 1}),
            Arrays.asList(new Object[] {"c", 1}), //$NON-NLS-1$
            };

        FakeDataManager manager = new FakeDataManager();
        TestProcessor.sampleData1(manager);
        TestProcessor.helpProcess(plan, manager, expected);
    }
    
    @Test public void testIntersectExcept() {
        String sql = "select e1, e2 from pm1.g2 except select e1, 1 from pm1.g2 intersect select 'a', e2 from pm1.g2"; //$NON-NLS-1$
        
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {"SELECT g_0.e1 FROM pm1.g2 AS g_0", "SELECT g_0.e1, g_0.e2 FROM pm1.g2 AS g_0", "SELECT g_0.e2 FROM pm1.g2 AS g_0"}); //$NON-NLS-1$  //$NON-NLS-2$ //$NON-NLS-3$
        
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] {null, 1}),
            Arrays.asList(new Object[] {"a", 0}), //$NON-NLS-1$
            Arrays.asList(new Object[] {"a", 3}), //$NON-NLS-1$
            Arrays.asList(new Object[] {"b", 2}), //$NON-NLS-1$
            Arrays.asList(new Object[] {"c", 1}), //$NON-NLS-1$
            };

        FakeDataManager manager = new FakeDataManager();
        TestProcessor.sampleData1(manager);
        TestProcessor.helpProcess(plan, manager, expected);
    }
    
    @Test public void testUnionExcept() {
        String sql = "(select 'a' union select 'b' union select 'c') except select 'c'"; //$NON-NLS-1$
        
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {}); 
        
        TestOptimizer.checkNodeTypes(plan, new int[] {
                0,      // Access
                0,      // DependentAccess
                0,      // DependentSelect
                0,      // DependentProject
                0,      // DupRemove
                0,      // Grouping
                0,      // NestedLoopJoinStrategy
                1,      // MergeJoinStrategy
                0,      // Null
                0,      // PlanExecution
                4,      // Project
                0,      // Select
                0,      // Sort
                2       // UnionAll
            });
        
        List<?>[] expected = new List[] {
            Arrays.asList(new Object[] {"a"}), //$NON-NLS-1$
            Arrays.asList(new Object[] {"b"}), //$NON-NLS-1$
            };

        FakeDataManager manager = new FakeDataManager();
        TestProcessor.sampleData1(manager);
        TestProcessor.helpProcess(plan, manager, expected);
    }
    
    @Test public void testUnionArrayNull() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create view v (col string[]) as select null union all select null", "x", "y");
        
        ProcessorPlan plan = TestOptimizer.helpPlan("select * from v", metadata, new String[] {}); 
        
        List<?>[] expected = new List[] {
            Collections.singletonList(null), Collections.singletonList(null),
            };

        FakeDataManager manager = new FakeDataManager();
        TestProcessor.helpProcess(plan, manager, expected);
    }
    
    @Test public void testNestedUnionPlan() throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign table t1 (col string); "
                + "create foreign table t2 (col string);"
                + "create foreign table t3 (col string);"
                + "create foreign table t4 (col string);", "x", "y");
        
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.QUERY_INTERSECT, true);
        bsc.setCapabilitySupport(Capability.QUERY_UNION, true);
        
        ProcessorPlan plan = TestOptimizer.helpPlan(
                "(select * from t1 union all select * from t2 union all select * from t3) intersect select * from t4", metadata, 
                new String[] {"(SELECT g_3.col AS c_0 FROM y.t1 AS g_3 UNION ALL SELECT g_2.col AS c_0 FROM y.t2 AS g_2 UNION ALL SELECT g_1.col AS c_0 FROM y.t3 AS g_1) INTERSECT SELECT g_0.col AS c_0 FROM y.t4 AS g_0"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING); 
        
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

}
