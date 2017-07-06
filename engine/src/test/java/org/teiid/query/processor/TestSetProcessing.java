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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Test;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
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

}
