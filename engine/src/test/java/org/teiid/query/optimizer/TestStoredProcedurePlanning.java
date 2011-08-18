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

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.query.metadata.TempMetadataAdapter;
import org.teiid.query.metadata.TempMetadataStore;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.unittest.RealMetadataFactory;


public class TestStoredProcedurePlanning {
    
    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 1a
     */
	@Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery1() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq1()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1, e2 FROM pm1.g1" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN); 
    }
    
    /**
     * Test planning stored queries
     */
	@Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery2() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq1()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1, e2 FROM pm1.g1" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN); 
    }
    
    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 1b
     */
	@Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery3() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq2('1')", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1, e2 FROM pm1.g1 WHERE e1 = '1'" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN); 
    }
	@Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery4() {
        ProcessorPlan plan = TestOptimizer.helpPlan("select x.e1 from (EXEC pm1.sq1()) as x", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN); 
    }

    @Test public void testStoredQuery5() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sp1()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "EXEC pm1.sp1()" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, new int[] {
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
    
    @Test public void testStoredQuery6() {
        ProcessorPlan plan = TestOptimizer.helpPlan("select x.e1 from (EXEC pm1.sp1()) as x", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "EXEC pm1.sp1()" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, new int[] {
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
            2,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }
    
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery7() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sqsp1()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "EXEC pm1.sp1()" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, new int[] {
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
            2,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }
    
    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 1c
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery8() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq3('1', 1)", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1, e2 FROM pm1.g1 WHERE e1 = '1'", "SELECT e1, e2 FROM pm1.g1 WHERE e2 = 1" }); //$NON-NLS-1$ //$NON-NLS-2$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        }); 
    }
    
    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 5a
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery9() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq4()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] {"SELECT e1, e2 FROM pm1.g1" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN); 
    }
    
    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 5b
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery10() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq5('1')", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1, e2 FROM pm1.g1 WHERE e1 = '1'"}); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN); 
    }
    
     /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 5c
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery11() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq6()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] {"SELECT e1, e2 FROM pm1.g1 WHERE e1 = '1'" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN); 
    }
    
     /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 6a
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery12() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq7()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN); 
    }
    
    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 6c
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery13() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq8('1')", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1 WHERE e1 = '1'" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN); 
    }
    
    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 6b
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery14() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq9('1')", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1 WHERE e1 = '1'" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN); 
    }
    
    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 6d
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery15() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq10('1', 2)", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT e1 FROM pm1.g1 WHERE (e1 = '1') AND (e2 = 2)" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN); 
    }
    
    /**
     * Test planning stored queries. 
     */
    @Test public void testStoredQuery16() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sp2(1)", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "EXEC pm1.sp2(1)" }); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, new int[] {
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
    
    /**
     * Test planning stored queries. GeminiStoredQueryTestPlan - 6d
     */
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery17() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq11(1, 2)", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "EXEC pm1.sp2(?)" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
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
            2,      // Project
            1,      // Select
            0,      // Sort
            0       // UnionAll
        }); 
    }
    
    //GeminiStoredQueryTestPlan - 2a, 2b
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery18() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq12('1', 1)", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "INSERT INTO pm1.g1 (e1, e2) VALUES ('1', 1)" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN); 
    }
    
    //GeminiStoredQueryTestPlan - 2c
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery19() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq13('1')", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "INSERT INTO pm1.g1 (e1, e2) VALUES ('1', 2)" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN); 
    }
    
    //GeminiStoredQueryTestPlan - 3c
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery20() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq14('1', 2)", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "UPDATE pm1.g1 SET e1 = '1' WHERE e2 = 2" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN); 
    }
    
    //GeminiStoredQueryTestPlan - 4b
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery21() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq15('1', 2)", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "DELETE FROM pm1.g1 WHERE (e1 = '1') AND (e2 = 2)" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);               
    }
    
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery22() {
        ProcessorPlan plan = TestOptimizer.helpPlan("select e1 from (EXEC pm1.sq1()) as x where e1='a' union (select e1 from vm1.g2 where e1='b')", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE g_0.e1 = 'a'", "SELECT g_0.e1 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_0.e1 = 'b') AND (g_1.e1 = 'b')" }); //$NON-NLS-1$ //$NON-NLS-2$

        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            1,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            0,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        });               
    }
    
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery23() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq16()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "INSERT INTO pm1.g1 (e1, e2) VALUES ('1', 2)" }); //$NON-NLS-1$
            
        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);               
    }
    
    @Test public void testStoredQuery24() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sp3()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "EXEC pm1.sp3()" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, new int[] {
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

    // test implicit type conversion of argument
    @Ignore("stored procedure wrapper removal logic has been removed")
    @Test public void testStoredQuery25() {
        ProcessorPlan plan = TestOptimizer.helpPlan("EXEC pm1.sq15(1, 2)", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "DELETE FROM pm1.g1 WHERE (e1 = '1') AND (e2 = 2)" }); //$NON-NLS-1$

        TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN);
    }

    @Test public void testStoredQueryXML1() {
        TestOptimizer.helpPlan("EXEC pm1.sq18()", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), new String[] { }); //$NON-NLS-1$
    }
    
    /**
     * union of two stored procs - case #1466
     */
    @Test public void testStoredProc1() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT * FROM (EXEC pm1.sp2(1)) AS x UNION ALL SELECT * FROM (EXEC pm1.sp2(2)) AS y", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "EXEC pm1.sp2(1)", "EXEC pm1.sp2(2)" }); //$NON-NLS-1$ //$NON-NLS-2$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            4,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        }); 
    }

    /**
     * union of stored proc and query - case #1466
     */
    @Test public void testStoredProc2() {
        ProcessorPlan plan = TestOptimizer.helpPlan("SELECT * FROM (EXEC pm1.sp2(1)) AS x UNION ALL SELECT e1, e2 FROM pm1.g1", new TempMetadataAdapter(RealMetadataFactory.example1Cached(), new TempMetadataStore()), //$NON-NLS-1$
            new String[] { "EXEC pm1.sp2(1)", "SELECT e1, e2 FROM pm1.g1" }); //$NON-NLS-1$ //$NON-NLS-2$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            0,      // Grouping
            0,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            2,      // Project
            0,      // Select
            0,      // Sort
            1       // UnionAll
        }); 
    }    

}
