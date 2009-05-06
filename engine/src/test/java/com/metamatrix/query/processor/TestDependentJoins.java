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

import java.util.Arrays;
import java.util.List;

import com.metamatrix.query.optimizer.TestOptimizer;
import com.metamatrix.query.optimizer.capabilities.BasicSourceCapabilities;
import com.metamatrix.query.optimizer.capabilities.FakeCapabilitiesFinder;
import com.metamatrix.query.optimizer.capabilities.SourceCapabilities.Capability;
import com.metamatrix.query.processor.relational.JoinNode;
import com.metamatrix.query.processor.relational.RelationalNode;
import com.metamatrix.query.processor.relational.RelationalPlan;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.unittest.FakeMetadataFacade;
import com.metamatrix.query.unittest.FakeMetadataFactory;
import com.metamatrix.query.unittest.FakeMetadataObject;

import junit.framework.TestCase;


public class TestDependentJoins extends TestCase {
    
    /** 
     * @param sql
     * @return
     */
    static ProcessorPlan helpGetPlan(String sql) {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(2));
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false); //fake data manager doesn't support order by
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestProcessor.helpParse(sql),
                                                       FakeMetadataFactory.example1Cached(),
                                                       capFinder);
        return plan;
    }
    
    /** SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm1.g1.e1=pm2.g1.e1 AND pm1.g1.e2=pm2.g1.e2 */
    public void testMultiCritDepJoin1() { 
       // Create query 
       String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm1.g1.e1=pm2.g1.e1 AND pm1.g1.e2=pm2.g1.e2 order by pm1.g1.e1 option makedep pm1.g1"; //$NON-NLS-1$
       
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "c" }) //$NON-NLS-1$
       };    
       
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       TestProcessor.sampleData1(dataManager);
       
       // Plan query
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, FakeMetadataFactory.example1Cached());

       // Run query
       TestProcessor.helpProcess(plan, dataManager, expected);
   }

    /** SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm2.g1.e1=pm1.g1.e1 AND pm1.g1.e2=pm2.g1.e2 */
    public void testMultiCritDepJoin2() { 
       // Create query 
       String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm2.g1.e1=pm1.g1.e1 AND pm1.g1.e2=pm2.g1.e2 order by pm1.g1.e1 option makedep pm1.g1"; //$NON-NLS-1$
       
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "c" }) //$NON-NLS-1$
       };    
       
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       TestProcessor.sampleData1(dataManager);
       
       ProcessorPlan plan = helpGetPlan(sql);

       // Run query
       TestProcessor.helpProcess(plan, dataManager, expected);
   }

    /** SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm2.g1.e1=pm1.g1.e1 AND pm1.g1.e2=pm2.g1.e2 */
    public void testMultiCritDepJoin3() { 
       // Create query 
       String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm2.g1.e1=pm1.g1.e1 AND pm1.g1.e2=pm2.g1.e2 order by pm1.g1.e1 option makedep pm1.g1"; //$NON-NLS-1$
       
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "c" }) //$NON-NLS-1$
       };    
       
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       TestProcessor.sampleData1(dataManager);
       
       // Plan query
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, FakeMetadataFactory.example1Cached());

       // Run query
       TestProcessor.helpProcess(plan, dataManager, expected);
   }

    /** SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm2.g1.e1=pm1.g1.e1 AND pm1.g1.e2=pm2.g1.e2 */
    public void testMultiCritDepJoin4() { 
       // Create query 
       String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm2.g1.e1=pm1.g1.e1 AND pm1.g1.e2=pm2.g1.e2 order by pm1.g1.e1 option makedep pm1.g1"; //$NON-NLS-1$
       
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "c" }) //$NON-NLS-1$
       };    
       
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       TestProcessor.sampleData1(dataManager);
       
       // Plan query
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, FakeMetadataFactory.example1Cached());

       // Run query
       TestProcessor.helpProcess(plan, dataManager, expected);
   }

    /** SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm2.g1.e1=pm1.g1.e1 AND concat(pm1.g1.e1, 'a') = concat(pm2.g1.e1, 'a') AND pm1.g1.e2=pm2.g1.e2 */
    public void testMultiCritDepJoin5() { 
       // Create query 
       String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE concat(pm1.g1.e1, 'a') = concat(pm2.g1.e1, 'a') AND pm1.g1.e2=pm2.g1.e2 order by pm1.g1.e1 option makedep pm1.g1"; //$NON-NLS-1$
       
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "c" }) //$NON-NLS-1$
       };    
       
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       TestProcessor.sampleData1(dataManager);
       
       // Plan query
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, FakeMetadataFactory.example1Cached());

       // Run query
       TestProcessor.helpProcess(plan, dataManager, expected);
   }

    public void testMultiCritDepJoin5a() { 
        // Create query 
        String sql = "SELECT X.e1 FROM pm1.g1 as X, pm2.g1 WHERE concat(X.e1, 'a') = concat(pm2.g1.e1, 'a') AND X.e2=pm2.g1.e2 order by x.e1"; //$NON-NLS-1$
       
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }) //$NON-NLS-1$
        };    
       
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
       
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, FakeMetadataFactory.example1Cached());

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
   }

   public void testMultiCritDepJoin5b() { 
       //Create query 
       String sql = "SELECT X.e1, X.e2 FROM pm1.g1 as X, pm2.g1 WHERE concat(X.e1, convert(X.e4, string)) = concat(pm2.g1.e1, convert(pm2.g1.e4, string)) AND X.e2=pm2.g1.e2 order by x.e1 option makedep x"; //$NON-NLS-1$
       
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { "a", new Integer(0) }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a", new Integer(0) }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a", new Integer(0) }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a", new Integer(0) }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a", new Integer(3) }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "b", new Integer(2) }), //$NON-NLS-1$
       };    
       
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       TestProcessor.sampleData1(dataManager);
       
       // Plan query
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, FakeMetadataFactory.example1Cached());

       // Run query
       TestProcessor.helpProcess(plan, dataManager, expected);
   }

    /** SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm1.g1.e1 = concat(pm2.g1.e1, '') AND pm1.g1.e2=pm2.g1.e2 */
    public void testMultiCritDepJoin6() { 
       // Create query 
       String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm1.g1.e1 = concat(pm2.g1.e1, '') AND pm1.g1.e2=pm2.g1.e2 order by pm1.g1.e1 option makedep pm1.g1"; //$NON-NLS-1$
       
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "c" }) //$NON-NLS-1$
       };    
       
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       TestProcessor.sampleData1(dataManager);
       
       // Plan query
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, FakeMetadataFactory.example1Cached());

       // Run query
       TestProcessor.helpProcess(plan, dataManager, expected);
   }

    /** SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE concat(pm1.g1.e1, '') = pm2.g1.e1 AND pm1.g1.e2=pm2.g1.e2 */
    public void testMultiCritDepJoin7() { 
       // Create query 
       String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE concat(pm1.g1.e1, '') = pm2.g1.e1 AND pm1.g1.e2=pm2.g1.e2 order by pm1.g1.e1 option makedep pm1.g1"; //$NON-NLS-1$
       
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "c" }) //$NON-NLS-1$
       };    
       
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       TestProcessor.sampleData1(dataManager);
       
       // Plan query
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, FakeMetadataFactory.example1Cached());

       // Run query
       TestProcessor.helpProcess(plan, dataManager, expected);
   }

    /** SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm1.g1.e1 = pm2.g1.e1 AND pm1.g1.e2 <> pm2.g1.e2 */
    public void testMultiCritDepJoin8() { 
       // Create query 
       String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm1.g1.e1 = pm2.g1.e1 AND pm1.g1.e2 <> pm2.g1.e2 option makedep pm1.g1"; //$NON-NLS-1$
       
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
       };    
       
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       TestProcessor.sampleData1(dataManager);
       
       // Plan query
       ProcessorPlan plan = helpGetPlan(sql);

       // Run query
       TestProcessor.helpProcess(plan, dataManager, expected);
   }

    /** SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm1.g1.e2 <> pm2.g1.e2 */
    public void testMultiCritDepJoin9() { 
       // Create query 
       String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm1.g1.e2 <> pm2.g1.e2 option makedep pm1.g1"; //$NON-NLS-1$
       
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { null }),
           Arrays.asList(new Object[] { null }),
           Arrays.asList(new Object[] { null }),
           Arrays.asList(new Object[] { null }),
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
       };    
       
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       TestProcessor.sampleData1(dataManager);
       
       // Plan query
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, FakeMetadataFactory.example1Cached());

       // Run query
       TestProcessor.helpProcess(plan, dataManager, expected);
   }     

    /** SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm1.g1.e3=pm2.g1.e3 AND pm1.g1.e2=pm2.g1.e2 AND pm2.g1.e1 = 'a' */
    public void testMultiCritDepJoin10() { 
       // Create query 
       String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm1.g1.e3=pm2.g1.e3 AND pm1.g1.e2=pm2.g1.e2 AND pm2.g1.e1 = 'a' option makedep pm1.g1"; //$NON-NLS-1$
       
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a" }) //$NON-NLS-1$
       };    
       
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       TestProcessor.sampleData1(dataManager);
       
       // Plan query
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, FakeMetadataFactory.example1Cached());

       // Run query
       TestProcessor.helpProcess(plan, dataManager, expected);
   }       

    public void testLargeSetInDepJoinWAccessPatternCausingSortNodeInsertCanHandleAlias() {
        helpTestDepAccessCausingSortNodeInsert(true);
    }
    
    public void testLargeSetInDepJoinWAccessPatternCausingSortNodeInsertCannotHandleAlias() {
        helpTestDepAccessCausingSortNodeInsert(false);
    }
    
    public void helpTestDepAccessCausingSortNodeInsert(boolean accessNodeHandlesAliases) {
        String sql = "SELECT a.e1, b.e1, b.e2 FROM pm4.g1 a INNER JOIN pm1.g1 b ON a.e2=b.e2 AND a.e1 = b.e1 OPTION MAKEDEP a"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] { 
              Arrays.asList(new Object[] { "aa ", "aa ", new Integer(0)}), //$NON-NLS-1$ //$NON-NLS-2$
             Arrays.asList(new Object[] { "bb   ", "bb   ", new Integer(1)}), //$NON-NLS-1$ //$NON-NLS-2$
             Arrays.asList(new Object[] { "cc  ", "cc  ", new Integer(2)}) //$NON-NLS-1$ //$NON-NLS-2$
        };    

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData2b(dataManager);

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities depcaps = new BasicSourceCapabilities();
        depcaps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        depcaps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1));
        depcaps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        if(accessNodeHandlesAliases) {
            depcaps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        }
        
        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);
        
        capFinder.addCapabilities("pm4", depcaps); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Slightly modify metadata to set max set size to just a few rows - this
        // will allow us to test the dependent overflow case
        FakeMetadataFacade fakeMetadata = FakeMetadataFactory.example1Cached();
        
        Command command = TestProcessor.helpParse(sql);   
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, fakeMetadata, capFinder);
        
        //Verify a dependent join (not merge join) was used
        assertTrue(plan instanceof RelationalPlan);
        RelationalPlan relationalPlan = (RelationalPlan)plan;
        RelationalNode project = relationalPlan.getRootNode();
        RelationalNode join = project.getChildren()[0];
        assertTrue("Expected instance of JoinNode (for dep join) but got " + join.getClass(), join instanceof JoinNode); //$NON-NLS-1$

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);          
    }
    
    public void testCase5130() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$

        String sql = "select a.intkey from bqt1.smalla a, bqt1.smallb b where concat(a.stringkey, 't') = b.stringkey option makedep a"; //$NON-NLS-1$ 
         
        // Plan query 
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, FakeMetadataFactory.exampleBQTCached(), null, capFinder, new String[] {"SELECT a.stringkey, a.intkey FROM bqt1.smalla AS a", "SELECT b.stringkey FROM bqt1.smallb AS b"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$
 
        TestOptimizer.checkNodeTypes(plan, new int[] { 
            2,      // Access 
            0,      // DependentAccess 
            0,      // DependentSelect 
            0,      // DependentProject 
            0,      // DupRemove 
            0,      // Grouping 
            0,      // Join 
            1,      // MergeJoin 
            0,      // Null 
            0,      // PlanExecution 
            2,      // Project 
            1,      // Select 
            0,      // Sort 
            0       // UnionAll 
        });
        
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.stringkey FROM bqt1.smallb AS g_0",  //$NON-NLS-1$ 
                            new List[] { Arrays.asList(new Object[] { "1t" }), //$NON-NLS-1$
                                         Arrays.asList(new Object[] { "2" })}); //$NON-NLS-1$
        dataManager.addData("SELECT g_0.stringkey, g_0.intkey FROM bqt1.smalla AS g_0",  //$NON-NLS-1$ 
                            new List[] { Arrays.asList(new Object[] { "1", new Integer(1) })}); //$NON-NLS-1$
        
        
        List[] expected = new List[] {   
            Arrays.asList(new Object[] { new Integer(1) }), 
        };
        
        TestProcessor.helpProcess(plan, dataManager, expected);
        
        assertFalse(dataManager.getCommandHistory().contains("SELECT a.stringkey, a.intkey FROM bqt1.smalla AS a WHERE concat(a.stringkey, 't') IN ('1', '2')")); //$NON-NLS-1$
    }
    
    public void testCase5130a() throws Exception {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        capFinder.addCapabilities("BQT1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("BQT2", caps); //$NON-NLS-1$
        
        String sql = "select a.intkey from bqt1.smalla a, bqt2.smallb b where concat(a.stringkey, 't') = b.stringkey and a.intkey = b.intkey option makedep a"; //$NON-NLS-1$ 
         
        // Plan query 
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, FakeMetadataFactory.exampleBQTCached(), null, capFinder, 
                                                    new String[] {"SELECT g_0.stringkey, g_0.intkey FROM bqt1.smalla AS g_0 WHERE g_0.intkey IN (<dependent values>)", "SELECT g_0.stringkey, g_0.intkey FROM bqt2.smallb AS g_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
 
        TestOptimizer.checkNodeTypes(plan, new int[] { 
            1,      // Access 
            1,      // DependentAccess 
            0,      // DependentSelect 
            0,      // DependentProject 
            0,      // DupRemove 
            0,      // Grouping 
            0,      // Join 
            1,      // MergeJoin 
            0,      // Null 
            0,      // PlanExecution 
            2,      // Project 
            1,      // Select 
            0,      // Sort 
            0       // UnionAll 
        });
        
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT g_0.stringkey, g_0.intkey FROM bqt2.smallb AS g_0",  //$NON-NLS-1$ 
                            new List[] { Arrays.asList(new Object[] { "1t", new Integer(1) }), //$NON-NLS-1$
                                         Arrays.asList(new Object[] { "2t", new Integer(2) })}); //$NON-NLS-1$
        dataManager.addData("SELECT g_0.stringkey, g_0.intkey FROM bqt1.smalla AS g_0 WHERE g_0.intkey IN (1, 2)",  //$NON-NLS-1$ 
                            new List[] { Arrays.asList(new Object[] { "1", new Integer(1) })}); //$NON-NLS-1$
        
        
        List[] expected = new List[] {   
            Arrays.asList(new Object[] { new Integer(1) }), 
        };
        
        TestProcessor.helpProcess(plan, dataManager, expected);
        
        assertFalse(dataManager.getCommandHistory().contains("SELECT a.stringkey, a.intkey FROM bqt2.smalla AS a WHERE (concat(a.stringkey, 't') IN ('1t', '2')) AND (a.intkey IN (1))")); //$NON-NLS-1$
    }
    
    static void sampleData4(FakeDataManager dataMgr) throws Exception {
        FakeMetadataFacade metadata = FakeMetadataFactory.example1Cached();
    
        // Group pm1.g1
        FakeMetadataObject groupID = (FakeMetadataObject) metadata.getGroupID("pm1.g1"); //$NON-NLS-1$
        List elementIDs = metadata.getElementIDsInGroupID(groupID);
        List elementSymbols = FakeDataStore.createElements(elementIDs);
    
        dataMgr.registerTuples(
            groupID,
            elementSymbols,
            
            new List[] { 
                Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
                Arrays.asList(new Object[] { "b",   new Integer(1),     Boolean.TRUE,   null }), //$NON-NLS-1$
                Arrays.asList(new Object[] { "c",   new Integer(2),     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
                } );       
            
        // Group pm6.g1
        groupID = (FakeMetadataObject) metadata.getGroupID("pm6.g1"); //$NON-NLS-1$
        elementIDs = metadata.getElementIDsInGroupID(groupID);
        elementSymbols = FakeDataStore.createElements(elementIDs);
    
        dataMgr.registerTuples(
            groupID,
            elementSymbols,
            
            new List[] { 
                Arrays.asList(new Object[] { "b",   new Integer(0) }), //$NON-NLS-1$
                Arrays.asList(new Object[] { "d",   new Integer(3) }), //$NON-NLS-1$
                Arrays.asList(new Object[] { "e",   new Integer(1) }), //$NON-NLS-1$
                } );      
    }

    /** SELECT pm1.g1.e1 FROM pm1.g1, pm6.g1 WHERE pm1.g1.e1=pm6.g1.e1 OPTION MAKEDEP pm6.g1 */
    public void testLargeSetInDepAccess() throws Exception {
        // Create query 
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm6.g1 WHERE pm1.g1.e1=pm6.g1.e1 OPTION MAKEDEP pm6.g1"; //$NON-NLS-1$

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData4(dataManager);

        // Slightly modify metadata to set max set size to just a few rows - this
        // will allow us to test the dependent overflow case
        FakeMetadataFacade fakeMetadata = FakeMetadataFactory.example1Cached();

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities depcaps = new BasicSourceCapabilities();
        depcaps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        depcaps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1));
        depcaps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);

        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);

        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm6", depcaps); //$NON-NLS-1$

        List[] expected = new List[] {
            Arrays.asList(new Object[] {
                new String("b")})}; //$NON-NLS-1$

        Command command = TestProcessor.helpParse(sql);
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, fakeMetadata, capFinder);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

    public void testLargeSetInDepAccessMultiJoinCriteria() {
        //     Create query 
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm1.g1.e1=pm2.g1.e1 AND pm1.g1.e2=pm2.g1.e2 order by e1 OPTION MAKEDEP pm2.g1"; //$NON-NLS-1$
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);

        // Slightly modify metadata to set max set size to just a few rows - this
        // will allow us to test the dependent overflow case
        FakeMetadataFacade fakeMetadata = FakeMetadataFactory.example1();

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities depcaps = new BasicSourceCapabilities();
        depcaps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        depcaps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1));

        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);

        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", depcaps); //$NON-NLS-1$

        List[] expected = new List[] {
            Arrays.asList(new Object[] {
                new String("a")}), //$NON-NLS-1$
            Arrays.asList(new Object[] {
                new String("a")}), //$NON-NLS-1$
            Arrays.asList(new Object[] {
                new String("a")}), //$NON-NLS-1$
            Arrays.asList(new Object[] {
                new String("a")}), //$NON-NLS-1$
            Arrays.asList(new Object[] {
                new String("a")}), //$NON-NLS-1$
            Arrays.asList(new Object[] {
                new String("b")}), //$NON-NLS-1$
            Arrays.asList(new Object[] {
                new String("c")})}; //$NON-NLS-1$

        Command command = TestProcessor.helpParse(sql);
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, fakeMetadata, capFinder);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);

    }

    public void testLargeSetInDepAccessWithAccessPattern() {
        String sql = "SELECT a.e1, b.e1, b.e2 FROM pm4.g1 a INNER JOIN pm1.g1 b ON a.e1=b.e1 AND a.e2 = b.e2"; //$NON-NLS-1$

        // Create expected results
        List[] expected = new List[] {
            Arrays.asList(new Object[] {
                "aa ", "aa ", new Integer(0)}), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] {
                "bb   ", "bb   ", new Integer(1)}), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] {
                "cc  ", "cc  ", new Integer(2)}) //$NON-NLS-1$ //$NON-NLS-2$
        };

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData2b(dataManager);

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities depcaps = new BasicSourceCapabilities();
        depcaps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        depcaps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1));
        depcaps.setCapabilitySupport(Capability.CRITERIA_IN, true);

        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);

        capFinder.addCapabilities("pm4", depcaps); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        // Slightly modify metadata to set max set size to just a few rows - this
        // will allow us to test the dependent overflow case
        FakeMetadataFacade fakeMetadata = FakeMetadataFactory.example1Cached();

        Command command = TestProcessor.helpParse(sql);
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, fakeMetadata, capFinder);

        //Verify a dependent join (not merge join) was used
        assertTrue(plan instanceof RelationalPlan);
        RelationalPlan relationalPlan = (RelationalPlan)plan;
        RelationalNode project = relationalPlan.getRootNode();
        RelationalNode join = project.getChildren()[0];
        assertTrue("Expected instance of JoinNode (for dep join) but got " + join.getClass(), join instanceof JoinNode); //$NON-NLS-1$

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }
    
    /** SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2 WHERE pm1.g1.e1 = pm1.g2.e1 AND pm1.g1.e2 = -100 OPTION MAKEDEP pm1.g2 */
    public void testDependentNoRows() { 
       // Create query 
       String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2 WHERE pm1.g1.e1 = pm1.g2.e1 AND pm1.g1.e2 = -100 OPTION MAKEDEP pm1.g2"; //$NON-NLS-1$
        
       // Create expected results
       List[] expected = new List[] { 
       };    
        
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       TestProcessor.sampleData1(dataManager);
        
       // Plan query
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, FakeMetadataFactory.example1Cached());

       // Run query
       TestProcessor.helpProcess(plan, dataManager, expected);
    }

    /** SELECT pm1.g1.e2, pm2.g1.e2 FROM pm1.g1, pm2.g1 WHERE (pm1.g1.e2+1)=pm2.g1.e2 OPTION MAKEDEP pm1.g2 */
    public void testExpressionInDepJoin() { 
       // Create query 
       String sql = "SELECT pm1.g1.e2, pm2.g1.e2 FROM pm1.g1, pm2.g1 WHERE (pm1.g1.e2+1)=pm2.g1.e2 OPTION MAKEDEP pm2.g1"; //$NON-NLS-1$
       
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { new Integer(0), new Integer(1) }),
           Arrays.asList(new Object[] { new Integer(0), new Integer(1) }),
           Arrays.asList(new Object[] { new Integer(0), new Integer(1) }),
           Arrays.asList(new Object[] { new Integer(0), new Integer(1) }),
           Arrays.asList(new Object[] { new Integer(1), new Integer(2) }),
           Arrays.asList(new Object[] { new Integer(1), new Integer(2) }),
           Arrays.asList(new Object[] { new Integer(2), new Integer(3) })
       };    
       
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       TestProcessor.sampleData1(dataManager);
       
       // Plan query

       FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
       BasicSourceCapabilities caps = new BasicSourceCapabilities();
       caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
       caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, new Integer(1000));
       capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
       capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
       
       Command command = TestProcessor.helpParse(sql);   
       ProcessorPlan plan = TestProcessor.helpGetPlan(command, FakeMetadataFactory.example1Cached(), capFinder);

       // Run query
       TestProcessor.helpProcess(plan, dataManager, expected);
   }    
    
}
