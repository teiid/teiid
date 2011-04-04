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

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Test;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.relational.JoinNode;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.FakeMetadataFacade;
import org.teiid.query.unittest.FakeMetadataFactory;
import org.teiid.query.unittest.FakeMetadataObject;
import org.teiid.query.util.CommandContext;

@SuppressWarnings({"unchecked", "nls"})
public class TestDependentJoins {
    
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
    @Test public void testMultiCritDepJoin1() { 
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
    @Test public void testMultiCritDepJoin2() { 
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
    @Test public void testMultiCritDepJoin3() { 
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
    @Test public void testMultiCritDepJoin4() { 
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
    @Test public void testMultiCritDepJoin5() { 
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

    @Test public void testMultiCritDepJoin5a() { 
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

   @Test public void testMultiCritDepJoin5b() { 
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
    @Test public void testMultiCritDepJoin6() { 
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
    @Test public void testMultiCritDepJoin7() { 
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
    @Test public void testMultiCritDepJoin8() { 
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
    @Test public void testMultiCritDepJoin9() { 
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
    @Test public void testMultiCritDepJoin10() { 
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

    @Test public void testLargeSetInDepJoinWAccessPatternCausingSortNodeInsertCanHandleAlias() {
        helpTestDepAccessCausingSortNodeInsert(true);
    }
    
    @Test public void testLargeSetInDepJoinWAccessPatternCausingSortNodeInsertCannotHandleAlias() {
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
    
    @Test public void testCase5130() {
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
    
    @Test public void testCase5130a() throws Exception {
        HardcodedDataManager dataManager = helpTestDependentJoin(false);
        
        assertFalse(dataManager.getCommandHistory().contains("SELECT a.stringkey, a.intkey FROM bqt2.smalla AS a WHERE (concat(a.stringkey, 't') IN ('1t', '2')) AND (a.intkey IN (1))")); //$NON-NLS-1$
    }
    
    @Test public void testUnlimitedIn() throws Exception {
    	helpTestDependentJoin(true);
    }

	private HardcodedDataManager helpTestDependentJoin(boolean unlimitIn)
			throws TeiidComponentException, TeiidProcessingException {
		FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);
        if (unlimitIn) {
        	caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, -1);
        }
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
		return dataManager;
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
    @Test public void testLargeSetInDepAccess() throws Exception {
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

    @Test public void testLargeSetInDepAccessMultiJoinCriteria() throws Exception {
    	helpTestLargeSetInDepAccessMultiJoinCriteria(1, -1, 1, 2);
    }
    
    @Test public void testLargeSetInDepAccessMultiJoinCriteriaConcurrent() throws Exception {
    	//allows concurrent
    	helpTestLargeSetInDepAccessMultiJoinCriteria(1, -1, 4, 4);
    }
    
    @Test public void testLargeSetInDepAccessMultiJoinCriteriaCompound() throws Exception {
    	//max predicates forces multiple queries
    	helpTestLargeSetInDepAccessMultiJoinCriteria(1, 4, 3, 3);
    }
    
    @Test public void testLargeSetInDepAccessMultiJoinCriteriaCompoundAll() throws Exception {
    	//max predicates allows a one shot
    	helpTestLargeSetInDepAccessMultiJoinCriteria(1, 10, 2, 2);
    }
    
    /**
     * concurrentOpen will be minimum of 2 to gather the pm1 results.
     */
    public void helpTestLargeSetInDepAccessMultiJoinCriteria(int maxInSize, int maxPredicates, int maxConcurrency, int concurrentOpen) throws Exception {
        //     Create query 
        String sql = "SELECT pm1.g1.e1 FROM (pm1.g2 cross join pm1.g1) inner join pm2.g1 makedep ON pm1.g1.e1=pm2.g1.e1 AND pm1.g1.e2=pm2.g1.e2 AND pm1.g2.e4 = pm2.g1.e4 order by e1"; //$NON-NLS-1$
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);

        // Slightly modify metadata to set max set size to just a few rows - this
        // will allow us to test the dependent overflow case
        FakeMetadataFacade fakeMetadata = FakeMetadataFactory.example1Cached();

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities depcaps = new BasicSourceCapabilities();
        depcaps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        depcaps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, maxInSize);
        depcaps.setSourceProperty(Capability.MAX_DEPENDENT_PREDICATES, maxPredicates);

        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);

        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", depcaps); //$NON-NLS-1$

        List[] expected = new List[] {
            Arrays.asList("a"), //$NON-NLS-1$
            Arrays.asList("a"), //$NON-NLS-1$
            Arrays.asList("a"), //$NON-NLS-1$
            Arrays.asList("a"), //$NON-NLS-1$
            Arrays.asList("a"), //$NON-NLS-1$
            Arrays.asList("a"), //$NON-NLS-1$
            Arrays.asList("a"), //$NON-NLS-1$
            Arrays.asList("a"), //$NON-NLS-1$
            Arrays.asList("a"), //$NON-NLS-1$
            Arrays.asList("b"), //$NON-NLS-1$
        	}; 

        Command command = TestProcessor.helpParse(sql);
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, fakeMetadata, capFinder);
        TestOptimizer.checkAtomicQueries(new String[] {
        		"SELECT pm1.g2.e4 FROM pm1.g2", 
        		"SELECT pm2.g1.e1, pm2.g1.e2, pm2.g1.e4 FROM pm2.g1 WHERE (pm2.g1.e1 IN (<dependent values>)) AND (pm2.g1.e2 IN (<dependent values>)) AND (pm2.g1.e4 IN (<dependent values>))", 
        		"SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1"
        }, plan);
        CommandContext cc = TestProcessor.createCommandContext();
        cc.setUserRequestSourceConcurrency(maxConcurrency);
        FakeTupleSource.resetStats();
        // Run query
        TestProcessor.helpProcess(plan, cc, dataManager, expected);

        assertEquals("Wrong number of concurrent source queries", concurrentOpen, FakeTupleSource.maxOpen);
    }

    @Test public void testLargeSetInDepAccessWithAccessPattern() {
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
    @Test public void testDependentNoRows() { 
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
    @Test public void testExpressionInDepJoin() { 
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
    
    @Test public void testDependentJoinBackoff() throws Exception {
        // Create query 
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm6.g1 WHERE pm1.g1.e1=pm6.g1.e1"; //$NON-NLS-1$

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData4(dataManager);

        FakeMetadataFacade fakeMetadata = FakeMetadataFactory.example1();

        FakeMetadataFactory.setCardinality("pm1.g1", 1, fakeMetadata);
        FakeMetadataFactory.setCardinality("pm6.g1", 1000, fakeMetadata);
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

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, fakeMetadata, new String[] {
        		"SELECT pm6.g1.e1 FROM pm6.g1 WHERE pm6.g1.e1 IN (<dependent values>) ORDER BY pm6.g1.e1",
        		"SELECT pm1.g1.e1 FROM pm1.g1"
        }, capFinder, ComparisonMode.EXACT_COMMAND_STRING);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
        
        //note that the dependent join was not actually performed
        assertEquals(new HashSet<String>(Arrays.asList("SELECT pm1.g1.e1 FROM pm1.g1", "SELECT pm6.g1.e1 FROM pm6.g1 ORDER BY pm6.g1.e1")), 
        		new HashSet<String>(dataManager.getQueries()));
    }
    
}
