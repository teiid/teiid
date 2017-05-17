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
import static org.teiid.query.processor.TestProcessor.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.api.exception.query.QueryMetadataException;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.core.TeiidComponentException;
import org.teiid.core.TeiidProcessingException;
import org.teiid.language.Array;
import org.teiid.language.Comparison;
import org.teiid.language.Literal;
import org.teiid.language.Parameter;
import org.teiid.language.Select;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.processor.relational.JoinNode;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;
import org.teiid.translator.ExecutionFactory.NullOrder;

@SuppressWarnings({"rawtypes", "unchecked", "nls"})
public class TestDependentJoins {
    
    /** 
     * @param sql
     * @return
     */
    static ProcessorPlan helpGetPlan(String sql) {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 2);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false); //fake data manager doesn't support order by
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
        
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(TestProcessor.helpParse(sql),
                                                       RealMetadataFactory.example1Cached(),
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
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

       // Run query
       TestProcessor.helpProcess(plan, dataManager, expected);
   }
    
    @Test public void testMultiCritDepJoinOverMax() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm1.g2, pm2.g1 WHERE pm1.g1.e1 = pm1.g2.e1 and pm1.g1.e2=pm2.g1.e2 and pm1.g2.e1=pm2.g1.e1 option makedep pm1.g1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "d" }), //$NON-NLS-1$
        };    
        
        // Construct data manager with data
        HardcodedDataManager hdm = new HardcodedDataManager(true);
        hdm.addData("SELECT g_0.e2 AS c_0, g_0.e1 AS c_1 FROM pm2.g1 AS g_0 ORDER BY c_0, c_1", Arrays.asList(1, "a"), Arrays.asList(1, "b"), Arrays.asList(1, "c"), Arrays.asList(1, "d"));
        hdm.addData("SELECT g_0.e2 AS c_0, g_1.e1 AS c_1, g_0.e1 AS c_2 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_0.e2 = 1) AND (g_1.e1 IN ('a', 'b'))", Arrays.asList(1, "a", "a"), Arrays.asList(1, "b", "b"));
        hdm.addData("SELECT g_0.e2 AS c_0, g_1.e1 AS c_1, g_0.e1 AS c_2 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_0.e2 = 1) AND (g_1.e1 IN ('c', 'd'))", Arrays.asList(1, "c", "c"), Arrays.asList(1, "d", "d"));
        
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 2);
        caps.setSourceProperty(Capability.MAX_DEPENDENT_PREDICATES, 3);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(caps));

        // Run query
        TestProcessor.helpProcess(plan, hdm, expected);
    }
    
    @Test public void testDependentView() { 
        String sql = "SELECT v.e1 FROM (select distinct e1 from pm1.g1) as v, pm2.g1 WHERE v.e1=pm2.g1.e1 order by v.e1 option makedep v"; //$NON-NLS-1$
        
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }) //$NON-NLS-1$
        };    
        
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

        TestProcessor.helpProcess(plan, dataManager, expected);
    }
    
    /**
     * specific test for TEIID-2094
     */
    @Test public void testDependentView1() { 
        String sql = "SELECT v.e1 FROM (select distinct e1 from pm1.g1) as v, pm2.g1 WHERE v.e1=pm2.g1.e1 order by v.e1 option makedep v"; //$NON-NLS-1$
        
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
        };    
        
        HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
        dataManager.addData("SELECT g_0.e1 AS c_0 FROM g1 AS g_0 ORDER BY c_0", new List<?>[] {Arrays.asList("a")});
        dataManager.addData("SELECT v_0.c_0 FROM (SELECT DISTINCT g_0.e1 AS c_0 FROM g1 AS g_0) AS v_0 WHERE v_0.c_0 = 'a'", new List<?>[] {Arrays.asList("a")});
        
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_INLINE_VIEWS, true);
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(caps));

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
    
    @Test public void testMultiCritDepJoin2a() { 
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
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.ARRAY_TYPE, true);
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
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

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
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

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
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

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
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
   }

   @Test public void testMultiCritDepJoin5b() { 
       //Create query 
       String sql = "SELECT X.e1, X.e2 FROM pm1.g1 as X, pm2.g1 WHERE concat(X.e1, convert(X.e4, string)) = concat(pm2.g1.e1, convert(pm2.g1.e4, string)) AND X.e2=pm2.g1.e2 order by x.e1, x.e2 option makedep x"; //$NON-NLS-1$
       
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { "a", 0 }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a", 0 }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a", 0 }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a", 0 }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "a", 3 }), //$NON-NLS-1$
           Arrays.asList(new Object[] { "b", 2 }), //$NON-NLS-1$
       };    
       
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       TestProcessor.sampleData1(dataManager);
       
       // Plan query
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

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
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

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
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

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
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

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
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

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
              Arrays.asList(new Object[] { "aa ", "aa ", 0}), //$NON-NLS-1$ //$NON-NLS-2$
             Arrays.asList(new Object[] { "bb   ", "bb   ", 1}), //$NON-NLS-1$ //$NON-NLS-2$
             Arrays.asList(new Object[] { "cc  ", "cc  ", 2}) //$NON-NLS-1$ //$NON-NLS-2$
        };    

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities depcaps = new BasicSourceCapabilities();
        depcaps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        depcaps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 1);
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
        QueryMetadataInterface fakeMetadata = RealMetadataFactory.example1Cached();
        
        Command command = TestProcessor.helpParse(sql);   
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, fakeMetadata, capFinder);
        
        //Verify a dependent join (not merge join) was used
        assertTrue(plan instanceof RelationalPlan);
        RelationalPlan relationalPlan = (RelationalPlan)plan;
        RelationalNode project = relationalPlan.getRootNode();
        RelationalNode join = project.getChildren()[0];
        assertTrue("Expected instance of JoinNode (for dep join) but got " + join.getClass(), join instanceof JoinNode); //$NON-NLS-1$

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData2b(dataManager, fakeMetadata);

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
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), null, capFinder, new String[] {"SELECT a.stringkey, a.intkey FROM bqt1.smalla AS a", "SELECT b.stringkey FROM bqt1.smallb AS b"}, TestOptimizer.SHOULD_SUCCEED); //$NON-NLS-1$ //$NON-NLS-2$
 
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
        dataManager.addData("SELECT g_0.StringKey FROM BQT1.SmallB AS g_0",  //$NON-NLS-1$ 
                            new List[] { Arrays.asList(new Object[] { "1t" }), //$NON-NLS-1$
                                         Arrays.asList(new Object[] { "2" })}); //$NON-NLS-1$
        dataManager.addData("SELECT g_0.StringKey, g_0.IntKey FROM BQT1.SmallA AS g_0",  //$NON-NLS-1$ 
                            new List[] { Arrays.asList(new Object[] { "1", 1 })}); //$NON-NLS-1$
        
        
        List[] expected = new List[] {   
            Arrays.asList(new Object[] { 1 }), 
        };
        
        TestProcessor.helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testCase5130a() throws Exception {
        helpTestDependentJoin(false);
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
        ProcessorPlan plan = TestOptimizer.helpPlan(sql, RealMetadataFactory.exampleBQTCached(), null, capFinder, 
                                                    new String[] {"SELECT g_0.StringKey, g_0.IntKey FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey IN (<dependent values>)", "SELECT g_0.StringKey, g_0.IntKey FROM BQT2.SmallB AS g_0"}, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$ //$NON-NLS-2$
 
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
        dataManager.addData("SELECT g_0.StringKey, g_0.IntKey FROM BQT2.SmallB AS g_0",  //$NON-NLS-1$ 
                            new List[] { Arrays.asList(new Object[] { "1t", 1 }), //$NON-NLS-1$
                                         Arrays.asList(new Object[] { "2t", 2 })}); //$NON-NLS-1$
        dataManager.addData("SELECT g_0.StringKey, g_0.IntKey FROM BQT1.SmallA AS g_0 WHERE g_0.IntKey IN (1, 2)",  //$NON-NLS-1$ 
                            new List[] { Arrays.asList(new Object[] { "1", 1 })}); //$NON-NLS-1$
        
        
        List[] expected = new List[] {   
            Arrays.asList(new Object[] { 1 }), 
        };
        
        TestProcessor.helpProcess(plan, dataManager, expected);
		return dataManager;
	}
    
    static void sampleData4(FakeDataManager dataMgr) throws Exception {
        QueryMetadataInterface metadata = RealMetadataFactory.example1Cached();
    
        dataMgr.registerTuples(
        		metadata,
            "pm1.g1", new List[] { 
    				Arrays.asList(new Object[] { "a",   0,     Boolean.FALSE,  new Double(2.0) }), //$NON-NLS-1$
    				Arrays.asList(new Object[] { "q",   null,     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
    				Arrays.asList(new Object[] { "b",   1,     Boolean.TRUE,   null }), //$NON-NLS-1$
				    Arrays.asList(new Object[] { "c",   2,     Boolean.FALSE,  new Double(0.0) }), //$NON-NLS-1$
				    } );       
            
        dataMgr.registerTuples(
        		metadata,
            "pm6.g1", new List[] { 
				    Arrays.asList(new Object[] { "b",   1 }), //$NON-NLS-1$
				    Arrays.asList(new Object[] { "d",   3 }), //$NON-NLS-1$
				    Arrays.asList(new Object[] { "e",   1 }), //$NON-NLS-1$
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
        QueryMetadataInterface fakeMetadata = RealMetadataFactory.example1Cached();

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities depcaps = new BasicSourceCapabilities();
        depcaps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        depcaps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 1);
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
    
    @Test public void testLargeSetInDepAccessMultiJoinCriteriaSetConstraint() throws Exception {
    	helpTestLargeSetInDepAccessMultiJoinCriteria(1, 1, 1, 2);
    }
    
    @Test public void testLargeSetInDepAccessMultiJoinCriteriaConcurrent() throws Exception {
    	//allows concurrent
    	helpTestLargeSetInDepAccessMultiJoinCriteria(1, -1, 4, 5);
    }
    
    @Test public void testLargeSetInDepAccessMultiJoinCriteriaCompound() throws Exception {
    	//max predicates forces multiple queries
    	helpTestLargeSetInDepAccessMultiJoinCriteria(1, 4, 3, 2);
    }
    
    @Test public void testLargeSetInDepAccessMultiJoinCriteriaCompoundAll() throws Exception {
    	//max predicates allows a one shot
    	helpTestLargeSetInDepAccessMultiJoinCriteria(1, 10, 2, 2);
    }
    
    @Test public void testLargeSetMultipleDependentSources() throws Exception {
    	String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 makedep, /*+ makeind */ pm1.g2 where pm1.g1.e1=pm2.g1.e1 AND pm1.g2.e2=pm2.g1.e2 order by e1"; //$NON-NLS-1$
    	
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities depcaps = new BasicSourceCapabilities();
        depcaps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        depcaps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 1);
        depcaps.setSourceProperty(Capability.MAX_DEPENDENT_PREDICATES, 4);

        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);

        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", depcaps); //$NON-NLS-1$

        List[] expected = new List[] {
            Arrays.asList("a"), //$NON-NLS-1$
            Arrays.asList("a"), //$NON-NLS-1$
        	}; 

        Command command = TestProcessor.helpParse(sql);
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);
        TestOptimizer.checkAtomicQueries(new String[] {
        		"SELECT pm1.g2.e2 FROM pm1.g2", 
        		"SELECT pm2.g1.e1, pm2.g1.e2 FROM pm2.g1 WHERE (pm2.g1.e1 IN (<dependent values>)) AND (pm2.g1.e2 IN (<dependent values>))", 
        		"SELECT pm1.g1.e1 FROM pm1.g1"
        }, plan);
        
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1 FROM pm1.g1", new List<?>[] {Arrays.asList("a")});
        dataManager.addData("SELECT pm1.g2.e2 FROM pm1.g2", new List<?>[] {Arrays.asList(1), Arrays.asList(2), Arrays.asList(3)});
        dataManager.addData("SELECT pm2.g1.e1, pm2.g1.e2 FROM pm2.g1 WHERE (pm2.g1.e1 = 'a') AND ((pm2.g1.e2 = 1) OR (pm2.g1.e2 = 2))", new List<?>[] {Arrays.asList("a", 1)});
        dataManager.addData("SELECT pm2.g1.e1, pm2.g1.e2 FROM pm2.g1 WHERE (pm2.g1.e1 = 'a') AND (pm2.g1.e2 = 3)", new List<?>[] {Arrays.asList("a", 3)});
        CommandContext cc = TestProcessor.createCommandContext();
        TestProcessor.helpProcess(plan, cc, dataManager, expected);
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
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);
        TestOptimizer.checkAtomicQueries(new String[] {
        		"SELECT pm1.g2.e4 FROM pm1.g2", 
        		"SELECT pm2.g1.e4, pm2.g1.e1, pm2.g1.e2 FROM pm2.g1 WHERE pm2.g1.e4 IN (<dependent values>)", 
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
                "aa ", "aa ", 0}), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] {
                "bb   ", "bb   ", 1}), //$NON-NLS-1$ //$NON-NLS-2$
            Arrays.asList(new Object[] {
                "cc  ", "cc  ", 2}) //$NON-NLS-1$ //$NON-NLS-2$
        };

        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities depcaps = new BasicSourceCapabilities();
        depcaps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        depcaps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 1);
        depcaps.setCapabilitySupport(Capability.CRITERIA_IN, true);

        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        caps.setCapabilitySupport(Capability.QUERY_FROM_GROUP_ALIAS, true);

        capFinder.addCapabilities("pm4", depcaps); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$

        QueryMetadataInterface fakeMetadata = RealMetadataFactory.example1Cached();

        Command command = TestProcessor.helpParse(sql);
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, fakeMetadata, capFinder);

        //Verify a dependent join (not merge join) was used
        assertTrue(plan instanceof RelationalPlan);
        RelationalPlan relationalPlan = (RelationalPlan)plan;
        RelationalNode project = relationalPlan.getRootNode();
        RelationalNode join = project.getChildren()[0];
        assertTrue("Expected instance of JoinNode (for dep join) but got " + join.getClass(), join instanceof JoinNode); //$NON-NLS-1$

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData2b(dataManager, fakeMetadata);

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
       ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

       // Run query
       TestProcessor.helpProcess(plan, dataManager, expected);
    }

    /** SELECT pm1.g1.e2, pm2.g1.e2 FROM pm1.g1, pm2.g1 WHERE (pm1.g1.e2+1)=pm2.g1.e2 OPTION MAKEDEP pm1.g2 */
    @Test public void testExpressionInDepJoin() { 
       // Create query 
       String sql = "SELECT pm1.g1.e2, pm2.g1.e2 FROM pm1.g1, pm2.g1 WHERE (pm1.g1.e2+1)=pm2.g1.e2 order by pm1.g1.e2, pm2.g1.e2 OPTION MAKEDEP pm2.g1"; //$NON-NLS-1$
       
       // Create expected results
       List[] expected = new List[] { 
           Arrays.asList(new Object[] { 0, 1 }),
           Arrays.asList(new Object[] { 0, 1 }),
           Arrays.asList(new Object[] { 0, 1 }),
           Arrays.asList(new Object[] { 0, 1 }),
           Arrays.asList(new Object[] { 1, 2 }),
           Arrays.asList(new Object[] { 1, 2 }),
           Arrays.asList(new Object[] { 2, 3 })
       };    
       
       // Construct data manager with data
       FakeDataManager dataManager = new FakeDataManager();
       TestProcessor.sampleData1(dataManager);
       
       // Plan query

       FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
       BasicSourceCapabilities caps = new BasicSourceCapabilities();
       caps.setCapabilitySupport(Capability.CRITERIA_IN, true);
       caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 1000);
       capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
       capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$
       
       Command command = TestProcessor.helpParse(sql);   
       ProcessorPlan plan = TestProcessor.helpGetPlan(command, RealMetadataFactory.example1Cached(), capFinder);

       // Run query
       TestProcessor.helpProcess(plan, dataManager, expected);
   }    
    
    @Test public void testDependentJoinBackoff() throws Exception {
        FakeDataManager dataManager = helpTestBackoff(true);
        
        //note that the dependent join was not actually performed
        assertEquals(new HashSet<String>(Arrays.asList("SELECT pm6.g1.e1, pm6.g1.e2 FROM pm6.g1 ORDER BY pm6.g1.e1, pm6.g1.e2", "SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1")), 
        		new HashSet<String>(dataManager.getQueries()));
    }
    
    @Test public void testDependentJoinBackoff1() throws Exception {
        FakeDataManager dataManager = helpTestBackoff(false);
        
        //note that the dependent join was performed
        assertEquals(4, new HashSet<String>(dataManager.getQueries()).size());
    }
    
    @Test public void testIssue1899() throws Exception {
    	String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm3.g1 WHERE pm1.g1.e1=pm3.g1.e1"; //$NON-NLS-1$

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm3.g1.e1 FROM pm3.g1 ORDER BY pm3.g1.e1", new List<?>[] {Arrays.asList("a"), Arrays.asList("b"), Arrays.asList("c")});
        dataManager.addData("SELECT pm1.g1.e1 FROM pm1.g1", new List<?>[] {Arrays.asList("a")});

        TransformationMetadata fakeMetadata = RealMetadataFactory.example4();
        fakeMetadata.getGroupID("pm1.g1").getAccessPatterns().clear();
        RealMetadataFactory.setCardinality("pm1.g1", 1000, fakeMetadata);
    	fakeMetadata.getElementID("pm1.g1.e1").setDistinctValues(40);
        RealMetadataFactory.setCardinality("pm3.g1", 1, fakeMetadata);
    	fakeMetadata.getElementID("pm3.g1.e1").setDistinctValues(1);
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities depcaps = new BasicSourceCapabilities();
        depcaps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        depcaps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        depcaps.setSourceProperty(Capability.QUERY_ORDERBY_DEFAULT_NULL_ORDER, NullOrder.HIGH);

        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);
        caps.setSourceProperty(Capability.QUERY_ORDERBY_DEFAULT_NULL_ORDER, NullOrder.HIGH);

        capFinder.addCapabilities("pm3", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm1", depcaps); //$NON-NLS-1$

        List[] expected = new List[] {
            Arrays.asList(new Object[] {
                new String("a")})}; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, fakeMetadata, new String[] {
        		"SELECT pm1.g1.e1 FROM pm1.g1 WHERE pm1.g1.e1 IN (<dependent values>)", 
        		"SELECT pm3.g1.e1 FROM pm3.g1 ORDER BY pm3.g1.e1"
        }, capFinder, ComparisonMode.EXACT_COMMAND_STRING);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }

	private FakeDataManager helpTestBackoff(boolean setNdv) throws Exception,
			QueryMetadataException, TeiidComponentException,
			TeiidProcessingException {
		// Create query 
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm6.g1 WHERE pm1.g1.e1=pm6.g1.e1 and pm1.g1.e2=pm6.g1.e2"; //$NON-NLS-1$

        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        sampleData4(dataManager);

        TransformationMetadata fakeMetadata = RealMetadataFactory.example1();

        RealMetadataFactory.setCardinality("pm1.g1", 1, fakeMetadata);
        if (setNdv) {
        	fakeMetadata.getElementID("pm1.g1.e1").setDistinctValues(1);
        	fakeMetadata.getElementID("pm1.g1.e2").setDistinctValues(1);
        }
        RealMetadataFactory.setCardinality("pm6.g1", 1000, fakeMetadata);
        if (setNdv) {
        	fakeMetadata.getElementID("pm6.g1.e1").setDistinctValues(1000);
        	fakeMetadata.getElementID("pm6.g1.e2").setDistinctValues(1000);
        }
        // Plan query
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities depcaps = new BasicSourceCapabilities();
        depcaps.setCapabilitySupport(Capability.CRITERIA_IN, true);
        depcaps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 1);
        depcaps.setCapabilitySupport(Capability.QUERY_ORDERBY, true);

        BasicSourceCapabilities caps = new BasicSourceCapabilities();
        caps.setCapabilitySupport(Capability.CRITERIA_IN, true);

        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm6", depcaps); //$NON-NLS-1$

        List[] expected = new List[] {
            Arrays.asList(new Object[] {
                new String("b")})}; //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan(sql, fakeMetadata, new String[] {
        		"SELECT pm6.g1.e1, pm6.g1.e2 FROM pm6.g1 WHERE (pm6.g1.e1 IN (<dependent values>)) AND (pm6.g1.e2 IN (<dependent values>)) ORDER BY pm6.g1.e1, pm6.g1.e2", 
        		"SELECT pm1.g1.e1, pm1.g1.e2 FROM pm1.g1"
        }, capFinder, ComparisonMode.EXACT_COMMAND_STRING);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
		return dataManager;
	}
    
    @Test public void testDjHint() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1 WHERE e1 IN /*+ DJ */ (select e1 from pm2.g1) order by pm1.g1.e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
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
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        TestOptimizer.checkDependentJoinCount(plan, 1);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testMakeIndHint() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1 FROM /*+ MAKEIND */ pm1.g1, pm2.g1 WHERE pm1.g1.e1 = pm2.g1.e1 AND pm1.g1.e2=pm2.g1.e2 order by pm1.g1.e1"; //$NON-NLS-1$
        
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
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());
        TestOptimizer.checkDependentJoinCount(plan, 1);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testMakeIndHintPushdown() { 
    	helpTestPushdown(true);
    	helpTestPushdown(false);
    }

	private void helpTestPushdown(boolean supportsArrayType) {
		// Create query 
        String sql = "SELECT pm1.g1.e1 FROM /*+ MAKEIND */ pm1.g1, pm2.g1 WHERE pm1.g1.e1 = pm2.g1.e1 AND pm1.g1.e2=pm2.g1.e2 order by pm1.g1.e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
        };    
        
        // Construct data manager with data
        HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
        dataManager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM g1 AS g_0 ORDER BY c_0, c_1", new List[] {Arrays.asList("a", 1), Arrays.asList("b", 2)});
        if (supportsArrayType) {
        	dataManager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM g1 AS g_0 WHERE (g_0.e1, g_0.e2) = (?, ?) ORDER BY c_0, c_1", new List[] {Arrays.asList("a", 1)});
        } else {
        	dataManager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM g1 AS g_0 WHERE g_0.e1 = ? AND g_0.e2 = ? ORDER BY c_0, c_1", new List[] {Arrays.asList("a", 1)});
        }
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.DEPENDENT_JOIN, true);
        bsc.setCapabilitySupport(Capability.ARRAY_TYPE, supportsArrayType);
        bsc.setSourceProperty(Capability.MAX_DEPENDENT_PREDICATES, 1);
        bsc.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 1);
        DefaultCapabilitiesFinder dcf = new DefaultCapabilitiesFinder(bsc);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached(), dcf);
        TestOptimizer.checkDependentJoinCount(plan, 1);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
        
        Select s = (Select)dataManager.getPushdownCommands().get(1);
        assertEquals(1, s.getDependentValues().size());
        List<? extends List<?>> vals = s.getDependentValues().values().iterator().next();
        assertEquals(2, vals.size());
        if (supportsArrayType) {
        	Comparison comp = (Comparison) s.getWhere();
        	Parameter p = (Parameter)((Array)comp.getRightExpression()).getExpressions().get(0);
        	assertEquals(0, p.getValueIndex());
        	assertNotNull(s.getDependentValues().get(p.getDependentValueId()));
        }
	}
	
    @Test public void testIndependentDupRemoval1() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1, pm1.g1.e2, x.e2 FROM pm1.g1, pm2.g2, /*+ makeind */ (select distinct e1, e2 from pm2.g1) x WHERE x.e1=pm1.g1.e1 AND pm2.g2.e2=x.e2"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", 1, 1 }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b", 1, 1 }), //$NON-NLS-1$
        };   
        
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 1);
        caps.setSourceProperty(Capability.MAX_DEPENDENT_PREDICATES, 1);

        QueryMetadataInterface fakeMetadata = RealMetadataFactory.example1Cached();

        Command command = TestProcessor.helpParse(sql);
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, fakeMetadata, new DefaultCapabilitiesFinder(caps));
        
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT DISTINCT g_0.e1, g_0.e2 FROM pm2.g1 AS g_0", new List<?>[] {Arrays.asList("b", 1), Arrays.asList("a", 1)});
        dataManager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 WHERE g_0.e1 = 'b'", new List<?>[] {Arrays.asList("b", 1)});
        dataManager.addData("SELECT g_0.e2 AS c_0 FROM pm2.g2 AS g_0 ORDER BY c_0", new List<?>[] {Arrays.asList(1), Arrays.asList(2), Arrays.asList(3)});
        dataManager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 WHERE g_0.e1 = 'a'", new List<?>[] {Arrays.asList("a", 1)});
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }
	
    @Test public void testIndependentDupRemoval() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1, pm1.g1.e2, pm2.g1.e2 FROM pm1.g1, pm2.g1 WHERE pm2.g1.e1=pm1.g1.e1 AND pm1.g1.e2<pm2.g1.e2 order by pm1.g1.e1 option makedep pm1.g1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a", 0, 3 }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a", 0, 3 }), //$NON-NLS-1$
        };   
        
        BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
        caps.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 1);
        caps.setSourceProperty(Capability.MAX_DEPENDENT_PREDICATES, 1);
        caps.setCapabilitySupport(Capability.QUERY_ORDERBY, false);

        QueryMetadataInterface fakeMetadata = RealMetadataFactory.example1Cached();

        Command command = TestProcessor.helpParse(sql);
        ProcessorPlan plan = TestProcessor.helpGetPlan(command, fakeMetadata, new DefaultCapabilitiesFinder(caps));
        
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        
        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }
    
    /**
     * ensures that we omit dependent join columns over the max
     */
	@Test public void testPushdownMax() {
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 MAKEDEP(max:2) WHERE pm1.g1.e1 = pm2.g1.e1 AND pm1.g1.e2=pm2.g1.e2 order by pm1.g1.e1"; //$NON-NLS-1$
        
        helpTestMax(sql);
	}
	
	@Test public void testPushdownMaxInd() {
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1 MAKEIND(max:2), pm2.g1 WHERE pm1.g1.e1 = pm2.g1.e1 AND pm1.g1.e2=pm2.g1.e2 order by pm1.g1.e1"; //$NON-NLS-1$
        
        helpTestMax(sql);
	}

	private void helpTestMax(String sql) {
		List<?>[] expected = new List<?>[] { 
            Arrays.asList("a"), //$NON-NLS-1$
            Arrays.asList("b"), //$NON-NLS-1$
            Arrays.asList("c"), //$NON-NLS-1$
        };    
        
        HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
        dataManager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM g1 AS g_0 ORDER BY c_0, c_1", new List<?>[] {Arrays.asList("a", 1), Arrays.asList("b", 1), Arrays.asList("c", 1)});
        
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.DEPENDENT_JOIN, true);
        DefaultCapabilitiesFinder dcf = new DefaultCapabilitiesFinder(bsc);
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached(), dcf);
        TestOptimizer.checkDependentJoinCount(plan, 1);

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
        
        Select s = (Select)dataManager.getPushdownCommands().get(1);
        assertNull(s.getDependentValues());
	}
	
    @Test public void testFullDepJoin() throws Exception {
    	BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.FULL_DEPENDENT_JOIN, true);
		ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1, pm1.g1.e2, pm2.g1.e2 FROM pm1.g1, pm2.g1 where (pm1.g1.e1 = pm2.g1.e1) option makedep pm1.g1(join)", TestOptimizer.example1(), //$NON-NLS-1$
            new String[] { "WITH TEIID_TEMP__1 (col1, col2) AS (<dependent values>) SELECT g_0.e1, g_0.e2, g_1.col2 FROM pm1.g1 AS g_0, TEIID_TEMP__1 AS g_1 WHERE g_0.e1 = g_1.col1" }, new DefaultCapabilitiesFinder(caps), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$
		
		TestOptimizer.checkNodeTypes(plan, TestOptimizer.FULL_PUSHDOWN); 
		
		List<?>[] expected = new List<?>[] { 
            Arrays.asList(new Object[] { "a", 2, 1 }), //$NON-NLS-1$
        };  
		
		HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
		dataManager.addData("SELECT g_0.e1, g_0.e2 FROM g1 AS g_0", new List<?>[] {Arrays.asList("a", 1)});
		dataManager.addData("WITH TEIID_TEMP__1 (e1, e2) AS (?) SELECT g_0.e1, g_0.e2, g_1.col2 FROM g1 AS g_0, TEIID_TEMP__1 AS g_1 WHERE g_0.e1 = g_1.col1", new List<?>[] {Arrays.asList("a", 2, 1)});
		TestProcessor.helpProcess(plan, dataManager, expected);
		Select select = (Select)dataManager.getPushdownCommands().get(1);
		List<? extends List<?>> vals = select.getWith().getItems().get(0).getDependentValues();
		assertEquals(1, vals.size());
    }
    
    @Test public void testNoFullDepJoin() throws Exception {
    	BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.FULL_DEPENDENT_JOIN, true);
    	String sql = "select pm1.g1.e1, pm1.g1.e2, pm2.g1.e2 FROM pm1.g1, pm2.g1 where (pm1.g1.e1 = pm2.g1.e1) option makedep pm1.g1(no join)";
		assertEquals("SELECT pm1.g1.e1, pm1.g1.e2, pm2.g1.e2 FROM pm1.g1, pm2.g1 WHERE pm1.g1.e1 = pm2.g1.e1 OPTION MAKEDEP pm1.g1(NO JOIN)", QueryParser.getQueryParser().parseCommand(sql).toString());
		//pass a debug analysisrecord to test debug annotations
		TestOptimizer.helpPlanCommand(TestOptimizer.helpGetCommand(sql, TestOptimizer.example1(), null), TestOptimizer.example1(), 
				new DefaultCapabilitiesFinder(caps), new AnalysisRecord(true, true), 
				new String[] { "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0" }, TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testBindings() throws Exception {
    	BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.DEPENDENT_JOIN_BINDINGS, true);
		ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1, pm1.g1.e2, pm2.g1.e2 FROM pm1.g1, /*+ makedep */ pm2.g1 where (pm1.g1.e1 = pm2.g1.e1)", TestOptimizer.example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0" }, new DefaultCapabilitiesFinder(caps), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$
		
		List<?>[] expected = new List<?>[] { 
            Arrays.asList(new Object[] { "a", 1, 2 }), //$NON-NLS-1$
        };  
		
		HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
		dataManager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM g1 AS g_0 ORDER BY c_0", new List<?>[] {Arrays.asList("a", 1)});
		dataManager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM g1 AS g_0 WHERE g_0.e1 = 'a' ORDER BY c_0", new List<?>[] {Arrays.asList("a", 2)});
		TestProcessor.helpProcess(plan, dataManager, expected);
		Select select = (Select)dataManager.getPushdownCommands().get(1);
		assertTrue(((Literal)((Comparison)select.getWhere()).getRightExpression()).isBindEligible());
    }
    
    /**
     * Test if the join cannot be pushed due to other capabilities
     * @throws Exception
     */
    @Test public void testFullDepJoin1() throws Exception {
    	BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.FULL_DEPENDENT_JOIN, true);
    	caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
    	caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);
		ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1, pm1.g1.e2, pm2.g1.e2 FROM pm1.g1, pm2.g1 where (pm1.g1.e1 = pm2.g1.e1) option makedep pm1.g1(join)", TestOptimizer.example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0" }, new DefaultCapabilitiesFinder(caps), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$
		
		List<?>[] expected = new List<?>[] { 
            Arrays.asList(new Object[] { "a", 2, 1 }), //$NON-NLS-1$
        };  
		
		HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
		dataManager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM g1 AS g_0 ORDER BY c_0", new List<?>[] {Arrays.asList("a", 1)});
		dataManager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM g1 AS g_0 WHERE g_0.e1 = 'a' ORDER BY c_0", new List<?>[] {Arrays.asList("a", 2)});
		TestProcessor.helpProcess(plan, dataManager, expected);
    }
    
    /**
     * Test if the join cannot be pushed due to other capabilities
     * @throws Exception
     */
    @Test public void testFullDepJoinOptimizer() throws Exception {
    	BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.FULL_DEPENDENT_JOIN, true);
    	caps.setCapabilitySupport(Capability.ROW_LIMIT, true);
    	String sql = "select pm1.g1.e1, pm1.g1.e2, pm2.g1.e2 FROM pm1.g1, pm2.g1 where (pm1.g1.e1 = pm2.g1.e1) limit 10";
    	TransformationMetadata metadata = TestOptimizer.example1();
    	RealMetadataFactory.setCardinality("pm1.g1", 100, metadata);
    	metadata.getElementID("pm2.g1.e1").setDistinctValues(1000);
    	RealMetadataFactory.setCardinality("pm2.g1", 1000, metadata);
    	CommandContext cc = new CommandContext();
    	cc.setBufferManager(BufferManagerFactory.getStandaloneBufferManager());
		ProcessorPlan plan = TestOptimizer.getPlan(TestOptimizer.helpGetCommand(sql, metadata, null), metadata, new DefaultCapabilitiesFinder(caps), null, true, cc);
		TestOptimizer.checkAtomicQueries(new String[] {"WITH TEIID_TEMP__1 (col1, col2) AS (<dependent values>) SELECT g_0.col1 AS c_0, g_0.col2 AS c_1, g_1.e2 AS c_2 FROM TEIID_TEMP__1 AS g_0, pm2.g1 AS g_1 WHERE g_0.col1 = g_1.e1 LIMIT 10" }, plan); //$NON-NLS-1$ //$NON-NLS-2$
		
		List<?>[] expected = new List<?>[] { 
            Arrays.asList(new Object[] { "a", 2, 1 }), //$NON-NLS-1$
        };  
		
		HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
		dataManager.addData("SELECT g_0.e1, g_0.e2 FROM g1 AS g_0", new List<?>[] {Arrays.asList("a", 1)});
		dataManager.addData("WITH TEIID_TEMP__1 (e1, e2) AS (?) SELECT g_0.col1 AS c_0, g_0.col2 AS c_1, g_1.e2 AS c_2 FROM TEIID_TEMP__1 AS g_0, g1 AS g_1 WHERE g_0.col1 = g_1.e1 LIMIT 10", new List<?>[] {Arrays.asList("a", 2, 1)});
		TestProcessor.helpProcess(plan, dataManager, expected);
		
		caps.setCapabilitySupport(Capability.ROW_LIMIT, false);
		plan = TestOptimizer.getPlan(TestOptimizer.helpGetCommand(sql, metadata, null), metadata, new DefaultCapabilitiesFinder(caps), null, true, cc);
		TestOptimizer.checkAtomicQueries(new String[] {"SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0", "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0" }, plan); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testMultiwayDuplicate() throws Exception {
    	BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
		ProcessorPlan plan = TestOptimizer.helpPlan("select pm1.g1.e1, pm1.g1.e2, pm2.g1.e2 FROM pm1.g2, pm1.g1, /*+ makedep */ pm2.g1 where pm1.g1.e1 = pm2.g1.e1 and pm1.g2.e1 = pm2.g1.e1", TestOptimizer.example1(), //$NON-NLS-1$
            new String[] { "SELECT g_1.e1 AS c_0, g_0.e1 AS c_1, g_1.e2 AS c_2 FROM pm1.g2 AS g_0, pm1.g1 AS g_1 WHERE g_0.e1 = g_1.e1 ORDER BY c_0",
			"SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g1 AS g_0 WHERE g_0.e1 IN (<dependent values>) ORDER BY c_0" }, new DefaultCapabilitiesFinder(caps), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$
		
		List<?>[] expected = new List<?>[] { 
            Arrays.asList(new Object[] { "a", 1, 2 }), //$NON-NLS-1$
        };  
		
		HardcodedDataManager dataManager = new HardcodedDataManager(RealMetadataFactory.example1Cached());
		dataManager.addData("SELECT g_1.e1 AS c_0, g_0.e1 AS c_1, g_1.e2 AS c_2 FROM g2 AS g_0, g1 AS g_1 WHERE g_0.e1 = g_1.e1 ORDER BY c_0", new List<?>[] {Arrays.asList("a", "a", 1)});
		dataManager.addData("SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM g1 AS g_0 WHERE g_0.e1 = 'a' ORDER BY c_0", new List<?>[] {Arrays.asList("a", 2)});
		TestProcessor.helpProcess(plan, dataManager, expected);
    }
    
    /**
     * Similar to the above, but uses two makeind hints from a join that is not pushed
     * @throws Exception
     */
    @Ignore
    @Test public void testMultiwayDuplicate1() throws Exception {
    	BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
    	caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
    	caps.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);
		TestOptimizer.helpPlan("select pm1.g1.e1, pm1.g1.e2, pm2.g1.e2 FROM /*+ makeind */ pm1.g2, /*+ makeind */pm1.g1, /*+ makedep */ pm2.g1 where pm1.g1.e1 = pm2.g1.e1 and pm1.g2.e1 = pm2.g1.e1", TestOptimizer.example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0 ORDER BY c_0",
			"SELECT g_0.e1 AS c_0 FROM pm1.g2 AS g_0 ORDER BY c_0",
			"SELECT g_0.e1, g_0.e2 FROM pm2.g1 AS g_0 WHERE (g_0.e1 IN (<dependent values>)) AND (g_0.e1 IN (<dependent values>))" }, new DefaultCapabilitiesFinder(caps), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testMultiwayDuplicate2() throws Exception {
    	BasicSourceCapabilities caps = TestOptimizer.getTypicalCapabilities();
		TestOptimizer.helpPlan("select pm1.g1.e1, pm1.g1.e2, pm2.g1.e2 FROM pm1.g2, pm1.g1, /*+ makeind */ pm2.g1 where pm1.g1.e1 = pm2.g1.e1 and pm1.g2.e1 = pm2.g1.e1", TestOptimizer.example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm2.g1 AS g_0 ORDER BY c_0", "SELECT g_1.e1 AS c_0, g_0.e1 AS c_1, g_1.e2 AS c_2 FROM pm1.g2 AS g_0, pm1.g1 AS g_1 WHERE (g_0.e1 = g_1.e1) AND (g_1.e1 IN (<dependent values>)) ORDER BY c_0" }, new DefaultCapabilitiesFinder(caps), TestOptimizer.ComparisonMode.EXACT_COMMAND_STRING ); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @Test public void testSplitPredicateSameTable() throws Exception { 
        // Create query 
        String sql = "SELECT a.e1, b.e3 FROM /*+ makedep */ (select pm1.g1.e1, pm1.g1.e2 from pm1.g1, pm1.g2 where pm1.g1.e3 = pm1.g2.e3) as a, "
        		+ "(select pm2.g1.e1, pm2.g1.e2, pm2.g2.e3 from pm2.g1, pm2.g2 where pm2.g1.e3 = pm2.g2.e3) as b WHERE a.e1=b.e1 AND a.e2=b.e2"; //$NON-NLS-1$
        
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.ARRAY_TYPE, true);

        // Run query
        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {
        	"SELECT g_0.e1 AS c_0, g_0.e2 AS c_1 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE (g_0.e3 = g_1.e3) AND ((g_0.e1, g_0.e2) IN (<dependent values>)) ORDER BY c_0, c_1", 
        	"SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_1.e3 AS c_2 FROM pm2.g1 AS g_0, pm2.g2 AS g_1 WHERE g_0.e3 = g_1.e3 ORDER BY c_0, c_1"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
    }
    
    @Test public void testSplitPredicateDifferentTable() throws Exception { 
        // Create query 
        String sql = "SELECT a.e1, b.e3 FROM /*+ makedep */ (select pm1.g1.e1, pm1.g2.e2 from pm1.g1, pm1.g2 where pm1.g1.e3 = pm1.g2.e3) as a, "
        		+ "(select pm2.g1.e1, pm2.g1.e2, pm2.g2.e3 from pm2.g1, pm2.g2 where pm2.g1.e3 = pm2.g2.e3) as b WHERE a.e1=b.e1 AND a.e2=b.e2"; //$NON-NLS-1$
        
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setCapabilitySupport(Capability.ARRAY_TYPE, true);

        // Run query
        TestOptimizer.helpPlan(sql, RealMetadataFactory.example1Cached(), new String[] {
        	"SELECT g_0.e1 AS c_0, g_0.e2 AS c_1, g_1.e3 AS c_2 FROM pm2.g1 AS g_0, pm2.g2 AS g_1 WHERE g_0.e3 = g_1.e3 ORDER BY c_0, c_1"
        	, "SELECT g_0.e1 AS c_0, g_1.e2 AS c_1 FROM pm1.g1 AS g_0, pm1.g2 AS g_1 WHERE (g_0.e3 = g_1.e3) AND (g_0.e1 IN (<dependent values>)) AND (g_1.e2 IN (<dependent values>)) ORDER BY c_0, c_1"}, new DefaultCapabilitiesFinder(bsc), ComparisonMode.EXACT_COMMAND_STRING);
    }
    
    @Test public void testIndependentNestedOrderedLimit() {
	    String sql = "with a (x, y, z) as (select e1, e2, e3 from pm1.g1) SELECT a.x, b.e1 from a, /*+ makeind */ (SELECT * from pm1.g2, a where e1 = x and z = 1 order by e2 limit 2) as b where a.x = b.e1"; //$NON-NLS-1$
	    
	    List<?>[] expected = new List[] { 
	        Arrays.asList("a", "a"),
	        Arrays.asList("a", "a"),
	        Arrays.asList("a", "a"),
	        Arrays.asList("a", "a"),
	        Arrays.asList("a", "a"),
	        Arrays.asList("a", "a"),
	    };    
	
	    FakeDataManager dataManager = new FakeDataManager();
	    dataManager.setBlockOnce();
	    sampleData1(dataManager);
	    
	    ProcessorPlan plan = TestProcessor.helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached());
	    
	    helpProcess(plan, dataManager, expected);
	}
    
    @Test public void testNestedLeftDependent() {
	    String sql = "SELECT pm1.g1.e1, pm1.g2.e2 from /*+ makeind */ pm1.g1 inner join /*+ preserve */ (/*+ makeind */ pm1.g2 inner join pm1.g3 on pm1.g2.e2 = pm1.g3.e2) on pm1.g1.e1 = pm1.g2.e1"; //$NON-NLS-1$
	    
	    List<?>[] expected = new List[] { 
	    		Arrays.asList("c", 0),
		        Arrays.asList("a", 1),
		        Arrays.asList("b", 2),
	    };    
	    
	    HardcodedDataManager hdm = new HardcodedDataManager();
	    hdm.addData("SELECT g_0.e1 AS c_0 FROM pm1.g1 AS g_0 ORDER BY c_0", Arrays.asList("a"), Arrays.asList("b"), Arrays.asList("c"));
	    hdm.addData("SELECT g_0.e2 AS c_0, g_0.e1 AS c_1 FROM pm1.g2 AS g_0 WHERE g_0.e1 IN ('a', 'b')", Arrays.asList(1, "a"), Arrays.asList(2, "b"));
	    hdm.addData("SELECT g_0.e2 AS c_0, g_0.e1 AS c_1 FROM pm1.g2 AS g_0 WHERE g_0.e1 = 'c'", Arrays.asList(0, "c"));
	    hdm.addData("SELECT g_0.e2 AS c_0 FROM pm1.g3 AS g_0 WHERE g_0.e2 IN (0, 1)", Arrays.asList(1), Arrays.asList(0));
	    hdm.addData("SELECT g_0.e2 AS c_0 FROM pm1.g3 AS g_0 WHERE g_0.e2 = 2", Arrays.asList(2));
	    
	    BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
	    bsc.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 2);
	    bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_INNER, false);
	    bsc.setCapabilitySupport(Capability.QUERY_FROM_JOIN_OUTER, false);
	    
	    ProcessorPlan plan = TestProcessor.helpGetPlan(helpParse(sql), RealMetadataFactory.example1Cached(), new DefaultCapabilitiesFinder(bsc));
	    
	    helpProcess(plan, hdm, expected);
	}
    
    @Test public void testLargeInAsDependentSet() { 
        String sql = "SELECT pm1.g1.e2 FROM pm1.g1 WHERE pm1.g1.e1 in ('a', 'b', 'c', 'd')"; //$NON-NLS-1$
        
        List[] expected = new List[] { 
            Arrays.asList(1),
            Arrays.asList(2),
        };    
        
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.setBlockOnce(true);
        dataManager.addData("SELECT g_0.e2 FROM pm1.g1 AS g_0 WHERE g_0.e1 IN ('a', 'b')", Arrays.asList(1));
        dataManager.addData("SELECT g_0.e2 FROM pm1.g1 AS g_0 WHERE g_0.e1 IN ('c', 'd')", Arrays.asList(2));
        
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 2);
        bsc.setSourceProperty(Capability.MAX_DEPENDENT_PREDICATES, 1);
        
        DefaultCapabilitiesFinder dcf = new DefaultCapabilitiesFinder(bsc);
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached(), dcf);

        TestProcessor.helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testMultiCritDepJoin1WithLargeIn() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, pm2.g1 WHERE pm1.g1.e1=pm2.g1.e1 and pm1.g1.e2 IN (1,2,3) option makedep pm1.g1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
        };    
        
        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.setBlockOnce(true);
        dataManager.addData("SELECT g_0.e1 AS c_0 FROM pm2.g1 AS g_0 ORDER BY c_0", Arrays.asList("a"));
        dataManager.addData("SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE (g_0.e2 = 1) AND (g_0.e1 = 'a')", Arrays.asList("a"));
        dataManager.addData("SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE (g_0.e2 = 2) AND (g_0.e1 = 'a')", Arrays.asList("a"));
        dataManager.addData("SELECT g_0.e1 FROM pm1.g1 AS g_0 WHERE (g_0.e2 = 3) AND (g_0.e1 = 'a')", Arrays.asList("a"));
        
        BasicSourceCapabilities bsc = TestOptimizer.getTypicalCapabilities();
        bsc.setSourceProperty(Capability.MAX_IN_CRITERIA_SIZE, 2);
        bsc.setSourceProperty(Capability.MAX_DEPENDENT_PREDICATES, 1);
        
        DefaultCapabilitiesFinder dcf = new DefaultCapabilitiesFinder(bsc);
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached(), dcf);

        TestProcessor.helpProcess(plan, dataManager, expected);
    }
    
}
