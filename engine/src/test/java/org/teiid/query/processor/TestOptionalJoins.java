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
import java.util.List;

import org.junit.Test;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("rawtypes")
public class TestOptionalJoins {
    
    @Test public void testOptionalJoinNode1() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, /* optional */ pm1.g2 where pm1.g1.e1 = 'a'"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
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
    
    @Test public void testOptionalJoinNode2() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1, /* optional */ pm1.g2, pm1.g3 where pm1.g1.e1 = 'a' and pm1.g1.e1 = pm1.g3.e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
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
    
    @Test public void testOptionalJoinNode3() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1 LEFT OUTER JOIN /* optional */ pm1.g2 on pm1.g1.e1 = pm1.g2.e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null }), 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
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
    
    @Test public void testOptionalJoinNode4() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1 FROM (pm1.g1 LEFT OUTER JOIN /* optional */ pm1.g2 on pm1.g1.e1 = pm1.g2.e1) LEFT OUTER JOIN /* optional */ pm1.g3 on pm1.g1.e1 = pm1.g3.e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null }), 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
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
    
    @Test public void testOptionalJoinNode5() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1 FROM (pm1.g1 LEFT OUTER JOIN pm1.g2 on pm1.g1.e1 = pm1.g2.e1) LEFT OUTER JOIN /* optional */ pm1.g3 on pm1.g1.e1 = pm1.g3.e1 order by e1"; //$NON-NLS-1$
        
        // Create expected results
        List<?>[] expected = new List<?>[] { 
            Arrays.asList(new Object[] { null }), 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$       
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testOptionalJoinNode6() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1 FROM (pm1.g1 LEFT OUTER JOIN /* optional */ pm1.g2 on pm1.g1.e1 = pm1.g2.e1) LEFT OUTER JOIN pm1.g3 on pm1.g1.e1 = pm1.g3.e1 order by e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null }), 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
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
    
    @Test public void testOptionalJoinNode7() { 
        // Create query 
        String sql = "SELECT pm1.g3.e1 FROM /* optional */ (pm1.g1 LEFT OUTER JOIN pm1.g2 on pm1.g1.e1 = pm1.g2.e1) LEFT OUTER JOIN pm1.g3 on pm1.g1.e1 = pm1.g3.e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null }), 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
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
    
    @Test public void testOptionalJoinNode8() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1 LEFT OUTER JOIN /* optional */ (select * from pm1.g2) as X on pm1.g1.e1 = x.e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null }), 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
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
    
    @Test public void testOptionalJoinNode9() { 
        // Create query 
        String sql = "SELECT pm1.g2.e1 FROM pm1.g2, /* optional */ vm1.g1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null }), 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
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
    
    @Test public void testOptionalJoinNode10() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1 FROM /* optional */ vm1.g1, pm1.g1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null }), 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
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
    
    @Test public void testOptionalJoinNode11() { 
        // Create query 
        String sql = "SELECT pm1.g1.e1 FROM pm1.g1 LEFT OUTER JOIN /* optional */ vm1.g2 on pm1.g1.e1 = vm1.g2.e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null }), 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
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
    
    @Test public void testOptionalJoinNode12() { 
        // Create query 
        String sql = "SELECT pm1.g3.e1 FROM /* optional */ (pm1.g1 LEFT OUTER JOIN vm1.g1 on pm1.g1.e1 = vm1.g1.e1) LEFT OUTER JOIN pm1.g3 on pm1.g1.e1 = pm1.g3.e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null }), 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
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
    
    @Test public void testOptionalJoinNode13() { 
        // Create query 
        String sql = "SELECT count(pm1.g1.e1) FROM pm1.g1 LEFT OUTER JOIN /* optional */ pm1.g2 on pm1.g1.e1 = pm1.g2.e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(5) }) 
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testOptionalJoinNode15() { 
        // Create query 
        String sql = "SELECT x.e1 FROM (select vm1.g1.e1, vm1.g2.e2 from vm1.g1 LEFT OUTER JOIN /* optional */vm1.g2 on vm1.g1.e2 = vm1.g2.e2) AS x"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null }), 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
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
    
    @Test public void testOptionalJoinNode16() { 
        // Create query 
        String sql = "SELECT x.e1 FROM (select vm1.g1.e1, vm1.g2.e2 from vm1.g1 LEFT OUTER JOIN /* optional */vm1.g2 on vm1.g1.e2 = vm1.g2.e2) AS x order by x.e1"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { null }), 
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
    
    @Test public void testOptionalJoinNode17() { 
        // Create query 
        String sql = "SELECT length(z) FROM /* optional */ pm1.g1, (select distinct e2 as y, e3 || 'x' as z from pm1.g1 ORDER BY y, z) AS x"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(6) }),
            Arrays.asList(new Object[] { new Integer(6) }),
            Arrays.asList(new Object[] { new Integer(5) }),
            Arrays.asList(new Object[] { new Integer(6) }),
            Arrays.asList(new Object[] { new Integer(5) }),
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }
    
    @Test public void testOptionalJoinNode18() { 
        // Create query 
        String sql = "SELECT x.e1 FROM (select vm1.g1.e1, vm1.g2.e2 from vm1.g1 LEFT OUTER JOIN /* optional */vm1.g2 on vm1.g1.e2 = vm1.g2.e2) AS x"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { null }), 
            Arrays.asList(new Object[] { "a" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "c" }), //$NON-NLS-1$
            Arrays.asList(new Object[] { "b" }), //$NON-NLS-1$
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
    
    @Test public void testOptionalJoinNode19() { 
        // Create query 
        String sql = "SELECT length(z) FROM /* optional */ pm1.g1 inner join (select e2 as y, e3 || 'x' as z from pm1.g1 ORDER BY z) AS x on pm1.g1.e2=x.y"; //$NON-NLS-1$
        
        // Create expected results
        List[] expected = new List[] { 
            Arrays.asList(new Object[] { new Integer(6) }), 
            Arrays.asList(new Object[] { new Integer(6) }), 
            Arrays.asList(new Object[] { new Integer(5) }), 
            Arrays.asList(new Object[] { new Integer(5) }), 
            Arrays.asList(new Object[] { new Integer(6) }), 
            Arrays.asList(new Object[] { new Integer(6) }) 
        };    
    
        // Construct data manager with data
        FakeDataManager dataManager = new FakeDataManager();
        TestProcessor.sampleData1(dataManager);
        
        // Plan query
        ProcessorPlan plan = TestProcessor.helpGetPlan(sql, RealMetadataFactory.example1Cached());

        // Run query
        TestProcessor.helpProcess(plan, dataManager, expected);
    }    

    
}
