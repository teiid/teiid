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

package org.teiid.query.optimizer.relational;

import static org.junit.Assert.*;
import static org.teiid.query.optimizer.TestOptimizer.*;

import java.util.Collection;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.client.plan.Annotation;
import org.teiid.query.analysis.AnalysisRecord;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.optimizer.TestOptimizer;
import org.teiid.query.optimizer.TestOptimizer.ComparisonMode;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.DefaultCapabilitiesFinder;
import org.teiid.query.optimizer.capabilities.SourceCapabilities.Capability;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.processor.relational.RelationalPlan;
import org.teiid.query.sql.lang.Command;
import org.teiid.query.tempdata.GlobalTableStoreImpl;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.util.CommandContext;

@SuppressWarnings("nls")
public class TestMaterialization {
	
    @Test public void testMaterializedTransformation() throws Exception {
        String userSql = "SELECT MATVIEW.E1 FROM MATVIEW"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);
        
        Command command = helpGetCommand(userSql, metadata, null);
        
        TestOptimizer.helpPlanCommand(command, metadata, getGenericFinder(), analysis, new String[] {"SELECT g_0.e1 FROM MatTable.MatTable AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        Collection<Annotation> annotations = analysis.getAnnotations();
        assertNotNull("Expected annotations but got none", annotations); //$NON-NLS-1$
        assertTrue("Expected one annotation", annotations.size() == 1); //$NON-NLS-1$
        assertEquals("Expected catagory mat view", annotations.iterator().next().getCategory(), Annotation.MATERIALIZED_VIEW); //$NON-NLS-1$
    }

    @Ignore("we no longer auto detect this case, if we need this logic it will have to be added to the rewriter since it changes select into to an insert")
    @Test public void testMaterializedTransformationLoading() throws Exception {
        String userSql = "SELECT MATVIEW.E1 INTO MatTable.MatStage FROM MATVIEW"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);

        Command command = helpGetCommand(userSql, metadata, null);
        
        TestOptimizer.helpPlanCommand(command, metadata, getGenericFinder(), analysis, new String[] {"SELECT g_0.x FROM MatSrc.MatSrc AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$

        Collection<Annotation> annotations = analysis.getAnnotations();
        assertNotNull("Expected annotations but got none", annotations); //$NON-NLS-1$
        assertTrue("Expected one annotation", annotations.size() == 1); //$NON-NLS-1$
        assertEquals("Expected catagory mat view", annotations.iterator().next().getCategory(), Annotation.MATERIALIZED_VIEW); //$NON-NLS-1$
    }    
    
    @Test public void testMaterializedTransformationNoCache() throws Exception {
        String userSql = "SELECT MATVIEW.E1 FROM MATVIEW OPTION NOCACHE MatView.MatView"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);
        
        Command command = helpGetCommand(userSql, metadata, null);
        
        TestOptimizer.helpPlanCommand(command, metadata, getGenericFinder(), analysis, new String[] {"SELECT g_0.x FROM MatSrc.MatSrc AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        
        Collection<Annotation> annotations = analysis.getAnnotations();
        assertNotNull("Expected annotations but got none", annotations); //$NON-NLS-1$
        assertTrue("Expected one annotation", annotations.size() == 1); //$NON-NLS-1$
        assertEquals("Expected catagory mat view", annotations.iterator().next().getCategory(), Annotation.MATERIALIZED_VIEW); //$NON-NLS-1$
    }
    
    //related to defect 14423
    @Test public void testMaterializedTransformationNoCache2() throws Exception {
        String userSql = "SELECT MATVIEW.E1 FROM MATVIEW OPTION NOCACHE"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);
        
        Command command = helpGetCommand(userSql, metadata, null);
        
        TestOptimizer.helpPlanCommand(command, metadata, getGenericFinder(), analysis, new String[] {"SELECT g_0.x FROM MatSrc.MatSrc AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
        
        Collection<Annotation> annotations = analysis.getAnnotations();
        assertNotNull("Expected annotations but got none", annotations); //$NON-NLS-1$
        assertTrue("Expected one annotation", annotations.size() == 1); //$NON-NLS-1$
        assertEquals("Expected catagory mat view", annotations.iterator().next().getCategory(), Annotation.MATERIALIZED_VIEW); //$NON-NLS-1$
    }
    
    @Test public void testNoCacheInTransformation() throws Exception {
        String userSql = "SELECT VGROUP.E1 FROM VGROUP"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);
        
        Command command = helpGetCommand(userSql, metadata, null);
        
        TestOptimizer.helpPlanCommand(command, metadata, getGenericFinder(), analysis, new String[] {"SELECT g_0.x FROM MatSrc.MatSrc AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }
    
    @Test public void testTableNoCacheDoesntCascade() throws Exception {
        String userSql = "SELECT MATVIEW1.E1 FROM MATVIEW1 option nocache matview.matview1"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);
        
        Command command = helpGetCommand(userSql, metadata, null);
        
        TestOptimizer.helpPlanCommand(command, metadata, getGenericFinder(), analysis, new String[] {"SELECT g_0.e1 FROM MatTable.MatTable AS g_0 WHERE g_0.e1 = '1'"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }
    
    @Test public void testNoCacheCascade() throws Exception {
        String userSql = "SELECT MATVIEW1.E1 FROM MATVIEW1 option nocache"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);
        
        Command command = helpGetCommand(userSql, metadata, null);
        
        TestOptimizer.helpPlanCommand(command, metadata, getGenericFinder(), analysis, new String[] {"SELECT g_0.x FROM MatSrc.MatSrc AS g_0 WHERE g_0.x = '1'"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }
    
    @Test public void testDefaultMaterialization() throws Exception {
        String userSql = "SELECT * from vgroup2"; //$NON-NLS-1$
        
        TransformationMetadata metadata = RealMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);
        
        Command command = helpGetCommand(userSql, metadata, null);
        CommandContext cc = new CommandContext();
    	GlobalTableStoreImpl gts = new GlobalTableStoreImpl(null, metadata.getVdbMetaData(), metadata);
    	cc.setGlobalTableStore(gts);
        ProcessorPlan plan = TestOptimizer.getPlan(command, metadata, getGenericFinder(), analysis, true, cc);
        TestOptimizer.checkAtomicQueries(new String[] {"SELECT #MAT_MATVIEW.VGROUP2.x FROM #MAT_MATVIEW.VGROUP2"}, plan);
        Collection<Annotation> annotations = analysis.getAnnotations();
        assertNotNull("Expected annotations but got none", annotations); //$NON-NLS-1$
        assertEquals("Expected one annotation", 1, annotations.size()); //$NON-NLS-1$
        assertEquals("Expected catagory mat view", annotations.iterator().next().getCategory(), Annotation.MATERIALIZED_VIEW); //$NON-NLS-1$
    }
    
    @Test public void testDefaultMaterializationWithPK() throws Exception {
        String userSql = "SELECT * from vgroup3 where x = 'foo'"; //$NON-NLS-1$
        
        TransformationMetadata metadata = RealMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);
        
        Command command = helpGetCommand(userSql, metadata, null);
        CommandContext cc = new CommandContext();
        GlobalTableStoreImpl gts = new GlobalTableStoreImpl(null, metadata.getVdbMetaData(), metadata);
    	cc.setGlobalTableStore(gts);
        RelationalPlan plan = (RelationalPlan)TestOptimizer.getPlan(command, metadata, getGenericFinder(), analysis, true, cc);
        assertEquals(1f, plan.getRootNode().getEstimateNodeCardinality());
        TestOptimizer.checkAtomicQueries(new String[] {"SELECT #MAT_MATVIEW.VGROUP3.x, #MAT_MATVIEW.VGROUP3.y FROM #MAT_MATVIEW.VGROUP3 WHERE #MAT_MATVIEW.VGROUP3.x = 'foo'"}, plan);
        Collection<Annotation> annotations = analysis.getAnnotations();
        assertNotNull("Expected annotations but got none", annotations); //$NON-NLS-1$
        assertEquals("Expected one annotation", 1, annotations.size()); //$NON-NLS-1$
        assertEquals("Expected catagory mat view", annotations.iterator().next().getCategory(), Annotation.MATERIALIZED_VIEW); //$NON-NLS-1$
    }
    
    @Test public void testDefaultMaterializationWithCacheHint() throws Exception {
        String userSql = "SELECT * from vgroup4"; //$NON-NLS-1$
        
        TransformationMetadata metadata = RealMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);
        
        Command command = helpGetCommand(userSql, metadata, null);
        CommandContext cc = new CommandContext();
        GlobalTableStoreImpl gts = new GlobalTableStoreImpl(null, metadata.getVdbMetaData(), metadata);
    	cc.setGlobalTableStore(gts);
        ProcessorPlan plan = TestOptimizer.getPlan(command, metadata, getGenericFinder(), analysis, true, cc);
        TestOptimizer.checkAtomicQueries(new String[] {"SELECT #MAT_MATVIEW.VGROUP4.x FROM #MAT_MATVIEW.VGROUP4"}, plan);
        Collection<Annotation> annotations = analysis.getAnnotations();
        assertNotNull("Expected annotations but got none", annotations); //$NON-NLS-1$
        assertEquals("Expected one annotation", 1, annotations.size()); //$NON-NLS-1$
        assertEquals("Expected catagory mat view", annotations.iterator().next().getCategory(), Annotation.MATERIALIZED_VIEW); //$NON-NLS-1$
    }

    @Test public void testManagedMaterializedTransformation() throws Exception {
        String userSql = "SELECT * FROM ManagedMatView"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);
        
        Command command = helpGetCommand(userSql, metadata, null);
        
        RelationalPlan plan = (RelationalPlan) TestOptimizer.helpPlanCommand(command, metadata, getGenericFinder(),
                analysis,
                new String[] {
                        "SELECT g_0.e1 FROM MatTable.MatTable AS g_0 WHERE mvstatus('MatView', 'ManagedMatView') = 1" }, //$NON-NLS-1$
                ComparisonMode.EXACT_COMMAND_STRING);

        Collection<Annotation> annotations = analysis.getAnnotations();
        assertNotNull("Expected annotations but got none", annotations); //$NON-NLS-1$
        assertEquals("Expected catagory mat view", annotations.iterator().next().getCategory(), Annotation.MATERIALIZED_VIEW); //$NON-NLS-1$
    }
    
    @Test public void testManagedMaterializedTransformationJoin() throws Exception {
        //make sure view removal is not inhibited
        String userSql = "SELECT * FROM ManagedMatView left outer join MatTable1 on ManagedMatView.e1 = MatTable1.e1"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);
        
        Command command = helpGetCommand(userSql, metadata, null);
        
        TestOptimizer.helpPlanCommand(command, metadata, getGenericFinder(),
                analysis,
                new String[] {
                        "SELECT g_1.e1, g_0.e1 FROM MatTable.MatTable AS g_0 LEFT OUTER JOIN MatTable.MatTable1 AS g_1 ON g_0.e1 = g_1.e1 WHERE mvstatus('MatView', 'ManagedMatView') = 1" }, //$NON-NLS-1$
                ComparisonMode.EXACT_COMMAND_STRING);
    }
    
    @Test public void testManagedMaterializedTransformationUnion() throws Exception {
        //make sure view removal is not inhibited
        String userSql = "SELECT * FROM ManagedMatView union all SELECT * FROM ManagedMatView"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);
        
        Command command = helpGetCommand(userSql, metadata, null);
        BasicSourceCapabilities capabilities = getTypicalCapabilities();
        capabilities.setCapabilitySupport(Capability.QUERY_UNION, true);
        TestOptimizer.helpPlanCommand(command, metadata, new DefaultCapabilitiesFinder(capabilities),
                analysis,
                new String[] {
                        "SELECT g_1.e1 AS c_0 FROM MatTable.MatTable AS g_1 WHERE mvstatus('MatView', 'ManagedMatView') = 1 UNION ALL SELECT g_0.e1 AS c_0 FROM MatTable.MatTable AS g_0" }, //$NON-NLS-1$
                ComparisonMode.EXACT_COMMAND_STRING);
    }
    
    @Test public void testManagedMaterializedTransformationSubquery() throws Exception {
        String userSql = "SELECT e1, (select min(e1) from ManagedMatView x where x.e1 > MatTable1.e1) FROM MatTable1"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = RealMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);
        
        Command command = helpGetCommand(userSql, metadata, null);
        BasicSourceCapabilities capabilities = getTypicalCapabilities();
        capabilities.setCapabilitySupport(Capability.QUERY_SUBQUERIES_CORRELATED, true);
        capabilities.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR, true);
        capabilities.setCapabilitySupport(Capability.QUERY_SUBQUERIES_SCALAR_PROJECTION, true);
        capabilities.setCapabilitySupport(Capability.QUERY_AGGREGATES_MIN, true);
        TestOptimizer.helpPlanCommand(command, metadata, new DefaultCapabilitiesFinder(capabilities),
                analysis,
                new String[] {
                        "SELECT g_0.e1, (SELECT MIN(g_1.e1) FROM MatTable.MatTable AS g_1 WHERE (mvstatus('MatView', 'ManagedMatView') = 1) AND (g_1.e1 > g_0.e1)) FROM MatTable.MatTable1 AS g_0" }, //$NON-NLS-1$
                ComparisonMode.EXACT_COMMAND_STRING);
    }
    
}
