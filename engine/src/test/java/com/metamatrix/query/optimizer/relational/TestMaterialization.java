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

package com.metamatrix.query.optimizer.relational;

import static com.metamatrix.query.optimizer.TestOptimizer.*;
import static org.junit.Assert.*;

import java.util.Collection;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.client.plan.Annotation;

import com.metamatrix.query.analysis.AnalysisRecord;
import com.metamatrix.query.metadata.QueryMetadataInterface;
import com.metamatrix.query.optimizer.TestOptimizer;
import com.metamatrix.query.sql.lang.Command;
import com.metamatrix.query.unittest.FakeMetadataFactory;

public class TestMaterialization {
	
    @Test public void testMaterializedTransformation() throws Exception {
        String userSql = "SELECT MATVIEW.E1 FROM MATVIEW"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleMaterializedView();
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
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleMaterializedView();
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
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleMaterializedView();
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
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleMaterializedView();
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
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);
        
        Command command = helpGetCommand(userSql, metadata, null);
        
        TestOptimizer.helpPlanCommand(command, metadata, getGenericFinder(), analysis, new String[] {"SELECT g_0.x FROM MatSrc.MatSrc AS g_0"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }
    
    @Test public void testTableNoCacheDoesntCascade() throws Exception {
        String userSql = "SELECT MATVIEW1.E1 FROM MATVIEW1 option nocache matview.matview1"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);
        
        Command command = helpGetCommand(userSql, metadata, null);
        
        TestOptimizer.helpPlanCommand(command, metadata, getGenericFinder(), analysis, new String[] {"SELECT g_0.e1 FROM MatTable.MatTable AS g_0 WHERE g_0.e1 = '1'"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }
    
    @Test public void testNoCacheCascade() throws Exception {
        String userSql = "SELECT MATVIEW1.E1 FROM MATVIEW1 option nocache"; //$NON-NLS-1$
        
        QueryMetadataInterface metadata = FakeMetadataFactory.exampleMaterializedView();
        AnalysisRecord analysis = new AnalysisRecord(true, DEBUG);
        
        Command command = helpGetCommand(userSql, metadata, null);
        
        TestOptimizer.helpPlanCommand(command, metadata, getGenericFinder(), analysis, new String[] {"SELECT g_0.x FROM MatSrc.MatSrc AS g_0 WHERE g_0.x = '1'"}, ComparisonMode.EXACT_COMMAND_STRING); //$NON-NLS-1$
    }

}
