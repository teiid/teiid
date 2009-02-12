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

package com.metamatrix.query.analysis;

import java.util.*;

import com.metamatrix.core.util.StringUtilities;

import junit.framework.TestCase;

/**
 */
public class TestAnalysisRecord extends TestCase {

    /**
     * Constructor for TestAnalysisRecord.
     * @param name
     */
    public TestAnalysisRecord(String name) {
        super(name);
    }

    public void testQueryPlan() {
        AnalysisRecord rec = new AnalysisRecord(true, false, false);
        assertTrue(rec.recordQueryPlan());
        
        Map plan = new HashMap();
        plan.put("node", "value"); //$NON-NLS-1$ //$NON-NLS-2$
        rec.setQueryPlan(plan);
        assertEquals(rec.getQueryPlan(), plan);
    }
    
    public void testAnnotations() {
        AnalysisRecord rec = new AnalysisRecord(false, true, false);
        assertTrue(rec.recordAnnotations());
        
        QueryAnnotation ann1 = new QueryAnnotation("cat", "ann", "res", QueryAnnotation.MEDIUM); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        QueryAnnotation ann2 = new QueryAnnotation("cat2", "ann2", "res2", QueryAnnotation.HIGH); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
      
        rec.addAnnotation(ann1);
        rec.addAnnotation(ann2);
        
        Collection annotations = rec.getAnnotations();
        assertEquals(2, annotations.size());
        assertTrue(annotations.contains(ann1));
        assertTrue(annotations.contains(ann2));
        
    }
    
    public void testDebugLog() {
        AnalysisRecord rec = new AnalysisRecord(false, false, true);
        assertTrue(rec.recordDebug());
        
        rec.println("a"); //$NON-NLS-1$
        rec.println("b"); //$NON-NLS-1$
        
        String log = rec.getDebugLog();
        assertEquals("a" + StringUtilities.LINE_SEPARATOR + "b" + StringUtilities.LINE_SEPARATOR, log); //$NON-NLS-1$ //$NON-NLS-2$
    }

}
