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

package org.teiid.query.analysis;

import java.util.Collection;

import junit.framework.TestCase;

import org.teiid.client.plan.Annotation;
import org.teiid.client.plan.Annotation.Priority;
import org.teiid.core.util.StringUtil;
import org.teiid.query.analysis.AnalysisRecord;


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

    public void testAnnotations() {
        AnalysisRecord rec = new AnalysisRecord(true, false);
        assertTrue(rec.recordAnnotations());
        
        Annotation ann1 = new Annotation("cat", "ann", "res", Priority.MEDIUM); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Annotation ann2 = new Annotation("cat2", "ann2", "res2", Priority.HIGH); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
      
        rec.addAnnotation(ann1);
        rec.addAnnotation(ann2);
        
        Collection<Annotation> annotations = rec.getAnnotations();
        assertEquals(2, annotations.size());
        assertTrue(annotations.contains(ann1));
        assertTrue(annotations.contains(ann2));
        
    }
    
    public void testDebugLog() {
        AnalysisRecord rec = new AnalysisRecord(false, true);
        assertTrue(rec.recordDebug());
        
        rec.println("a"); //$NON-NLS-1$
        rec.println("b"); //$NON-NLS-1$
        
        String log = rec.getDebugLog();
        assertEquals("a" + StringUtil.LINE_SEPARATOR + "b" + StringUtil.LINE_SEPARATOR, log); //$NON-NLS-1$ //$NON-NLS-2$
    }

}
