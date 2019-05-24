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

package org.teiid.query.analysis;

import java.util.Collection;

import junit.framework.TestCase;

import org.teiid.client.plan.Annotation;
import org.teiid.client.plan.Annotation.Priority;


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
        assertEquals("a\nb\n", log); //$NON-NLS-1$
    }

}
