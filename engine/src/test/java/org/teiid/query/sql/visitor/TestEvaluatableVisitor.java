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

package org.teiid.query.sql.visitor;

import org.junit.Test;
import org.teiid.query.resolver.TestResolver;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.visitor.EvaluatableVisitor;
import org.teiid.query.unittest.RealMetadataFactory;

import static org.junit.Assert.*;

public class TestEvaluatableVisitor {

    @Test public void testNestedNeedsEvaluation() throws Exception {
        Query command = (Query)TestResolver.helpResolve("select * from pm1.g1 where e1 in (select e1 from pm1.g2 where e2 = ?)", RealMetadataFactory.example1Cached()); //$NON-NLS-1$
        assertTrue(EvaluatableVisitor.needsProcessingEvaluation(command));
    }

}
