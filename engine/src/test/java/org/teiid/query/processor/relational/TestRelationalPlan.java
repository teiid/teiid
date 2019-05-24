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

package org.teiid.query.processor.relational;

import java.util.List;

import org.teiid.common.buffer.TupleBatch;
import org.teiid.query.processor.relational.RelationalNode;
import org.teiid.query.processor.relational.RelationalPlan;

import junit.framework.TestCase;


/**
 */
public class TestRelationalPlan extends TestCase {

    /**
     * Constructor for TestRelationalPlan.
     * @param arg0
     */
    public TestRelationalPlan(String arg0) {
        super(arg0);
    }

    public void testNoRowsFirstBatch() throws Exception {
        RelationalNode node = new FakeRelationalNode(0, new List[0]);

        RelationalPlan plan = new RelationalPlan(node);
        TupleBatch batch = plan.nextBatch();
        assertTrue("Did not get terminator batch", batch.getTerminationFlag()); //$NON-NLS-1$
    }

}
