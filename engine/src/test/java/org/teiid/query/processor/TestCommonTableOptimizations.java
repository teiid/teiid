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

package org.teiid.query.processor;

import static org.junit.Assert.*;
import static org.teiid.query.processor.TestProcessor.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Ignore;
import org.junit.Test;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestCommonTableOptimizations {

    @Test public void testDuplicateSourceQuery() {
        String sql = "SELECT e1 FROM pm1.g1 union all select e1 from pm1.g1"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
            Arrays.asList("a"), //$NON-NLS-1$
            Arrays.asList("b"), //$NON-NLS-1$
            Arrays.asList("a"), //$NON-NLS-1$
            Arrays.asList("b"), //$NON-NLS-1$
        };

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1 FROM pm1.g1", new List<?>[] {Arrays.asList("a"), Arrays.asList("b")});

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        helpProcess(plan, dataManager, expected);
        assertEquals(3, dataManager.getCommandHistory().size());
    }

    @Ignore
    @Test public void testDuplicateSimpleQuery() {
        String sql = "SELECT e1 FROM pm1.g1 union all select e2 from pm1.g1"; //$NON-NLS-1$

        List<?>[] expected = new List[] {
            Arrays.asList("a"), //$NON-NLS-1$
            Arrays.asList("b"), //$NON-NLS-1$
            Arrays.asList("a"), //$NON-NLS-1$
            Arrays.asList("b"), //$NON-NLS-1$
        };

        HardcodedDataManager dataManager = new HardcodedDataManager();
        dataManager.addData("SELECT pm1.g1.e1 FROM pm1.g1", new List<?>[] {Arrays.asList("a"), Arrays.asList("b")});

        ProcessorPlan plan = helpGetPlan(sql, RealMetadataFactory.example1Cached());

        helpProcess(plan, dataManager, expected);
        assertEquals(3, dataManager.getCommandHistory().size());
    }

}
