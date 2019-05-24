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

package org.teiid.query.optimizer.relational.rules;

import static org.teiid.query.optimizer.TestOptimizer.*;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.query.processor.ProcessorPlan;

public class TestCopyAllCriteria {

    @BeforeClass public static void setup() {
        RuleCopyCriteria.COPY_ALL = true;
    }

    @AfterClass public static void teardown() {
        RuleCopyCriteria.COPY_ALL = false;
    }

    @Test public void testCopyCriteriaWithTransitivePushdown1(){
        ProcessorPlan plan = helpPlan("select pm2.g1.e1, pm2.g2.e1 from pm2.g1, pm2.g2, pm2.g3 where pm2.g1.e1 = pm2.g2.e1 and pm2.g2.e1 = pm2.g3.e1 and pm2.g1.e1 = 'a'", example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1, g_1.e1 FROM pm2.g1 AS g_0, pm2.g2 AS g_1, pm2.g3 AS g_2 WHERE (g_0.e1 = g_1.e1) AND (g_1.e1 = g_2.e1) AND (g_0.e1 = 'a') AND (g_1.e1 = 'a') AND (g_2.e1 = 'a')" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testCopyCriteriaWithOuterJoin7_defect10050(){

        ProcessorPlan plan = helpPlan("select pm2.g1.e1, pm2.g2.e1 from pm2.g1 right outer join pm2.g2 on pm2.g2.e1=pm2.g1.e1 where pm2.g2.e1 IN ('a', 'b')", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm2.g1.e1, pm2.g2.e1 FROM pm2.g2 LEFT OUTER JOIN pm2.g1 ON pm2.g2.e1 = pm2.g1.e1 AND pm2.g1.e1 IN ('a', 'b') WHERE pm2.g2.e1 IN ('a', 'b')" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /**
     * Criteria should be copied across this join
     */
    @Test public void testCopyCriteriaWithOuterJoin_defect10050(){

        ProcessorPlan plan = helpPlan("select pm2.g1.e1, pm2.g2.e1 from pm2.g1 left outer join pm2.g2 on pm2.g1.e1=pm2.g2.e1 where pm2.g1.e1 IN ('a', 'b')", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm2.g1.e1, pm2.g2.e1 FROM pm2.g1 LEFT OUTER JOIN pm2.g2 ON pm2.g1.e1 = pm2.g2.e1 AND pm2.g2.e1 IN ('a', 'b') WHERE pm2.g1.e1 IN ('a', 'b')" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    /**
     * Criteria should be copied across this join
     */
    @Test public void testCopyCriteriaWithOuterJoin2_defect10050(){

        ProcessorPlan plan = helpPlan("select pm2.g1.e1, pm2.g2.e1 from pm2.g1 left outer join pm2.g2 on pm2.g1.e1=pm2.g2.e1 and pm2.g1.e2=pm2.g2.e2 where pm2.g1.e1 = 'a' and pm2.g1.e2 = 1", example1(), //$NON-NLS-1$
            new String[] { "SELECT g_0.e1, g_1.e1 FROM pm2.g1 AS g_0 LEFT OUTER JOIN pm2.g2 AS g_1 ON g_0.e1 = g_1.e1 AND g_0.e2 = g_1.e2 AND g_1.e1 = 'a' AND g_1.e2 = 1 WHERE (g_0.e1 = 'a') AND (g_0.e2 = 1)" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

    @Test public void testCopyCriteriaWithOuterJoin6_defect10050(){

        ProcessorPlan plan = helpPlan("select pm2.g1.e1, pm2.g2.e1 from pm2.g1 left outer join pm2.g2 on pm2.g2.e1=pm2.g1.e1 where pm2.g1.e1 IN ('a', 'b')", example1(), //$NON-NLS-1$
            new String[] { "SELECT pm2.g1.e1, pm2.g2.e1 FROM pm2.g1 LEFT OUTER JOIN pm2.g2 ON pm2.g2.e1 = pm2.g1.e1 AND pm2.g2.e1 IN ('a', 'b') WHERE pm2.g1.e1 IN ('a', 'b')" }); //$NON-NLS-1$
        checkNodeTypes(plan, FULL_PUSHDOWN);
    }

}
