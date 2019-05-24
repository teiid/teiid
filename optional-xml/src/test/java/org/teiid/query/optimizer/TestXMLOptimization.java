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

package org.teiid.query.optimizer;

import static org.teiid.query.rewriter.TestQueryRewriter.*;

import org.junit.Test;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.capabilities.BasicSourceCapabilities;
import org.teiid.query.optimizer.capabilities.FakeCapabilitiesFinder;
import org.teiid.query.processor.ProcessorPlan;
import org.teiid.query.unittest.RealMetadataFactory;

@SuppressWarnings("nls")
public class TestXMLOptimization {

    @Test public void testRewriteXmlElement() throws Exception {
        String original = "xmlserialize(document xmlelement(name a, xmlattributes('b' as c)) as string)"; //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        helpTestRewriteExpression(original, "'<a c=\"b\"></a>'", metadata);
    }

    @Test public void testRewriteXmlElement1() throws Exception {
        String original = "xmlelement(name a, xmlattributes(1+1 as c), BQT1.SmallA.timevalue)"; //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        helpTestRewriteExpression(original, "XMLELEMENT(NAME a, XMLATTRIBUTES(2 AS c), BQT1.SmallA.timevalue)", metadata);
    }

    @Test public void testRewriteXmlSerialize() throws Exception {
        String original = "xmlserialize(document xmlelement(name a, xmlattributes('b' as c)) as string)"; //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        helpTestRewriteExpression(original, "'<a c=\"b\"></a>'", metadata);
    }

    @Test public void testRewriteXmlTable() throws Exception {
        String original = "select * from xmltable('/' passing 1 + 1 as a columns x string default curdate()) as x"; //$NON-NLS-1$
        QueryMetadataInterface metadata = RealMetadataFactory.exampleBQTCached();
        helpTestRewriteCommand(original, "SELECT x.x FROM XMLTABLE('/' PASSING 2 AS a COLUMNS x string DEFAULT convert(curdate(), string)) AS x", metadata);
    }

    @Test public void testCountXMLAgg() {
        FakeCapabilitiesFinder capFinder = new FakeCapabilitiesFinder();
        BasicSourceCapabilities caps = TestAggregatePushdown.getAggregateCapabilities();
        capFinder.addCapabilities("pm1", caps); //$NON-NLS-1$
        capFinder.addCapabilities("pm2", caps); //$NON-NLS-1$

        ProcessorPlan plan = TestOptimizer.helpPlan("select count(X.e1), xmlagg(xmlelement(name e1, x.e1) order by x.e2) FROM pm1.g1 as X, pm2.g2 as Y group by X.e2", RealMetadataFactory.example1Cached(), null, capFinder,  //$NON-NLS-1$
            new String[]{"SELECT 1 FROM pm2.g2 AS g_0", "SELECT g_0.e2, g_0.e1 FROM pm1.g1 AS g_0"}, true); //$NON-NLS-1$
        TestOptimizer.checkNodeTypes(plan, new int[] {
            2,      // Access
            0,      // DependentAccess
            0,      // DependentSelect
            0,      // DependentProject
            0,      // DupRemove
            1,      // Grouping
            1,      // NestedLoopJoinStrategy
            0,      // MergeJoinStrategy
            0,      // Null
            0,      // PlanExecution
            1,      // Project
            0,      // Select
            0,      // Sort
            0       // UnionAll
        });
    }

}
