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

import java.util.Collection;

import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.optimizer.relational.plantree.NodeConstants;
import org.teiid.query.optimizer.relational.plantree.NodeFactory;
import org.teiid.query.optimizer.relational.plantree.PlanNode;
import org.teiid.query.optimizer.relational.rules.RulePlaceAccess;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.symbol.MultipleElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.query.unittest.RealMetadataFactory;

import junit.framework.TestCase;


public class TestRulePlaceAccess extends TestCase {

    private static final QueryMetadataInterface METADATA = RealMetadataFactory.example1Cached();

    // ################################## FRAMEWORK ################################

    /**
     * Constructor for TestRulePlaceAccess.
     * @param name
     */
    public TestRulePlaceAccess(String name) {
        super(name);
    }

    /**
     * Tests that any access patterns (a Collection of Collections of
     * Object element ids) for a physical group will be found and added
     * as a property of an ACCESS node.
     */
    public void testAddAccessPatterns2() throws Exception {
        Query query = new Query();

        From from = new From();
        GroupSymbol group = new GroupSymbol("pm4.g2"); //$NON-NLS-1$
        from.addGroup(group);
        query.setFrom(from);

        Select select = new Select();
        select.addSymbol(new MultipleElementSymbol());
        query.setSelect(select);

        group.setMetadataID(METADATA.getGroupID("pm4.g2")); //$NON-NLS-1$

        PlanNode n1 = NodeFactory.getNewNode(NodeConstants.Types.ACCESS);
        n1.setProperty(NodeConstants.Info.ATOMIC_REQUEST, query);
        n1.addGroup(group);

        RulePlaceAccess.addAccessPatternsProperty(n1, METADATA);

        Collection accessPatterns = (Collection)n1.getProperty(NodeConstants.Info.ACCESS_PATTERNS);
        assertNotNull(accessPatterns);
        assertTrue("Expected two access patterns, got " + accessPatterns.size(), accessPatterns.size() == 2); //$NON-NLS-1$
    }


}
