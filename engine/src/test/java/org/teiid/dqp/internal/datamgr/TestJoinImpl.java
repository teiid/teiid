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

package org.teiid.dqp.internal.datamgr;

import java.util.ArrayList;

import junit.framework.TestCase;

import org.teiid.language.Comparison;
import org.teiid.language.Join;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.lang.JoinPredicate;
import org.teiid.query.sql.lang.JoinType;
import org.teiid.query.sql.lang.UnaryFromClause;
import org.teiid.query.sql.symbol.ElementSymbol;


public class TestJoinImpl extends TestCase {

    /**
     * Constructor for TestJoinImpl.
     * @param name
     */
    public TestJoinImpl(String name) {
        super(name);
    }

    public static JoinPredicate helpExample(JoinType type) {
        ElementSymbol e1 = TestElementImpl.helpExample("vm1.g1", "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        ElementSymbol e2 = TestElementImpl.helpExample("vm1.g2", "e1"); //$NON-NLS-1$ //$NON-NLS-2$
        ArrayList criteria = new ArrayList();
        criteria.add(new CompareCriteria(e1, CompareCriteria.EQ, e2));
        return new JoinPredicate(new UnaryFromClause(e1.getGroupSymbol()),
                                 new UnaryFromClause(e2.getGroupSymbol()),
                                 type,
                                 criteria);
    }

    public static Join example(JoinType type) throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample(type));
    }

    public void testGetLeftItem() throws Exception {
        assertNotNull(example(JoinType.JOIN_CROSS).getLeftItem());
    }

    public void testGetRightItem() throws Exception {
        assertNotNull(example(JoinType.JOIN_CROSS).getRightItem());
    }

    public void testGetJoinType() throws Exception {
        assertEquals(Join.JoinType.CROSS_JOIN, example(JoinType.JOIN_CROSS).getJoinType());
        assertEquals(Join.JoinType.FULL_OUTER_JOIN, example(JoinType.JOIN_FULL_OUTER).getJoinType());
        assertEquals(Join.JoinType.INNER_JOIN, example(JoinType.JOIN_INNER).getJoinType());
        assertEquals(Join.JoinType.LEFT_OUTER_JOIN, example(JoinType.JOIN_LEFT_OUTER).getJoinType());
        assertEquals(Join.JoinType.RIGHT_OUTER_JOIN, example(JoinType.JOIN_RIGHT_OUTER).getJoinType());
    }

    public void testGetCriteria() throws Exception {
        Join join = example(JoinType.JOIN_INNER);
        assertTrue(join.getCondition() instanceof Comparison);
    }

}
