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
