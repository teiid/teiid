/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.internal.datamgr.language;

import java.util.ArrayList;
import java.util.Iterator;

import com.metamatrix.data.language.ICriteria;
import com.metamatrix.data.language.IJoin;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.JoinPredicate;
import com.metamatrix.query.sql.lang.JoinType;
import com.metamatrix.query.sql.lang.UnaryFromClause;
import com.metamatrix.query.sql.symbol.ElementSymbol;

import junit.framework.TestCase;

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
    
    public static JoinImpl example(JoinType type) throws Exception {
        return (JoinImpl)TstLanguageBridgeFactory.factory.translate(helpExample(type));
    }

    public void testGetLeftItem() throws Exception {
        assertNotNull(example(JoinType.JOIN_CROSS).getLeftItem());
    }

    public void testGetRightItem() throws Exception {
        assertNotNull(example(JoinType.JOIN_CROSS).getRightItem());
    }

    public void testGetJoinType() throws Exception {
        assertEquals(IJoin.CROSS_JOIN, example(JoinType.JOIN_CROSS).getJoinType());
        assertEquals(IJoin.FULL_OUTER_JOIN, example(JoinType.JOIN_FULL_OUTER).getJoinType());
        assertEquals(IJoin.INNER_JOIN, example(JoinType.JOIN_INNER).getJoinType());
        assertEquals(IJoin.LEFT_OUTER_JOIN, example(JoinType.JOIN_LEFT_OUTER).getJoinType());
        assertEquals(IJoin.RIGHT_OUTER_JOIN, example(JoinType.JOIN_RIGHT_OUTER).getJoinType());
    }

    public void testGetCriteria() throws Exception {
        JoinImpl join = example(JoinType.JOIN_INNER);
        assertNotNull(join.getCriteria());
        assertEquals(1, join.getCriteria().size());
        for (Iterator i = join.getCriteria().iterator(); i.hasNext();) {
            assertTrue(i.next() instanceof ICriteria);
        }
    }

}
