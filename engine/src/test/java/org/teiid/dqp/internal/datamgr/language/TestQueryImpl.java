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

package org.teiid.dqp.internal.datamgr.language;

import org.teiid.dqp.internal.datamgr.language.QueryImpl;

import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.query.sql.lang.CompoundCriteria;
import com.metamatrix.query.sql.lang.Query;

import junit.framework.TestCase;

public class TestQueryImpl extends TestCase {

    /**
     * Constructor for TestQueryImpl.
     * @param name
     */
    public TestQueryImpl(String name) {
        super(name);
    }

    public static Query helpExample() {
        return new Query(TestSelectImpl.helpExample(true),
                         TestFromImpl.helpExample(),
                         TestCompoundCriteriaImpl.helpExample(CompoundCriteria.AND),
                         TestGroupByImpl.helpExample(),
                         TestCompoundCriteriaImpl.helpExample(CompoundCriteria.AND),
                         TestOrderByImpl.helpExample(),
                         null);
    }
    
    public static QueryImpl example() throws Exception {
        return (QueryImpl)TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetSelect() throws Exception {
        assertNotNull(example().getSelect());
    }

    public void testGetFrom() throws Exception {
        assertNotNull(example().getFrom());
    }

    public void testGetWhere() throws Exception {
        assertNotNull(example().getWhere());
    }

    public void testGetGroupBy() throws Exception {
        assertNotNull(example().getGroupBy());
    }

    public void testGetHaving() throws Exception {
        assertNotNull(example().getHaving());
    }

    public void testGetOrderBy() throws Exception {
        assertNotNull(example().getOrderBy());
    }
    
    public void testGetColumnNames() throws Exception {
        String[] expected = {"e1", "e2", "e3", "e4"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        String[] names = example().getColumnNames();
        assertTrue(EquivalenceUtil.areEquivalent(expected, names));
    }
    
    public void testGetColumnTypes() throws Exception {
        Class[] expected = {String.class, String.class, String.class, String.class};
        Class[] types = example().getColumnTypes();
        assertTrue(EquivalenceUtil.areEquivalent(expected, types));
    }

}
