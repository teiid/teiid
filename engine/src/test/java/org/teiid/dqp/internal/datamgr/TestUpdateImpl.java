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

import java.util.List;

import junit.framework.TestCase;

import org.teiid.language.Update;
import org.teiid.query.sql.lang.CompareCriteria;
import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.GroupSymbol;


public class TestUpdateImpl extends TestCase {

    /**
     * Constructor for TestUpdateImpl.
     * @param name
     */
    public TestUpdateImpl(String name) {
        super(name);
    }
    
    public static org.teiid.query.sql.lang.Update helpExample() {
        GroupSymbol group = TestGroupImpl.helpExample("vm1.g1"); //$NON-NLS-1$
        org.teiid.query.sql.lang.Update result = new org.teiid.query.sql.lang.Update();
        result.setGroup(group);
        result.addChange(TestElementImpl.helpExample("vm1.g1", "e1"), new Constant(new Integer(1)));
        result.addChange(TestElementImpl.helpExample("vm1.g1", "e2"), new Constant(new Integer(1)));
        result.addChange(TestElementImpl.helpExample("vm1.g1", "e3"), new Constant(new Integer(1)));
        result.addChange(TestElementImpl.helpExample("vm1.g1", "e4"), new Constant(new Integer(1)));
        result.setCriteria(new CompareCriteria(new Constant(new Integer(1)), CompareCriteria.EQ, new Constant(new Integer(1))));
        return result;
    }
    
    public static Update example() throws Exception {
        return TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetGroup() throws Exception {
        assertNotNull(example().getTable());
    }

    public void testGetChanges() throws Exception {
        List changes = example().getChanges();
        assertNotNull(changes);
        assertEquals(4, changes.size());
    }

    public void testGetCriteria() throws Exception {
        assertNotNull(example().getWhere());
    }

}
