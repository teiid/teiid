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

import java.util.Iterator;
import java.util.List;

import junit.framework.TestCase;

import com.metamatrix.data.language.ICompareCriteria;
import com.metamatrix.query.sql.lang.CompareCriteria;
import com.metamatrix.query.sql.lang.Update;
import com.metamatrix.query.sql.symbol.Constant;
import com.metamatrix.query.sql.symbol.GroupSymbol;

public class TestUpdateImpl extends TestCase {

    /**
     * Constructor for TestUpdateImpl.
     * @param name
     */
    public TestUpdateImpl(String name) {
        super(name);
    }
    
    public static Update helpExample() {
        GroupSymbol group = TestGroupImpl.helpExample("vm1.g1"); //$NON-NLS-1$
        Update result = new Update();
        result.setGroup(group);
        result.addChange(TestElementImpl.helpExample("vm1.g1", "e1"), new Constant(new Integer(1)));
        result.addChange(TestElementImpl.helpExample("vm1.g1", "e2"), new Constant(new Integer(1)));
        result.addChange(TestElementImpl.helpExample("vm1.g1", "e3"), new Constant(new Integer(1)));
        result.addChange(TestElementImpl.helpExample("vm1.g1", "e4"), new Constant(new Integer(1)));
        result.setCriteria(new CompareCriteria(new Constant(new Integer(1)), CompareCriteria.EQ, new Constant(new Integer(1))));
        return result;
    }
    
    public static UpdateImpl example() throws Exception {
        return (UpdateImpl)TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetGroup() throws Exception {
        assertNotNull(example().getGroup());
    }

    public void testGetChanges() throws Exception {
        List changes = example().getChanges();
        assertNotNull(changes);
        assertEquals(4, changes.size());
        for (Iterator i = changes.iterator(); i.hasNext();) {
            assertTrue(i.next() instanceof ICompareCriteria);
        }
    }

    public void testGetCriteria() throws Exception {
        assertNotNull(example().getCriteria());
    }

}
