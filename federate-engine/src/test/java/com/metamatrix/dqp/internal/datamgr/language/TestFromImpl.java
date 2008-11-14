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
import java.util.List;

import com.metamatrix.data.language.IFromItem;
import com.metamatrix.query.sql.lang.From;
import com.metamatrix.query.sql.lang.UnaryFromClause;

import junit.framework.TestCase;

public class TestFromImpl extends TestCase {

    /**
     * Constructor for TestFromImpl.
     * @param name
     */
    public TestFromImpl(String name) {
        super(name);
    }
    
    public static From helpExample() {
        List clauses = new ArrayList();
        clauses.add(new UnaryFromClause(TestGroupImpl.helpExample("vm1.g1"))); //$NON-NLS-1$
        clauses.add(new UnaryFromClause(TestGroupImpl.helpExample("myAlias", "vm1.g2"))); //$NON-NLS-1$ //$NON-NLS-2$
        clauses.add(new UnaryFromClause(TestGroupImpl.helpExample("vm1.g3"))); //$NON-NLS-1$
        clauses.add(new UnaryFromClause(TestGroupImpl.helpExample("vm1.g4"))); //$NON-NLS-1$
        return new From(clauses);
    }
    
    public static FromImpl example() throws Exception {
        return (FromImpl)TstLanguageBridgeFactory.factory.translate(helpExample());
    }

    public void testGetItems() throws Exception {
        FromImpl from = example();
        assertNotNull(from.getItems());
        assertEquals(4, from.getItems().size());
        for (Iterator i = from.getItems().iterator(); i.hasNext();) {
            assertTrue(i.next() instanceof IFromItem);
        }
    }

}
