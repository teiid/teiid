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

package org.teiid.query.sql.symbol;


import junit.framework.TestCase;

import org.teiid.query.sql.lang.UnaryFromClause;


/** 
 * @since 4.2
 */
public class TestGroupSymbol extends TestCase {

    /**
     * Constructor for TestGroupSymbol.
     * @param name
     */
    public TestGroupSymbol(String name) {
        super(name);
    }

    public void testIsTempGroupSymbol() {
        GroupSymbol group = new GroupSymbol("g1"); //$NON-NLS-1$
        assertFalse(group.isTempGroupSymbol());
        group = new GroupSymbol("#temp"); //$NON-NLS-1$
        assertTrue(group.isTempGroupSymbol());
    }
    
    public void testIsNotTempGroupSymbol() {
        GroupSymbol group = new GroupSymbol("g1"); //$NON-NLS-1$
        assertFalse(group.isTempGroupSymbol());
        group = new GroupSymbol("temp"); //$NON-NLS-1$
        assertFalse(group.isTempGroupSymbol());
    }
    
    public void testEquality() {
        GroupSymbol group = new GroupSymbol("g1", "a"); //$NON-NLS-1$ //$NON-NLS-2$
        GroupSymbol group1 = new GroupSymbol("g1", "b"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(group, group1);
    }
    
    public void testInequality1() {
        GroupSymbol group = new GroupSymbol("g1", "a"); //$NON-NLS-1$ //$NON-NLS-2$
        GroupSymbol group1 = new GroupSymbol("g1"); //$NON-NLS-1$ 
        assertFalse(new UnaryFromClause(group).equals(new UnaryFromClause(group1)));
    }
    
}
