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

package com.metamatrix.admin.server;

import junit.framework.TestCase;

/**
 * Unit tests of AbstractAdminImpl
 * 
 * @since 4.3
 */
public class TestAbstractAdminImpl extends TestCase {

    public void testIdentifierMatches1() {
        assertFalse(AbstractAdminImpl.identifierMatches("a", "a.b.c")); //$NON-NLS-1$//$NON-NLS-2$
        assertTrue(AbstractAdminImpl.identifierMatches("a*", "a.b.c")); //$NON-NLS-1$//$NON-NLS-2$
        assertTrue(AbstractAdminImpl.identifierMatches("*a*", "a.b.c")); //$NON-NLS-1$//$NON-NLS-2$
        assertTrue(AbstractAdminImpl.identifierMatches("a.*", "a.b.c")); //$NON-NLS-1$//$NON-NLS-2$
        assertFalse(AbstractAdminImpl.identifierMatches("*a", "a.b.c")); //$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches2() {
        assertFalse(AbstractAdminImpl.identifierMatches("b", "a.b.c")); //$NON-NLS-1$//$NON-NLS-2$
        assertFalse(AbstractAdminImpl.identifierMatches("*b", "a.b.c")); //$NON-NLS-1$//$NON-NLS-2$
        assertFalse(AbstractAdminImpl.identifierMatches("b*", "a.b.c")); //$NON-NLS-1$//$NON-NLS-2$
        assertTrue(AbstractAdminImpl.identifierMatches("*b*", "a.b.c")); //$NON-NLS-1$//$NON-NLS-2$
        assertTrue(AbstractAdminImpl.identifierMatches("*.b.*", "a.b.c")); //$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches3() {
        assertFalse(AbstractAdminImpl.identifierMatches("c", "a.b.c")); //$NON-NLS-1$//$NON-NLS-2$
        assertTrue(AbstractAdminImpl.identifierMatches("*c", "a.b.c")); //$NON-NLS-1$//$NON-NLS-2$
        assertTrue(AbstractAdminImpl.identifierMatches("*c*", "a.b.c")); //$NON-NLS-1$//$NON-NLS-2$
        assertTrue(AbstractAdminImpl.identifierMatches("*.c", "a.b.c")); //$NON-NLS-1$//$NON-NLS-2$
        assertFalse(AbstractAdminImpl.identifierMatches("c*", "a.b.c")); //$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches4() {
        assertFalse(AbstractAdminImpl.identifierMatches("a", "car.b.c"));//$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches5() {
        assertFalse(AbstractAdminImpl.identifierMatches("a", "abc.b.c"));//$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches6() {
        assertFalse(AbstractAdminImpl.identifierMatches("a", "bca.b.c"));//$NON-NLS-1$//$NON-NLS-2$ 
    }

    public void testIdentifierMatches7() {
        assertFalse(AbstractAdminImpl.identifierMatches("a", "m.car.b"));//$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches8() {
        assertFalse(AbstractAdminImpl.identifierMatches("a", "m.abc.b"));//$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches9() {
        assertFalse(AbstractAdminImpl.identifierMatches("a", "m.bca.b"));//$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches10() {
        assertFalse(AbstractAdminImpl.identifierMatches("a", "b.c.car."));//$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches11() {
        assertFalse(AbstractAdminImpl.identifierMatches("a", "b.c.abc."));//$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches12() {
        assertFalse(AbstractAdminImpl.identifierMatches("a", "b.c.bca"));//$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches13() {
        assertTrue(AbstractAdminImpl.identifierMatches("*", "a.b.c")); //$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches14() {
        assertTrue(AbstractAdminImpl.identifierMatches("Parts*", "PartsSupplier")); //$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches15() {
        assertTrue(AbstractAdminImpl.identifierMatches("Parts*", "PartsSupplier.1")); //$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches16() {
        assertTrue(AbstractAdminImpl.identifierMatches("Parts*", "PartsSupplier.2")); //$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches17() {
        assertTrue(AbstractAdminImpl.identifierMatches("PartsSupplier.*", "PartsSupplier.1")); //$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches18() {
        assertTrue(AbstractAdminImpl.identifierMatches("PartsSupplier.*", "PartsSupplier.2")); //$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches19() {
        assertTrue(AbstractAdminImpl.identifierMatches("Parts*.*", "PartsSupplier.2")); //$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches20() {
        assertTrue(AbstractAdminImpl.identifierMatches("*Supplier*", "PartsSupplier.2")); //$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches21() {
        assertFalse(AbstractAdminImpl.identifierMatches("PartsSupplier", "PartsSupplier.2")); //$NON-NLS-1$//$NON-NLS-2$
    }

    public void testIdentifierMatches22() {
        assertTrue(AbstractAdminImpl.identifierMatches("PartsSupplier*", "PartsSupplier.2")); //$NON-NLS-1$//$NON-NLS-2$
    }
}
