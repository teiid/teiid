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

package com.metamatrix.common.xa;

import junit.framework.TestCase;

public class TestMMXid extends TestCase {
    private static final MMXid XID1 = new MMXid(0, new byte[] {
        1
    }, new byte[0]);
    private static final MMXid XID2 = new MMXid(0, new byte[] {
        2
    }, new byte[] {3});
    private static final MMXid XID1Copy = new MMXid(0, new byte[] {
        1
    }, new byte[0]);

    public void testEquals() {
        assertEquals(XID1, XID1Copy);
        assertFalse(XID1.equals(XID2));
    }
    
    public void testCopyConstructor() {
        MMXid xidcopy = new MMXid(XID1);
        assertEquals(XID1Copy, xidcopy);
        assertNotSame(XID1Copy, xidcopy);
    }
    
    public void testHashCode() {
        assertEquals(XID1.hashCode(), XID1Copy.hashCode());
        assertFalse(XID1.hashCode() == XID2.hashCode());
    }
    
    public void testToString() {
        assertEquals(XID1Copy.toString(), XID1.toString());
        assertEquals("MMXid global:1 branch:null format:0", XID1.toString()); //$NON-NLS-1$
        assertEquals("MMXid global:2 branch:3 format:0", XID2.toString()); //$NON-NLS-1$
    }
    
}
