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

package org.teiid.client.xa;

import org.teiid.client.xa.XidImpl;

import junit.framework.TestCase;

public class TestMMXid extends TestCase {
    private static final XidImpl XID1 = new XidImpl(0, new byte[] {
        1
    }, new byte[0]);
    private static final XidImpl XID2 = new XidImpl(0, new byte[] {
        2
    }, new byte[] {3});
    private static final XidImpl XID1Copy = new XidImpl(0, new byte[] {
        1
    }, new byte[0]);

    public void testEquals() {
        assertEquals(XID1, XID1Copy);
        assertFalse(XID1.equals(XID2));
    }
    
    public void testCopyConstructor() {
        XidImpl xidcopy = new XidImpl(XID1);
        assertEquals(XID1Copy, xidcopy);
        assertNotSame(XID1Copy, xidcopy);
    }
    
    public void testHashCode() {
        assertEquals(XID1.hashCode(), XID1Copy.hashCode());
        assertFalse(XID1.hashCode() == XID2.hashCode());
    }
    
    public void testToString() {
        assertEquals(XID1Copy.toString(), XID1.toString());
        assertEquals("Teiid-Xid global:1 branch:null format:0", XID1.toString()); //$NON-NLS-1$
        assertEquals("Teiid-Xid global:2 branch:3 format:0", XID2.toString()); //$NON-NLS-1$
    }
    
}
