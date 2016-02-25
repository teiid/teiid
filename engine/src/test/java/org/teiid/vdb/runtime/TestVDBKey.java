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

package org.teiid.vdb.runtime;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;

public class TestVDBKey {
    
    @Test public void testCaseInsensitive() {
        VDBKey key = new VDBKey("foo", 1);  //$NON-NLS-1$ 
        VDBKey key1 = new VDBKey("FOO", 1); //$NON-NLS-1$ 
        UnitTestUtil.helpTestEquivalence(0, key, key1);
    }
    
    @Test public void testNotEqual() {
        VDBKey key = new VDBKey("a", 1);  //$NON-NLS-1$ 
        VDBKey key1 = new VDBKey("b", 1); //$NON-NLS-1$ 
        assertFalse(key.equals(key1));
    }
    
    @Test public void testNameEndingInNumber() {
        VDBKey key = new VDBKey("a1", 1);  //$NON-NLS-1$ 
        VDBKey key1 = new VDBKey("a", 11); //$NON-NLS-1$ 
        assertFalse(key.equals(key1));
    }
    
    @Test public void testDiffertVersion() {
        VDBKey key = new VDBKey("a", 1);  //$NON-NLS-1$ 
        VDBKey key1 = new VDBKey("a", 11); //$NON-NLS-1$ 
        assertFalse(key.equals(key1));
    }
    
    @Test public void testSemanticVersion() {
        VDBKey key = new VDBKey("a.v1.2.3", 1);  //$NON-NLS-1$ 
        VDBKey key1 = new VDBKey("a.v1.11.3", 1); //$NON-NLS-1$
        assertTrue(key.isSemantic());
        assertTrue(key.isFullySpecified());
        assertFalse(key.isAtMost());
        assertFalse(key.equals(key1));
        assertEquals(-1, key.compareTo(key1));
        assertEquals(1, key1.compareTo(key));
    }
    
    @Test public void testShortSemanticVersion() {
        VDBKey key = new VDBKey("a.v1.3", 1);  //$NON-NLS-1$ 
        VDBKey key1 = new VDBKey("a.v1.2.", 1); //$NON-NLS-1$
        assertTrue(key.isSemantic());
        assertFalse(key.isFullySpecified());
        assertFalse(key.isAtMost());
        assertTrue(key1.isSemantic());
        assertFalse(key1.isFullySpecified());
        assertTrue(key1.isAtMost());
        assertFalse(key.equals(key1));
        assertEquals(1, key.compareTo(key1));
        assertEquals(-1, key1.compareTo(key));
    }
    
}
