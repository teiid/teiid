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

package org.teiid.core.util;

import org.teiid.core.util.EquivalenceUtil;

import junit.framework.TestCase;

public class TestEquivalenceUtil extends TestCase {

    /**
     * Constructor for TestEquivalenceUtil.
     * @param name
     */
    public TestEquivalenceUtil(String name) {
        super(name);
    }

    public void testAreEqual() {
        // Test with nulls
        assertTrue(EquivalenceUtil.areEqual(null, null));
        Object obj1 = new Integer(1000);
        assertFalse(EquivalenceUtil.areEqual(obj1, null));
        assertFalse(EquivalenceUtil.areEqual(null, obj1));
        // Reflexive
        assertTrue(EquivalenceUtil.areEqual(obj1, obj1));
        // Symmetric
        Object obj2 = new Integer(1000);
        assertTrue(EquivalenceUtil.areEqual(obj1, obj2));
        assertTrue(EquivalenceUtil.areEqual(obj2, obj1));
        obj2 = "1000"; //$NON-NLS-1$
        assertFalse(EquivalenceUtil.areEqual(obj1, obj2));
        assertFalse(EquivalenceUtil.areEqual(obj2, obj1));
        // Transitive
        obj2 = new Integer(1000);
        Object obj3 = new Integer(1000);
        assertTrue(EquivalenceUtil.areEqual(obj1, obj2));
        assertTrue(EquivalenceUtil.areEqual(obj2, obj3));
        assertTrue(EquivalenceUtil.areEqual(obj1, obj3));
    }

    public void testAreEquivalent() {
        assertTrue(EquivalenceUtil.areEquivalent(null, null));
        // Empty arrays and nulls
        Object[] array1 = new Object[0];
        assertFalse(EquivalenceUtil.areEquivalent(array1, null));
        assertFalse(EquivalenceUtil.areEquivalent(null, array1));
        assertTrue(EquivalenceUtil.areEquivalent(array1, array1));
        Object[] array2 = new Integer[0];
        assertTrue(EquivalenceUtil.areEquivalent(array1, array2));
        
        // Different arrays same length
        array1 = new Integer[] {new Integer(1), new Integer(2), new Integer(3)};
        array2 = new Object[] {new Integer(1), new Integer(2), new Integer(3)};
        assertTrue(EquivalenceUtil.areEquivalent(array1, array2));
        
        // Different arrays, differing lengths
        array2 = new Object[] {new Integer(1), new Integer(2), new Integer(3), null};
        assertFalse(EquivalenceUtil.areEquivalent(array1, array2));
    }

    /*
     * Test for boolean areStrictlyEquivalent(Object, Object)
     */
    public void testAreStrictlyEquivalentObjectObject() {
        // Equal references
        assertFalse(EquivalenceUtil.areStrictlyEquivalent((Object)null, (Object)null));
        Object obj1 = new Integer(1000);
        assertFalse(EquivalenceUtil.areStrictlyEquivalent(obj1, obj1));
        
        // unequal with null, symmetric
        assertFalse(EquivalenceUtil.areStrictlyEquivalent(obj1, null));
        assertFalse(EquivalenceUtil.areStrictlyEquivalent(null, obj1));
        
        // Equivalent, symmetric
        Object obj2 = new Integer(1000);
        assertTrue(EquivalenceUtil.areStrictlyEquivalent(obj1, obj2));
        assertTrue(EquivalenceUtil.areStrictlyEquivalent(obj2, obj1));
    }

    /*
     * Test for boolean areStrictlyEquivalent(Object[], Object[])
     */
    public void testAreStrictlyEquivalentObjectArrayObjectArray() {
        // Same reference
        assertFalse(EquivalenceUtil.areStrictlyEquivalent((Object[])null, (Object[])null));
        Object[] array1 = new Object[0];
        assertFalse(EquivalenceUtil.areStrictlyEquivalent(array1, array1));
        
        // Different references
        assertTrue(EquivalenceUtil.areStrictlyEquivalent(array1, null));
        assertTrue(EquivalenceUtil.areStrictlyEquivalent(null, array1));
        Object[] array2 = new String[2];
        assertFalse(EquivalenceUtil.areStrictlyEquivalent(array2, null));
        assertFalse(EquivalenceUtil.areStrictlyEquivalent(null, array2));
        
        array2 = new Integer[0];
        assertTrue(EquivalenceUtil.areStrictlyEquivalent(array1, array2));
        assertTrue(EquivalenceUtil.areStrictlyEquivalent(array2, array1));
        
        // Different lengths
        array1 = new Integer[] {new Integer(1), new Integer(2), new Integer(3)};
        array2 = new Object[] {new Integer(1), new Integer(2), new Integer(3), null};
        assertFalse(EquivalenceUtil.areStrictlyEquivalent(array1, array2));
        assertFalse(EquivalenceUtil.areStrictlyEquivalent(array2, array1));
        
        // Different arrays containing the same references
        array2 = new Object[array1.length];
        System.arraycopy(array1, 0, array2, 0, array1.length);
        assertFalse(EquivalenceUtil.areStrictlyEquivalent(array1, array2));
        assertFalse(EquivalenceUtil.areStrictlyEquivalent(array2, array1));
        
        // Different arrays containing different references
        array2 = new Object[] {new Integer(1), new Integer(2), new Integer(3)};
        assertTrue(EquivalenceUtil.areStrictlyEquivalent(array1, array2));
        assertTrue(EquivalenceUtil.areStrictlyEquivalent(array2, array1));
    }

}
