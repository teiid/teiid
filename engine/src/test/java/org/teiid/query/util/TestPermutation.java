/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.query.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.teiid.query.util.Permutation;


import junit.framework.TestCase;

/**
 */
public class TestPermutation extends TestCase {

    /**
     * Constructor for TestPermutation.
     * @param arg0
     */
    public TestPermutation(String arg0) {
        super(arg0);
    }

    public Object[] exampleItems(int num) {
        Object[] items = new Object[num];
        for(int i=0; i<items.length; i++) {
            items[i] = "" + i;     //$NON-NLS-1$
        }
        return items;
    }

    public void compareArrays(Object[] a1, Object[] a2) {
        assertEquals("Arrays are of differing lengths", a1.length, a2.length); //$NON-NLS-1$
        for(int i=0; i<a1.length; i++) {
            assertEquals("Arrays have differing object at index " + i, a1[i], a2[i]);             //$NON-NLS-1$
        }
    }

    public void compareOrders(List expected, List actual) {
        assertEquals("Number of orders differs", expected.size(), actual.size()); //$NON-NLS-1$
        for(int i=0; i<expected.size(); i++) {
            compareArrays( (Object[]) expected.get(i), (Object[]) actual.get(i) );
        }
    }

    public void testNull() {
        try {
            new Permutation(null);
            fail("Expected IllegalArgumentException"); //$NON-NLS-1$
        } catch(IllegalArgumentException e) {
        }
    }

    public void test1() {
        Permutation perm = new Permutation(exampleItems(0));
        Iterator iter = perm.generate();
        assertTrue("Should get no permutations for no items", ! iter.hasNext()); //$NON-NLS-1$

        try {
            iter.next();
            fail("Expected NoSuchElementException"); //$NON-NLS-1$
        } catch(NoSuchElementException e) {
        }
    }

    public void test2() {
        Permutation perm = new Permutation(exampleItems(2));
        Iterator iter = perm.generate(0);
        assertTrue("Should get no permutations for no items", ! iter.hasNext()); //$NON-NLS-1$
    }

    public void test3() {
        Permutation perm = new Permutation(exampleItems(1));
        Iterator iter = perm.generate();

        List orders = new ArrayList();
        while(iter.hasNext()) {
            orders.add(iter.next());
        }

        assertEquals("Should get one permutations for one item", 1, orders.size()); //$NON-NLS-1$
        compareArrays(exampleItems(1), (Object[]) orders.get(0));
    }

    public void test4() {
        Permutation perm = new Permutation(exampleItems(2));
        Iterator iter = perm.generate();

        List orders = new ArrayList();
        while(iter.hasNext()) {
            orders.add(iter.next());
        }

        List expected = new ArrayList();
        expected.add(new Object[] { "0", "1" }); //$NON-NLS-1$ //$NON-NLS-2$
        expected.add(new Object[] { "1", "0" }); //$NON-NLS-1$ //$NON-NLS-2$

        compareOrders(expected, orders);

    }

    public void test5() {
        Permutation perm = new Permutation(exampleItems(3));
        Iterator iter = perm.generate();

        List orders = new ArrayList();
        while(iter.hasNext()) {
            orders.add(iter.next());
        }

        List expected = new ArrayList();
        expected.add(new Object[] { "0", "1", "2" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        expected.add(new Object[] { "0", "2", "1" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        expected.add(new Object[] { "1", "0", "2" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        expected.add(new Object[] { "1", "2", "0" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        expected.add(new Object[] { "2", "0", "1" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        expected.add(new Object[] { "2", "1", "0" }); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

        compareOrders(expected, orders);

    }

    public void test6() {
        Permutation perm = new Permutation(exampleItems(3));
        Iterator iter = perm.generate(1);

        List orders = new ArrayList();
        while(iter.hasNext()) {
            orders.add(iter.next());
        }

        List expected = new ArrayList();
        expected.add(new Object[] { "0" }); //$NON-NLS-1$
        expected.add(new Object[] { "1" }); //$NON-NLS-1$
        expected.add(new Object[] { "2" }); //$NON-NLS-1$

        compareOrders(expected, orders);

    }

}
