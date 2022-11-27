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

package org.teiid.query.sql.lang;

import junit.framework.TestCase;

import org.teiid.query.sql.symbol.Constant;
import org.teiid.query.sql.symbol.Reference;


/**
 * @since 4.3
 */
public class TestLimit extends TestCase {

    public void testGetOffset() {
        Limit limit = new Limit(new Constant(new Integer(50)), new Constant(new Integer(100)));
        assertEquals(new Constant(new Integer(50)), limit.getOffset());
        assertEquals(new Constant(new Integer(50)), limit.getOffset()); // Idempotent
    }

    public void testGetRowLimit() {
        Limit limit = new Limit(new Constant(new Integer(10)), new Constant(new Integer(30)));
        assertEquals(new Constant(new Integer(30)), limit.getRowLimit());
        assertEquals(new Constant(new Integer(30)), limit.getRowLimit());
    }

    public void testEquals() {
        Limit limit1 = new Limit(new Constant(new Integer(100)), new Constant(new Integer(50)));
        Limit limit2 = new Limit(new Constant(new Integer(100)), new Constant(new Integer(50)));
        Limit limit3 = new Limit(new Constant(new Integer(99)), new Constant(new Integer(50)));
        Limit limit4 = new Limit(new Constant(new Integer(100)), new Constant(new Integer(49)));
        Limit limit5 = new Limit(null, new Constant(new Integer(50)));
        Limit limit6 = new Limit(new Constant(new Integer(0)), new Constant(new Integer(50)));
        Limit limit7 = new Limit(new Reference(0), new Constant(new Integer(50)));
        Limit limit8 = new Limit(new Reference(0), new Reference(1));
        Limit limit9 = new Limit(null, new Reference(1));
        assertTrue(limit1.equals(limit2));
        assertTrue(limit2.equals(limit1));
        assertTrue(limit1.equals(limit1));
        assertFalse(limit1.equals(null));
        assertFalse(limit1.equals(limit3));
        assertFalse(limit1.equals(limit4));
        assertFalse(limit6.equals(limit7));
        assertFalse(limit7.equals(limit6));
        assertFalse(limit5.equals(limit7));
        assertFalse(limit7.equals(limit5));
        assertFalse(limit7.equals(limit8));
        assertFalse(limit8.equals(limit9));
        assertFalse(limit5.equals(limit9));
    }

    public void testHashcode() {
        // Ensure that the hashcode for offset = null and offset = 0 are the same.
        Limit limit1 = new Limit(null, new Constant(new Integer(50)));
        Limit limit2 = new Limit(new Constant(new Integer(0)), new Constant(new Integer(50)));
        assertEquals(limit1.hashCode(), limit2.hashCode());
    }

    public void testClone() {
        Limit limit = new Limit(new Constant(new Integer(100)), new Constant(new Integer(50)));
        Limit clone = (Limit)limit.clone();
        assertTrue(limit != clone);
        assertTrue(limit.equals(clone));
        assertEquals(new Constant(new Integer(100)), clone.getOffset());
        assertEquals(new Constant(new Integer(50)), clone.getRowLimit());
    }

    public void testToString() {
        assertEquals("LIMIT 50", new Limit(null, new Constant(new Integer(50))).toString()); //$NON-NLS-1$
        assertEquals("LIMIT 100, 50", new Limit(new Constant(new Integer(100)), new Constant(new Integer(50))).toString()); //$NON-NLS-1$
        assertEquals("LIMIT ?, ?", new Limit(new Reference(0), new Reference(1)).toString()); //$NON-NLS-1$
        assertEquals("LIMIT -1, ?", new Limit(new Constant(new Integer(-1)), new Reference(1)).toString()); //$NON-NLS-1$
    }

}
