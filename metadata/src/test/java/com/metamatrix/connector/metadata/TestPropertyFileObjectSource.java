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

package com.metamatrix.connector.metadata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.teiid.connector.metadata.PropertyFileObjectSource;

import junit.framework.TestCase;
import com.metamatrix.core.MetaMatrixRuntimeException;

public class TestPropertyFileObjectSource extends TestCase {

    public TestPropertyFileObjectSource(String name) {
        super(name);
    }

    public void testNoneExistentFile() {
        PropertyFileObjectSource source = new PropertyFileObjectSource(null);
        String fileName = "fakeFiles/fake_does_not_exist.properties"; //$NON-NLS-1$
        try {
            source.getObjects(fileName, null);
            fail();
        } catch (MetaMatrixRuntimeException e) {
            assertEquals(fileName + " file not found", e.getMessage()); //$NON-NLS-1$ //$NON-NLS-2$
        }
    }

    public void testNoProperties() {
        PropertyFileObjectSource source = new PropertyFileObjectSource(null);
        Iterator iterator = source.getObjects("fakeFiles/fake0.properties", null).iterator(); //$NON-NLS-1$
        assertFalse(iterator.hasNext());
    }

    public void testOneProperty() {
        PropertyFileObjectSource source = new PropertyFileObjectSource(null);
        Iterator iterator = source.getObjects("fakeFiles/fake1.properties", null).iterator(); //$NON-NLS-1$
        assertTrue(iterator.hasNext());
        Map.Entry next = (Map.Entry) iterator.next();
        assertEquals(new Integer(1), next.getKey());
        assertEquals("value1", (String)next.getValue()); //$NON-NLS-1$
        assertFalse(iterator.hasNext());
    }

    public void testTwoProperties() {
        PropertyFileObjectSource source = new PropertyFileObjectSource(null);
        Iterator iterator = source.getObjects("fakeFiles/fake2.properties", null).iterator(); //$NON-NLS-1$
        assertTrue(iterator.hasNext());
        Map.Entry next = (Map.Entry) iterator.next();
        Integer key1 = (Integer) next.getKey();
        if (key1.equals(new Integer(1))) {
            assertEquals(new Integer(1), next.getKey());
            assertEquals("value1", (String)next.getValue()); //$NON-NLS-1$
            assertTrue(iterator.hasNext());
            next = (Map.Entry) iterator.next();
            assertEquals(new Integer(2), next.getKey());
            assertEquals("value2", (String)next.getValue()); //$NON-NLS-1$
            assertFalse(iterator.hasNext());
        } else if (key1.equals(new Integer(2))){
            assertEquals(new Integer(2), next.getKey());
            assertEquals("value2", (String)next.getValue()); //$NON-NLS-1$
            assertTrue(iterator.hasNext());
            next = (Map.Entry) iterator.next();
            assertEquals(new Integer(1), next.getKey());
            assertEquals("value1", (String)next.getValue()); //$NON-NLS-1$
            assertFalse(iterator.hasNext());
        } else {
            fail();
        }
    }

    public void testCriteria() {
        PropertyFileObjectSource source = new PropertyFileObjectSource(null);
        try {
            source.getObjects("fakeFiles/fake1.properties", helpGetCriteria()); //$NON-NLS-1$
            fail();
        } catch (UnsupportedOperationException e) {
            assertEquals("Criteria is not supported", e.getMessage()); //$NON-NLS-1$
        }
    }

    private Map helpGetCriteria() {
        Map result = new HashMap();
        result.put("key", "key1"); //$NON-NLS-1$ //$NON-NLS-2$
        return result;
    }
}
