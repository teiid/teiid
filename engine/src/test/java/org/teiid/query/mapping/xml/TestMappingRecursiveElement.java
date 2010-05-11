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

package org.teiid.query.mapping.xml;

import org.teiid.query.mapping.xml.MappingNodeConstants;
import org.teiid.query.mapping.xml.MappingRecursiveElement;

import junit.framework.TestCase;


/** 
 * Test class for MappingRecursiveElement
 */
public class TestMappingRecursiveElement extends TestCase {

    public void testRecursive() {
        MappingRecursiveElement element = new MappingRecursiveElement("test", "foo.bar"); //$NON-NLS-1$ //$NON-NLS-2$
        
        // test basics
        assertTrue(element.isRecursive());
        assertEquals("test", element.getName()); //$NON-NLS-1$        
        assertEquals("foo.bar", element.getMappingClass()); //$NON-NLS-1$
        
        // test criteria
        assertNull(element.getCriteria());
        element.setCriteria("foo >= bar"); //$NON-NLS-1$
        assertNotNull(element.getCriteria());
        assertEquals("foo >= bar", element.getCriteria()); //$NON-NLS-1$

        // test recursion depth
        assertEquals(MappingNodeConstants.Defaults.DEFAULT_RECURSION_LIMIT.intValue(), element.getRecursionLimit());
        assertFalse(element.throwExceptionOnRecurrsionLimit());
        element.setRecursionLimit(3, true);
        assertEquals(3, element.getRecursionLimit());
        assertTrue(element.throwExceptionOnRecurrsionLimit());
    }
}
