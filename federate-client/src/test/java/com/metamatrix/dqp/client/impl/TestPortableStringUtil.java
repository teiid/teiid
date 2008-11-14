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

package com.metamatrix.dqp.client.impl;

import junit.framework.TestCase;


/** 
 * @since 4.3
 */
public class TestPortableStringUtil extends TestCase {

    public void testEscapeString() {
        assertEquals("abc\\=def\\;ghi\\=jkl\\|some other \\\\text", PortableStringUtil.escapeString("abc=def;ghi=jkl|some other \\text")); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testUnescapeString() {
        assertEquals("abc=def;ghi=jkl|some other \\text", PortableStringUtil.unescapeString("abc\\=def\\;ghi\\=jkl\\|some other \\\\text")); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    public void testEncode() throws Exception {
        assertEquals("rO0ABXNyABFqYXZhLmxhbmcuSW50ZWdlchLioKT3gYc4AgABSQAFdmFsdWV4cgAQamF2YS5sYW5nLk51bWJlcoaslR0LlOCLAgAAeHAAAAAq", PortableStringUtil.encode(new Integer(42))); //$NON-NLS-1$
    }
    
    public void testDecode() throws Exception {
        assertEquals(new Integer(42), PortableStringUtil.decode("rO0ABXNyABFqYXZhLmxhbmcuSW50ZWdlchLioKT3gYc4AgABSQAFdmFsdWV4cgAQamF2YS5sYW5nLk51bWJlcoaslR0LlOCLAgAAeHAAAAAq")); //$NON-NLS-1$
    }
    
    public void testGetParts() {
        String[] parts = PortableStringUtil.getParts("abc=\\|def;ghi=jkl|some other \\text|item1;item2;item3", '|'); //$NON-NLS-1$
        assertNotNull(parts);
        assertEquals(3, parts.length);
        assertEquals("abc=\\|def;ghi=jkl", parts[0]); //$NON-NLS-1$
        assertEquals("some other \\text", parts[1]); //$NON-NLS-1$
        assertEquals("item1;item2;item3", parts[2]); //$NON-NLS-1$
    }
}
