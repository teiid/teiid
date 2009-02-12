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

package com.metamatrix.core.factory;

import junit.framework.TestCase;
import com.metamatrix.core.MetaMatrixRuntimeException;

public class TestComponentLoader extends TestCase {

    public TestComponentLoader(String name) {
        super(name);
    }

    public void test() {
        ComponentLoader loader = new ComponentLoader(TestComponentLoader.class.getClassLoader(),
            "fakeScript.bsh"); //$NON-NLS-1$
        String userName = (String) loader.load("UserName"); //$NON-NLS-1$
        assertEquals("sampleUserName", userName); //$NON-NLS-1$
    }

    public void testSet() {
        ComponentLoader loader = new ComponentLoader(TestComponentLoader.class.getClassLoader(),
            "fakeScript.bsh"); //$NON-NLS-1$
        loader.set("userName", "bob"); //$NON-NLS-1$ //$NON-NLS-2$
        String userName = (String) loader.load("UserName"); //$NON-NLS-1$
        assertEquals("bob", userName); //$NON-NLS-1$
    }

    public void testMissingScript() {
        try {
            new ComponentLoader(TestComponentLoader.class.getClassLoader(),
                "missingScript.bsh"); //$NON-NLS-1$
            fail();
        } catch (MetaMatrixRuntimeException e) {
            assertEquals("Resource not found: missingScript.bsh.", e.getMessage()); //$NON-NLS-1$
        }
    }
}
