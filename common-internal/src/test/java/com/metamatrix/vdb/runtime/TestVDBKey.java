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

package com.metamatrix.vdb.runtime;

import com.metamatrix.core.util.UnitTestUtil;
import com.metamatrix.vdb.runtime.VDBKey;

import junit.framework.TestCase;

public class TestVDBKey extends TestCase {
    
    public void testCaseInsensitive() {
        VDBKey key = new VDBKey("foo", "1");  //$NON-NLS-1$ //$NON-NLS-2$
        VDBKey key1 = new VDBKey("FOO", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        UnitTestUtil.helpTestEquivalence(0, key, key1);
    }
    
    public void testNotEqual() {
        VDBKey key = new VDBKey("a", "1");  //$NON-NLS-1$ //$NON-NLS-2$
        VDBKey key1 = new VDBKey("b", "1"); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse(key.equals(key1));
    }
    
    public void testNameEndingInNumber() {
        VDBKey key = new VDBKey("a1", "1");  //$NON-NLS-1$ //$NON-NLS-2$
        VDBKey key1 = new VDBKey("a", "11"); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse(key.equals(key1));
    }
    
    public void testDiffertVersion() {
        VDBKey key = new VDBKey("a", "1");  //$NON-NLS-1$ //$NON-NLS-2$
        VDBKey key1 = new VDBKey("a", "11"); //$NON-NLS-1$ //$NON-NLS-2$
        assertFalse(key.equals(key1));
    }

}
