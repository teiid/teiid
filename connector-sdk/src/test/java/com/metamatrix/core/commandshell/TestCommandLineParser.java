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

package com.metamatrix.core.commandshell;

import junit.framework.TestCase;

public class TestCommandLineParser extends TestCase {

    public TestCommandLineParser(String name) {
        super(name);
    }

    public void test() {
        String command = "checkIn samplePath testdata/fakeFile \"Oct 1, 2003\""; //$NON-NLS-1$
        String[] results = new CommandLineParser().parse(command);
        assertEquals("checkIn", results[0]);     //$NON-NLS-1$
        assertEquals("samplePath", results[1]);     //$NON-NLS-1$
        assertEquals("testdata/fakeFile", results[2]);     //$NON-NLS-1$
        assertEquals("Oct 1, 2003", results[3]);     //$NON-NLS-1$
    }
    
    public void testSimple() {
        String command = "getLatest samplePath"; //$NON-NLS-1$
        String[] results = new CommandLineParser().parse(command);
        assertEquals("getLatest", results[0]);     //$NON-NLS-1$
        assertEquals("samplePath", results[1]);     //$NON-NLS-1$
    }
    
    public void testTabsAsWhitespace() {
        String command = "getLatest\tsamplePath"; //$NON-NLS-1$
        String[] results = new CommandLineParser().parse(command);
        assertEquals("getLatest", results[0]);     //$NON-NLS-1$
        assertEquals("samplePath", results[1]);     //$NON-NLS-1$
    }
    
    /*
     * @see TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
    }

}
