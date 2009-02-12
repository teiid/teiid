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
import com.metamatrix.core.util.FileUtil;
import com.metamatrix.core.util.StringUtil;
import com.metamatrix.core.util.UnitTestUtil;

public class TestScriptReader extends TestCase {
    private String script;
    private ScriptReader reader;

    public TestScriptReader() {
        super();
    }

    public void testFirstTestScript() {
        reader.gotoScript("testGetChildrenSortOf"); //$NON-NLS-1$
        assertEquals("add a/b/foo a/fakeFile \"Oct 3, 2003\"", reader.nextCommandLine()); //$NON-NLS-1$ 
		assertEquals("getChildren a 0", reader.nextCommandLine()); //$NON-NLS-1$

    }

    public void testSecondTestScript() {
        reader.gotoScript("testGetChildren"); //$NON-NLS-1$
        assertEquals("add a/b/foo a/fakeFile \"Oct 3, 2003\"", reader.nextCommandLine()); //$NON-NLS-1$ 
		assertEquals("getChildren a 1", reader.nextCommandLine()); //$NON-NLS-1$

    }

    public void testCheckResults() {
        reader.gotoScript("testGetChildrenSortOf"); //$NON-NLS-1$
        assertEquals("add a/b/foo a/fakeFile \"Oct 3, 2003\"", reader.nextCommandLine()); //$NON-NLS-1$
        assertFalse(reader.checkResults());
        assertEquals("getChildren a 0", reader.nextCommandLine()); //$NON-NLS-1$
        assertTrue(reader.checkResults());
    }

    public void testGetExpectedResults() {
        reader.gotoScript("testGetChildrenSortOf"); //$NON-NLS-1$
        assertEquals("add a/b/foo a/fakeFile \"Oct 3, 2003\"", reader.nextCommandLine()); //$NON-NLS-1$ 
		assertEquals("getChildren a 0", reader.nextCommandLine()); //$NON-NLS-1$
        assertTrue(reader.checkResults());
        assertEquals("\t\t??" + StringUtil.LINE_SEPARATOR, reader.getExpectedResults()); //$NON-NLS-1$


    }

    public void testMultipleExpectedResults() {
        reader.gotoScript("testGetChildrenRecursive"); //$NON-NLS-1$
        assertEquals("add a/b/foo a/fakeFile \"Oct 3, 2003\"", reader.nextCommandLine()); //$NON-NLS-1$
        assertTrue(reader.checkResults());
        assertEquals("a" + StringUtil.LINE_SEPARATOR + "b" + StringUtil.LINE_SEPARATOR + "c" + StringUtil.LINE_SEPARATOR, reader.getExpectedResults()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		assertEquals("getChildren a 2", reader.nextCommandLine()); //$NON-NLS-1$
        assertTrue(reader.checkResults());
        assertEquals("x" + StringUtil.LINE_SEPARATOR + "y" + StringUtil.LINE_SEPARATOR + "z" + StringUtil.LINE_SEPARATOR, reader.getExpectedResults()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

    }

    public void testReadPastLastCommand() {
        reader.gotoScript("testGetChildrenRecursive"); //$NON-NLS-1$
        assertTrue(reader.hasMore());
        assertEquals("add a/b/foo a/fakeFile \"Oct 3, 2003\"", reader.nextCommandLine()); //$NON-NLS-1$
        assertTrue(reader.hasMore());
        assertEquals("getChildren a 2", reader.nextCommandLine()); //$NON-NLS-1$
        assertTrue(reader.hasMore());
        reader.getExpectedResults();
        assertFalse(reader.hasMore());
        assertNull(reader.nextCommandLine());
    }

    public void testNotCallingGotoTest() {
        try {
            reader.nextCommandLine();
            fail("Excepted exception."); //$NON-NLS-1$
        } catch (RuntimeException e) {
            assertEquals("Must call gotoTest() first.", e.getMessage()); //$NON-NLS-1$
        }
    }

    public void testGetTestNames() {
        String[] expectedResults = new String[] {
            "testGetChildrenSortOf",      //$NON-NLS-1$
            "testGetChildren",            //$NON-NLS-1$
            "testWithBracesInScript",     //$NON-NLS-1$
            "invalidScript",              //$NON-NLS-1$
            "testGetChildrenRecursive" }; //$NON-NLS-1$
        String[] results = reader.getScriptNames();
        assertEquals(expectedResults.length, results.length);
        assertEquals("testGetChildrenSortOf", results[0]); //$NON-NLS-1$
		assertEquals("testGetChildren", results[1]); //$NON-NLS-1$
		assertEquals("testWithBracesInScript", results[2]); //$NON-NLS-1$
		assertEquals("invalidScript", results[3]); //$NON-NLS-1$
		assertEquals("testGetChildrenRecursive", results[4]); //$NON-NLS-1$
    }

    public void testBracesInScript() {
        reader.gotoScript("testWithBracesInScript"); //$NON-NLS-1$
        assertEquals("run this}", reader.nextCommandLine()); //$NON-NLS-1$
		assertEquals("how about this{", reader.nextCommandLine()); //$NON-NLS-1$
		assertEquals("and this {...}", reader.nextCommandLine()); //$NON-NLS-1$
		assertEquals("and {{{}}}", reader.nextCommandLine()); //$NON-NLS-1$
		assertEquals("and this }}}{{{", reader.nextCommandLine()); //$NON-NLS-1$  
		assertEquals("and [ { } ]", reader.nextCommandLine()); //$NON-NLS-1$
    }

    public void testinvalidScript() {
        reader.gotoScript("invalidScript"); //$NON-NLS-1$
        assertEquals("closing brace must be on a line by itself}", reader.nextCommandLine()); //$NON-NLS-1$
    }

    /*
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        script = new FileUtil(UnitTestUtil.getTestDataPath() + "/fakeScript.txt").read(); //$NON-NLS-1$
        reader = new ScriptReader(script);
    }
}
