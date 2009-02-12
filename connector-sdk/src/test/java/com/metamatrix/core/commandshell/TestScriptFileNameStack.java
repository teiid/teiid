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

import java.io.File;
import junit.framework.TestCase;

public class TestScriptFileNameStack extends TestCase {
    ScriptFileNameStack names = new ScriptFileNameStack();

    public void testCurrentDefaultSetRelativeDirectory() {
        names.setDefaultScriptFileName("a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals("a" + File.separator + "b" + File.separator + "c", names.getUnexpandedCurrentScriptFileName()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testCurrentDefaultSetAbsoluteDirectory() {
        names.setDefaultScriptFileName(File.separator+"a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals(File.separator + "a" + File.separator + "b" + File.separator + "c", names.getUnexpandedCurrentScriptFileName()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    ////

    public void testCurrentDefaultWithExecutingScript() {
        names.setDefaultScriptFileName("a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        names.startingScriptFromFile("d"); //$NON-NLS-1$
        assertEquals("d", names.getUnexpandedCurrentScriptFileName()); //$NON-NLS-1$
    }

    public void testCurrentDefaultSetWithExecutingScript() {
        names.setDefaultScriptFileName(File.separator+"a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        names.startingScriptFromFile("d"); //$NON-NLS-1$
        assertEquals("d", names.getUnexpandedCurrentScriptFileName()); //$NON-NLS-1$
    }

    public void testCurrentDefaultWithExecutingRelativeScript() {
        names.setDefaultScriptFileName("a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        names.startingScriptFromFile("d"+File.separator+"e"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("e", names.getUnexpandedCurrentScriptFileName()); //$NON-NLS-1$
    }

    public void testCurrentDefaultSetWithExecutingAbsoluteScript() {
        names.setDefaultScriptFileName(File.separator+"a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        names.startingScriptFromFile(File.separator+"d"+File.separator+"e"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("e", names.getUnexpandedCurrentScriptFileName()); //$NON-NLS-1$
    }

    ////

    public void testExpandWithExecutingScript() {
        names.setDefaultScriptFileName("a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        names.startingScriptFromFile("d"); //$NON-NLS-1$
        assertEquals("e", names.expandScriptFileName("e")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testExpandWithExecutingScript2() {
        names.setDefaultScriptFileName(File.separator+"a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        names.startingScriptFromFile("d"); //$NON-NLS-1$
        assertEquals("e", names.expandScriptFileName("e")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testExpandWithExecutingRelativeScript() {
        names.setDefaultScriptFileName("a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        names.startingScriptFromFile("d"+File.separator+"e"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("d" + File.separator + "f", names.expandScriptFileName("f")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testExpandWithExecutingAbsoluteScript() {
        names.setDefaultScriptFileName(File.separator+"a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        names.startingScriptFromFile(File.separator+"d"+File.separator+"e"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals(File.separator + "d" + File.separator + "f", names.expandScriptFileName("f")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testExpandWithExecutingBaseScript() {
        names.setDefaultScriptFileName("a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        names.startingScriptFromFile("d"+File.separator+"e"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("d" + File.separator + "f", names.expandScriptFileName("f")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testExpandWithExecutingScriptStack() {
        names.setDefaultScriptFileName("a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        names.startingScriptFromFile("d"+File.separator+"e"); //$NON-NLS-1$ //$NON-NLS-2$
        names.startingScriptFromFile("f"+File.separator+"g"); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("f" + File.separator + "h", names.expandScriptFileName("h")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testExpandWithExecutingScriptStackExpand() {
        names.setDefaultScriptFileName("a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        names.startingScriptFromFile(names.expandScriptFileName("d"+File.separator+"e")); //$NON-NLS-1$ //$NON-NLS-2$
        names.startingScriptFromFile(names.expandScriptFileName("f"+File.separator+"g")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("d" + File.separator + "f" + File.separator + "h", names.expandScriptFileName("h")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testExpandWithExecutingScriptStackExpand2() {
        names.setDefaultScriptFileName("a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        names.startingScriptFromFile(names.expandScriptFileName("a"+File.separator+"b"+File.separator+"c")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        names.startingScriptFromFile(names.expandScriptFileName("d"+File.separator+"e")); //$NON-NLS-1$ //$NON-NLS-2$
        names.startingScriptFromFile(names.expandScriptFileName("f"+File.separator+"g")); //$NON-NLS-1$ //$NON-NLS-2$
        assertEquals("a" + File.separator + "b" + File.separator + "d" + File.separator + "f" + File.separator + "h", names.expandScriptFileName("h")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
    }

    public void testExpandWithExecutingScriptStackRelative() {
        names.setDefaultScriptFileName("a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        names.startingScriptFromFile("d"+File.separator+"e"); //$NON-NLS-1$ //$NON-NLS-2$
        names.startingScriptFromFile("d"+File.separator+"f"+File.separator+"g"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals("d" + File.separator + "f" + File.separator + "h", names.expandScriptFileName("h")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    ////

    public void testExpandFromRelativeDirectory() {
        names.setDefaultScriptFileName("a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals("d", names.expandScriptFileName("d")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testExpandFromAbsoluteDirectory() {
        names.setDefaultScriptFileName(File.separator+"a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals("d", names.expandScriptFileName("d")); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testExpandWithoutReferencesingBaseDirectoryAbsolute() {
        names.setDefaultScriptFileName(File.separator+"a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals(File.separator + "d" + File.separator + "e", names.expandScriptFileName(File.separator + "d" + File.separator + "e")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testExpandWithoutReferencesingBaseDirectoryRelative() {
        names.setDefaultScriptFileName(File.separator+"a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        assertEquals("d" + File.separator + "e", names.expandScriptFileName("d" + File.separator + "e")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
    }

    public void testHasBaseDefaultBeenSet() {
        assertFalse(names.hasDefaultScriptFileBeenSet());
        names.setDefaultScriptFileName("a"); //$NON-NLS-1$
        assertTrue(names.hasDefaultScriptFileBeenSet());
    }

    public void testSettingDefaultByUsing() {
        names.usingScriptFile("a"); //$NON-NLS-1$
        assertTrue(names.hasDefaultScriptFileBeenSet());
    }

    public void testUsingIgnoredAfterSetDefaultCalled() {
        names.setDefaultScriptFileName("a"); //$NON-NLS-1$
        names.usingScriptFile("b"); //$NON-NLS-1$
        assertEquals("a", names.getUnexpandedCurrentScriptFileName()); //$NON-NLS-1$
    }

    public void testExecutingScriptAsStack() {
        names.setDefaultScriptFileName(File.separator+"a"+File.separator+"b"+File.separator+"c"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        names.startingScriptFromFile(File.separator+"d"+File.separator+"e"); //$NON-NLS-1$ //$NON-NLS-2$
        names.startingScriptFromFile(File.separator+"f"+File.separator+"g"); //$NON-NLS-1$ //$NON-NLS-2$
        names.startingScriptFromFile(File.separator+"h"+File.separator+"i"); //$NON-NLS-1$ //$NON-NLS-2$
        names.finishedScript();
        assertEquals("g", names.getUnexpandedCurrentScriptFileName()); //$NON-NLS-1$
    }

    public void testClone() throws CloneNotSupportedException {
        names.startingScriptFromFile("a"); //$NON-NLS-1$
        names.startingScriptFromFile("b"); //$NON-NLS-1$
        ScriptFileNameStack names2 = (ScriptFileNameStack) names.clone();
        names.finishedScript();
        assertEquals("b", names2.getUnexpandedCurrentScriptFileName()); //$NON-NLS-1$
    }

}
