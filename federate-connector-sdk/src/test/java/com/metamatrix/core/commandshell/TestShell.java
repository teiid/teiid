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

package com.metamatrix.core.commandshell;

import junit.framework.TestCase;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.util.StringUtilities;

public class TestShell extends TestCase {

    private CommandShell shell;

    public void testInvalidMethodName() {
        assertEquals( "Could not find method 'com.metamatrix.core.commandshell.FakeCommandTarget.foo'.", shell.execute("foo") ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testInvalidMethodNameViaRun() {
        try {
            shell.run(new String[] {"foo"}); //$NON-NLS-1$
        } catch (MetaMatrixRuntimeException e) {
            assertEquals( "Could not find method 'com.metamatrix.core.commandshell.FakeCommandTarget.foo'.", e.getMessage() ); //$NON-NLS-1$
        }
    }

    public void testHelp() {
        assertEquals("checkin String byte[] java.util.Date " //$NON-NLS-1$
        + StringUtilities.LINE_SEPARATOR + "exit " //$NON-NLS-1$
        + StringUtilities.LINE_SEPARATOR + "getLatest String " //$NON-NLS-1$
        + StringUtilities.LINE_SEPARATOR + "getTrace " //$NON-NLS-1$
        + StringUtilities.LINE_SEPARATOR + "help " //$NON-NLS-1$
        + StringUtilities.LINE_SEPARATOR + "method0 String[] " //$NON-NLS-1$
        + StringUtilities.LINE_SEPARATOR + "method1 String int[] " //$NON-NLS-1$
        + StringUtilities.LINE_SEPARATOR + "quit" //$NON-NLS-1$
   , shell.execute("help").trim() ); //$NON-NLS-1$
    }

    public void testExecuteSkipsObjectMethods() {
        assertEquals( "Could not find method 'com.metamatrix.core.commandshell.FakeCommandTarget.hashCode'.", shell.execute("hashCode") ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testExecuteSkipsCommandTargetMethods() {
        assertEquals( "Could not find method 'com.metamatrix.core.commandshell.FakeCommandTarget.runningScript'.", shell.execute("runningScript") ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testExecuteSkipsMethodsToIgnore() {
        assertEquals( "Could not find method 'com.metamatrix.core.commandshell.FakeCommandTarget.methodToIgnore'.", shell.execute("methodToIgnore") ); //$NON-NLS-1$ //$NON-NLS-2$
    }

    /*
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        super.setUp();
        shell = new CommandShell( new FakeCommandTarget() );
    }

}
