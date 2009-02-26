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

import java.util.TimeZone;
import junit.framework.TestCase;

import com.metamatrix.common.util.TimestampWithTimezone;
import com.metamatrix.core.util.UnitTestUtil;

public class TestCommand extends TestCase {

    public TestCommand(String name) {
        super(name);
    }

	public void setUp() {
		TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT-06:00")); //$NON-NLS-1$
	}

	public void tearDown() {
		TimestampWithTimezone.resetCalendar(null);
	}

    public void test() throws Exception {
        FakeCommandTarget target = new FakeCommandTarget();
        String commandName = "getLatest"; //$NON-NLS-1$
        String[] args = new String[] { "samplePath" }; //$NON-NLS-1$
        Command command = new Command(target, commandName, args);
        command.execute();
        assertEquals("getLatest samplePath", target.getTrace()); //$NON-NLS-1$
    }

    public void testCaseInsensitiveMethodNames() throws Exception {
        FakeCommandTarget target = new FakeCommandTarget();
        String commandName = "GETlATEST"; //$NON-NLS-1$
        String[] args = new String[] { "samplePath" }; //$NON-NLS-1$
        Command command = new Command(target, commandName, args);
        command.execute();
        assertEquals("getLatest samplePath", target.getTrace()); //$NON-NLS-1$
    }

    public void testArgConversion() throws Exception {
        FakeCommandTarget target = new FakeCommandTarget();
        String commandName = "checkin"; //$NON-NLS-1$
        String[] args = new String[] { "samplePath", UnitTestUtil.getTestDataPath() + "/fakeFile", "2003-10-01:00:00:00" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Command command = new Command(target, commandName, args);
        command.execute();
        assertEquals("checkin samplePath <data> Wed Oct 01 00:00:00 GMT-06:00 2003", target.getTrace()); //$NON-NLS-1$
    }

    public void testArgConversionStringArray() throws Exception {
        FakeCommandTarget target = new FakeCommandTarget();
        String commandName = "method0"; //$NON-NLS-1$
        String[] args = new String[] { "arg1", "arg2" }; //$NON-NLS-1$ //$NON-NLS-2$
        Command command = new Command(target, commandName, args);
        command.execute();
        assertEquals("method0 arg1 arg2 ", target.getTrace()); //$NON-NLS-1$
    }

    public void testArgConversionIntArray() throws Exception {
        FakeCommandTarget target = new FakeCommandTarget();
        String commandName = "method1"; //$NON-NLS-1$
        String[] args = new String[] { "arg1", "1", "2" }; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
        Command command = new Command(target, commandName, args);
        command.execute();
        assertEquals("method1 arg1 1 2 ", target.getTrace()); //$NON-NLS-1$
    }

    public void testCommandParsing() throws Exception {
        FakeCommandTarget target = new FakeCommandTarget();
        String commandLine = "getLatest samplePath"; //$NON-NLS-1$
        Command command = new Command(target, commandLine);
        command.execute();
        assertEquals("getLatest samplePath", target.getTrace()); //$NON-NLS-1$
    }

    public void testCommandParsingDates() throws Exception {
        FakeCommandTarget target = new FakeCommandTarget();
        String commandLine = "checkin samplePath " + UnitTestUtil.getTestDataPath() + "/fakeFile \"2003-10-01:00:00:00\""; //$NON-NLS-1$ //$NON-NLS-2$
        Command command = new Command(target, commandLine);
        command.execute();
        assertEquals("checkin samplePath <data> Wed Oct 01 00:00:00 GMT-06:00 2003", target.getTrace()); //$NON-NLS-1$
    }

    public void testComment() throws Exception {
        FakeCommandTarget target = new FakeCommandTarget();
        String commandLine = "//command"; //$NON-NLS-1$
        Command command = new Command(target, commandLine);
        command.execute();
        assertEquals("", target.getTrace()); //$NON-NLS-1$
    }

}
