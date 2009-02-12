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

package com.metamatrix.common.log.format;

import junit.framework.TestCase;

import com.metamatrix.api.exception.ComponentNotFoundException;
import com.metamatrix.api.exception.MetaMatrixException;
import com.metamatrix.api.exception.MetaMatrixProcessingException;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.core.log.LogMessage;

/**
 *
 */
public class TestLogMessageFormat extends TestCase {

	// ################################## FRAMEWORK ################################

	public TestLogMessageFormat(String name) {
		super(name);
	}

	// ################################## TEST HELPERS ################################

    private static Throwable generateThrowable(){
        Exception e2 = new Exception("Test0"); //$NON-NLS-1$
        MetaMatrixException e = new ComponentNotFoundException(e2, "Test1"); //$NON-NLS-1$
        //e = new MetaMatrixException(e, "Test2");
        MetaMatrixRuntimeException ee = new MetaMatrixRuntimeException(e, "Test3"); //$NON-NLS-1$
        e = new MetaMatrixProcessingException(ee, "Test4"); //$NON-NLS-1$
        return e;
    }

    private static LogMessageFormat getLogMessageFormat(){
//        return new ReadableLogMessageFormat();
        return new DelimitedLogMessageFormat();
    }

	// ################################## ACTUAL TESTS ################################
	
    /**
     */
    public void testLogFormat(){
        String context = "FAKE_CONTEXT"; //$NON-NLS-1$
        int level = 2;
        Throwable e = generateThrowable();
        Object[] msgParts = new Object[]{ "This is a fake log message for testing."}; //$NON-NLS-1$
        LogMessage logMessage = new LogMessage(context, level, e, msgParts);
        LogMessageFormat format = getLogMessageFormat();

        String firstLine = format.formatMessage(logMessage).split("\\n")[0]; //$NON-NLS-1$
        assertTrue(firstLine.endsWith("|HostName|VMName|main|FAKE_CONTEXT|ERROR|This is a fake log message for testing.")); //$NON-NLS-1$
    }
	
}
