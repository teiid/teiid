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

package org.teiid.net.socket;

import java.util.Arrays;

import org.teiid.core.util.UnitTestUtil;
import org.teiid.net.socket.ServiceInvocationStruct;

import junit.framework.TestCase;

@SuppressWarnings("nls")
public class TestServiceInvocationStruct extends TestCase {

    public void testSerialize() throws Exception {
    	ServiceInvocationStruct struct = new ServiceInvocationStruct(new Object[] {new Integer(1), "hello"}, "doSomething", TestServiceInvocationStruct.class); 
    	
        Object serialized = UnitTestUtil.helpSerialize(struct);
        assertNotNull(serialized);
        assertTrue(serialized instanceof ServiceInvocationStruct);
        ServiceInvocationStruct copy = (ServiceInvocationStruct)serialized;
        assertTrue(Arrays.equals(struct.args, copy.args));
        assertEquals(struct.methodName, copy.methodName);
        assertEquals(struct.targetClass, copy.targetClass);
    }
}
