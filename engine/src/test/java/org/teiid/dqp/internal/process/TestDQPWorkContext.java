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

package org.teiid.dqp.internal.process;

import junit.framework.TestCase;

import com.metamatrix.core.util.UnitTestUtil;

public class TestDQPWorkContext extends TestCase {

	/**
	 * Constructor for TestRequestMessage.
	 * 
	 * @param name
	 */
	public TestDQPWorkContext(String name) {
		super(name);
	}

	public static DQPWorkContext example() {
		DQPWorkContext message = new DQPWorkContext();
		message.getSession().setVDBName("vdbName"); //$NON-NLS-1$
		message.getSession().setVDBVersion(1); 
		message.getSession().setApplicationName("querybuilder"); //$NON-NLS-1$
		message.getSession().setSessionId(String.valueOf(5));
		message.getSession().setUserName("userName"); //$NON-NLS-1$
		return message;
	}

	public void testSerialize() throws Exception {
		DQPWorkContext copy = UnitTestUtil.helpSerialize(example());

		assertEquals("5", copy.getSessionId()); //$NON-NLS-1$
		assertEquals("userName", copy.getUserName()); //$NON-NLS-1$
		assertEquals("vdbName", copy.getVdbName()); //$NON-NLS-1$
		assertEquals(1, copy.getVdbVersion());
		assertEquals("querybuilder", copy.getAppName()); //$NON-NLS-1$
	}

}
