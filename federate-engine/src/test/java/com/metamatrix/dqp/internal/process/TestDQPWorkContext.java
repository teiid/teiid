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

package com.metamatrix.dqp.internal.process;

import junit.framework.TestCase;

import com.metamatrix.core.util.TestExternalizeUtil;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;

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
		message.setTrustedPayload("myTrustedPayload"); //$NON-NLS-1$
		message.setUserName("userName"); //$NON-NLS-1$
		message.setVdbName("vdbName"); //$NON-NLS-1$
		message.setVdbVersion("vdbVersion"); //$NON-NLS-1$
		message.setAppName("querybuilder");
		message.setSessionId(new MetaMatrixSessionID(5));
		return message;
	}

	public void testSerialize() throws Exception {
		Object serialized = TestExternalizeUtil
				.helpSerializeRoundtrip(example());
		assertNotNull(serialized);
		assertTrue(serialized instanceof DQPWorkContext);
		DQPWorkContext copy = (DQPWorkContext) serialized;

		assertEquals("5", "5"); //$NON-NLS-1$
		assertEquals("myTrustedPayload", copy.getTrustedPayload()); //$NON-NLS-1$
		assertEquals("userName", copy.getUserName()); //$NON-NLS-1$
		assertEquals("vdbName", copy.getVdbName()); //$NON-NLS-1$
		assertEquals("vdbVersion", copy.getVdbVersion()); //$NON-NLS-1$
		assertEquals("querybuilder", copy.getAppName()); //$NON-NLS-1$
	}

}
