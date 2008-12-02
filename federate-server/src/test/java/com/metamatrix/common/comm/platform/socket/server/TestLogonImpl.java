/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
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

package com.metamatrix.common.comm.platform.socket.server;

import java.util.Properties;

import junit.framework.TestCase;

import org.mockito.Mockito;

import com.metamatrix.common.api.MMURL_Properties;
import com.metamatrix.platform.security.api.LogonResult;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.MetaMatrixSessionInfo;
import com.metamatrix.platform.security.api.service.SessionServiceInterface;

public class TestLogonImpl extends TestCase {

	public void testLogonResult() throws Exception {
		SessionServiceInterface ssi = Mockito
				.mock(SessionServiceInterface.class);

		String userName = "Fred"; //$NON-NLS-1$
		String applicationName = "test"; //$NON-NLS-1$
		String productName = "foo"; //$NON-NLS-1$
		Properties p = new Properties();
		p.setProperty(MMURL_Properties.JDBC.USER_NAME, userName);
		p.setProperty(MMURL_Properties.JDBC.APP_NAME, applicationName);
		p.setProperty(MMURL_Properties.CONNECTION.PRODUCT_NAME, productName);

		MetaMatrixSessionInfo resultInfo = new MetaMatrixSessionInfo(
				new MetaMatrixSessionID(1), userName, 0, applicationName, 0,
				"bar", new Properties(), "product", null, null); //$NON-NLS-1$ //$NON-NLS-2$

		Mockito.stub(ssi.createSession(userName, null, null, applicationName,
								productName, p)).toReturn(resultInfo);

		LogonImpl impl = new LogonImpl(ssi);

		LogonResult result = impl.logon(p);
		assertEquals(userName, result.getUserName());
		assertEquals(new MetaMatrixSessionID(1), result.getSessionID());
	}
}
