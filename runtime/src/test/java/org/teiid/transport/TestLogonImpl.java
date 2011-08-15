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

package org.teiid.transport;

import java.util.Properties;

import junit.framework.TestCase;

import org.mockito.Mockito;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.client.security.LogonResult;
import org.teiid.client.security.SessionToken;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.service.SessionService;
import org.teiid.net.TeiidURL;
import org.teiid.net.TeiidURL.CONNECTION.AuthenticationType;


public class TestLogonImpl extends TestCase {

	public void testLogonResult() throws Exception {
		SessionService ssi = Mockito.mock(SessionService.class);
		Mockito.stub(ssi.getAuthType()).toReturn(AuthenticationType.CLEARTEXT);
		DQPWorkContext.setWorkContext(new DQPWorkContext());
		String userName = "Fred"; //$NON-NLS-1$
		String applicationName = "test"; //$NON-NLS-1$
		Properties p = new Properties();
		p.setProperty(TeiidURL.CONNECTION.USER_NAME, userName);
		p.setProperty(TeiidURL.CONNECTION.APP_NAME, applicationName);

		SessionMetadata session = new SessionMetadata();
		session.setUserName(userName);
		session.setApplicationName(applicationName);
		session.setSessionId(String.valueOf(1));
		session.setSessionToken(new SessionToken(1, userName));

		Mockito.stub(ssi.createSession(userName, null, applicationName,p, false, true)).toReturn(session);

		LogonImpl impl = new LogonImpl(ssi, "fakeCluster"); //$NON-NLS-1$

		LogonResult result = impl.logon(p);
		assertEquals(userName, result.getUserName());
		assertEquals(String.valueOf(1), result.getSessionID());
	}
	
	
}
