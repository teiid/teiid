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

import static org.junit.Assert.*;

import java.util.Properties;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.security.ILogon;
import org.teiid.client.security.LogonException;
import org.teiid.client.security.LogonResult;
import org.teiid.client.security.SessionToken;
import org.teiid.core.util.Base64;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.internal.process.DQPWorkContext;
import org.teiid.dqp.service.SessionService;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.security.Credentials;
import org.teiid.security.SecurityHelper;
import org.teiid.services.SessionServiceImpl;
import org.teiid.services.TeiidLoginContext;

@SuppressWarnings("nls")
public class TestLogonImpl {
	SessionServiceImpl ssi;
	
	@Before
	public void setup() {
		ssi = new SessionServiceImpl() {

			@Override
			protected TeiidLoginContext authenticate(String userName,
					Credentials credentials, String applicationName,
					String securityDomain)
					throws LoginException {
				return new TeiidLoginContext(userName, null, securityDomain, null);
			}			
		};
		
		SecurityHelper sc = Mockito.mock(SecurityHelper.class);
		Mockito.stub(sc.getSubjectInContext("SC")).toReturn(new Subject());
		ssi.setSecurityHelper(sc);
	}
	
	@Test
	public void testLogonResult() throws Exception {
		SessionService ssi = Mockito.mock(SessionService.class);
		Mockito.stub(ssi.getAuthenticationType(Mockito.anyString(), Mockito.anyString(), Mockito.anyString())).toReturn(AuthenticationType.USERPASSWORD);
		DQPWorkContext.setWorkContext(new DQPWorkContext());
		String userName = "Fred"; //$NON-NLS-1$
		String applicationName = "test"; //$NON-NLS-1$
		Properties p = new Properties();
		p.setProperty(TeiidURL.CONNECTION.USER_NAME, userName);
		p.setProperty(TeiidURL.CONNECTION.APP_NAME, applicationName);
		p.setProperty(TeiidURL.JDBC.VDB_NAME, "x");
		p.setProperty(TeiidURL.JDBC.VDB_VERSION, "1");
		
		SessionMetadata session = new SessionMetadata();
		session.setUserName(userName);
		session.setApplicationName(applicationName);
		session.setSessionId(String.valueOf(1));
		session.setSessionToken(new SessionToken(1, userName));

		Mockito.stub(ssi.createSession("x", "1", AuthenticationType.USERPASSWORD, userName, null, applicationName,p)).toReturn(session); 

		LogonImpl impl = new LogonImpl(ssi, "fakeCluster"); //$NON-NLS-1$

		LogonResult result = impl.logon(p);
		assertEquals(userName, result.getUserName());
		assertEquals(String.valueOf(1), result.getSessionID());
	}
	
	@Test
	public void testLogonAuthenticationType() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.addProperty(SessionServiceImpl.GSS_PATTERN_PROPERTY, "GSS");
		vdb.setName("name");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		Mockito.stub(repo.getLiveVDB("name", 1)).toReturn(vdb);
		
		ssi.setVDBRepository(repo);
		ssi.setSecurityDomain("SC");
		
		// default transport - what Teiid has before TEIID-2863
		ssi.setAuthenticationType(AuthenticationType.USERPASSWORD); // this is transport default		
		DQPWorkContext.setWorkContext(new DQPWorkContext());
		Properties p = buildProperties("fred", "name");		
		LogonImpl impl = new LogonImpl(ssi, "fakeCluster"); //$NON-NLS-1$
		LogonResult result = impl.logon(p);
		assertEquals("fred", result.getUserName());
		
		// if no preference then choose USERPASSWORD
		ssi.setAuthenticationType(AuthenticationType.USERPASSWORD); // this is transport default		
		DQPWorkContext.setWorkContext(new DQPWorkContext());
		p = buildProperties("fred", "name");		
		impl = new LogonImpl(ssi, "fakeCluster"); //$NON-NLS-1$
		result = impl.logon(p);
		assertEquals("fred", result.getUserName());

		// if user name is set to "GSS", then the preference is set to "GSS"
		ssi.setAuthenticationType(AuthenticationType.USERPASSWORD); // this is transport default		
		DQPWorkContext.setWorkContext(new DQPWorkContext());
		p = buildProperties("GSS", "name");		
		FakeGssLogonImpl fimpl = new FakeGssLogonImpl(ssi, "fakeCluster"); //$NON-NLS-1$
		fimpl.addToken("bytes".getBytes(), "SecurityContext");
		p.put(ILogon.KRB5TOKEN, "bytes".getBytes());
		result = fimpl.logon(p);
		assertEquals("GSS@SC", result.getUserName());
		
		// if the transport default defined as GSS, then preference is USERPASSWORD, throw exception
		ssi.setAuthenticationType(AuthenticationType.GSS); 		
		try {
			DQPWorkContext.setWorkContext(new DQPWorkContext());
			p = buildProperties("fred", "name");		
			impl = new LogonImpl(ssi, "fakeCluster"); //$NON-NLS-1$
			result = impl.logon(p);
			fail("should have failed due server does not support USERPASSWORD");
		} catch(LogonException e) {
			// pass
		}
	}
	
	@Test
	public void testLogonAuthenticationTypeByVDB() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		ssi.setVDBRepository(repo);

		// when VDB value is is avavailble this will not be used
		ssi.setAuthenticationType(AuthenticationType.GSS); 
		
		// default transport - what Teiid has before TEIID-2863
		addVdb(repo, "name", "SC", AuthenticationType.USERPASSWORD.name());		
		DQPWorkContext.setWorkContext(new DQPWorkContext());
		Properties p = buildProperties("fred", "name");		
		LogonImpl impl = new LogonImpl(ssi, "fakeCluster"); //$NON-NLS-1$
		LogonResult result = impl.logon(p);
		assertEquals("fred", result.getUserName());
		
		// if no preference then choose USERPASSWORD
		VDBMetaData metadata = addVdb(repo, "name1", "SC", AuthenticationType.USERPASSWORD.name());
		metadata.addProperty(SessionServiceImpl.GSS_PATTERN_PROPERTY, "GSS");
		DQPWorkContext.setWorkContext(new DQPWorkContext());
		impl = new LogonImpl(ssi, "fakeCluster"); //$NON-NLS-1$
		p = buildProperties("fred", "name1");		
		result = impl.logon(p);
		assertEquals("fred", result.getUserName());

		p = buildProperties("GSS", "name1");
		FakeGssLogonImpl fimpl = new FakeGssLogonImpl(ssi, "fakeCluster"); //$NON-NLS-1$
		fimpl.addToken("bytes".getBytes(), "SecurityContext");
		p.put(ILogon.KRB5TOKEN, "bytes".getBytes());
		result = fimpl.logon(p);
		assertEquals("GSS@SC", result.getUserName());
				
		
		// here preference is GSS
		try {
			p = buildProperties("GSS", "name");		
			result = impl.logon(p);
			assertEquals("GSS", result.getUserName());
		} catch(LogonException e) {
			
		}
				
		// if the transport default defined as GSS, then preference is USERPASSWORD, throw exception
		try {
			addVdb(repo, "name2", "SC", "GSS");		
			DQPWorkContext.setWorkContext(new DQPWorkContext());
			impl = new LogonImpl(ssi, "fakeCluster"); //$NON-NLS-1$
			p = buildProperties("fred", "name2");		
			result = impl.logon(p);
			fail("should have failed due server does not support USERPASSWORD");
		} catch(LogonException e) {
			// pass
		}
	}	

	private Properties buildProperties(String userName, String vdbName) {
		Properties p = new Properties();
		p.setProperty(TeiidURL.CONNECTION.USER_NAME, userName);
		p.setProperty(TeiidURL.CONNECTION.APP_NAME, "test");
		p.setProperty(TeiidURL.JDBC.VDB_NAME, vdbName);
		p.setProperty(TeiidURL.JDBC.VDB_VERSION, "1");
		return p;
	}

	private VDBMetaData addVdb(VDBRepository repo, String name, String sc, String authenticationType) {
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName(name);
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		Mockito.stub(repo.getLiveVDB(name, 1)).toReturn(vdb);
		vdb.addProperty(SessionServiceImpl.SECURITY_DOMAIN_PROPERTY, sc);
		vdb.addProperty(SessionServiceImpl.AUTHENTICATION_TYPE_PROPERTY, authenticationType);
		return vdb;
	}	
	
	class FakeGssLogonImpl extends LogonImpl {

		public FakeGssLogonImpl(SessionService service, String clusterName) {
			super(service, clusterName);
		}

		public void addToken(byte[] token, Object securityContext) {
			this.gssServiceTickets.put(Base64.encodeBytes(MD5(token)), securityContext);
		}
	}
}
