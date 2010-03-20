package org.teiid.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.security.Credentials;
import org.teiid.services.TeiidLoginContext;
import org.teiid.services.SessionServiceImpl;


public class TestSessionServiceImpl {
	
	public void validateSession(boolean securityEnabled) throws Exception {
		final TeiidLoginContext impl =  Mockito.mock(TeiidLoginContext.class);
		Mockito.stub(impl.getUserName()).toReturn("steve@somedomain");
		Mockito.stub(impl.getLoginContext()).toReturn(Mockito.mock(LoginContext.class));
		final ArrayList<String> domains = new ArrayList<String>();
		domains.add("somedomain");				

		SessionServiceImpl ssi = new SessionServiceImpl() {
			@Override
			protected TeiidLoginContext authenticate(String userName, Credentials credentials, String applicationName, List<String> domains)
				throws LoginException {
				impl.authenticateUser(userName, credentials, applicationName, domains);
				return impl;
			}
		};
	
		ssi.setSecurityDomains("somedomain");
		
		try {
			ssi.validateSession(1);
			fail("exception expected"); //$NON-NLS-1$
		} catch (InvalidSessionException e) {
			
		}
		
		SessionMetadata info = ssi.createSession("steve", null, "foo", new Properties(), false); //$NON-NLS-1$ //$NON-NLS-2$
		if (securityEnabled) {
			Mockito.verify(impl).authenticateUser("steve", null, "foo", domains);
		}
		
		long id1 = info.getSessionId();
		ssi.validateSession(id1);
		
		assertEquals(1, ssi.getActiveSessionsCount());
		assertEquals(0, ssi.getSessionsLoggedInToVDB("a", 1).size()); //$NON-NLS-1$ //$NON-NLS-2$
		
		ssi.closeSession(id1);
		
		try {
			ssi.validateSession(id1);
			fail("exception expected"); //$NON-NLS-1$
		} catch (InvalidSessionException e) {
			
		}
		
		try {
			ssi.closeSession(id1);
			fail("exception expected"); //$NON-NLS-1$
		} catch (InvalidSessionException e) {
			
		}
	}
	
	@Test public void testvalidateSession() throws Exception{
		validateSession(true);
	}

	@Test public void testvalidateSession2() throws Exception {
		validateSession(false);
	}
	
}
