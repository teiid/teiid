package com.metamatrix.platform.security.session.service;

import java.util.Properties;

import junit.framework.TestCase;

import org.mockito.Mockito;

import com.metamatrix.admin.api.exception.security.InvalidSessionException;
import com.metamatrix.cache.FakeCache;
import com.metamatrix.common.id.dbid.spi.InMemoryIDController;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.MetaMatrixSessionInfo;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.membership.service.SuccessfulAuthenticationToken;

public class TestSessionServiceImpl extends TestCase {
	
	public void testValidateSession() throws Exception {
		SessionServiceImpl ssi = new SessionServiceImpl();
		ssi.setIdGenerator(new InMemoryIDController());
		ssi.setSessionCache(new FakeCache<MetaMatrixSessionID, MetaMatrixSessionInfo>("1")); //$NON-NLS-1$
		MembershipServiceInterface msi = Mockito.mock(MembershipServiceInterface.class);
		Mockito.stub(msi.authenticateUser("steve", null, null, "foo")).toReturn(new SuccessfulAuthenticationToken(null, "steve@somedomain")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		ssi.setMembershipService(msi);
		
		MetaMatrixSessionID id1 = new MetaMatrixSessionID(1);
		try {
			ssi.validateSession(id1);
			fail("exception expected"); //$NON-NLS-1$
		} catch (InvalidSessionException e) {
			
		}
		
		MetaMatrixSessionInfo info = ssi.createSession("steve", null, null, "foo", new Properties()); //$NON-NLS-1$ //$NON-NLS-2$
		id1 = info.getSessionID();
		ssi.validateSession(id1);
		
		assertEquals(1, ssi.getActiveSessionsCount());
		assertEquals(0, ssi.getSessionsLoggedInToVDB("a", "1").size()); //$NON-NLS-1$ //$NON-NLS-2$
		
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

}
