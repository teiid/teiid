package com.metamatrix.platform.security.session.service;

import java.util.Properties;

import org.mockito.Mockito;

import junit.framework.TestCase;

import com.metamatrix.api.exception.security.InvalidSessionException;
import com.metamatrix.api.exception.security.SessionServiceException;
import com.metamatrix.cache.FakeCache;
import com.metamatrix.platform.security.api.MetaMatrixSessionID;
import com.metamatrix.platform.security.api.MetaMatrixSessionInfo;
import com.metamatrix.platform.security.api.service.MembershipServiceInterface;
import com.metamatrix.platform.security.membership.service.SuccessfulAuthenticationToken;

public class TestSessionServiceImpl extends TestCase {
	
	public void testValidateSession() throws Exception {
		SessionServiceImpl ssi = new SessionServiceImpl() {
			@Override
			protected long getUniqueSessionID() throws SessionServiceException {
				return 1; //skip using the dbid generator
			}
		};
		ssi.setClusterName("test"); //$NON-NLS-1$
		ssi.setSessionCache(new FakeCache<MetaMatrixSessionID, MetaMatrixSessionInfo>());
		MembershipServiceInterface msi = Mockito.mock(MembershipServiceInterface.class);
		Mockito.stub(msi.authenticateUser("steve", null, null, "foo")).toReturn(new SuccessfulAuthenticationToken(null, "steve@somedomain")); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		ssi.setMembershipService(msi);
		
		MetaMatrixSessionID id1 = new MetaMatrixSessionID(1);
		try {
			ssi.validateSession(id1);
			fail("exception expected"); //$NON-NLS-1$
		} catch (InvalidSessionException e) {
			
		}
		
		ssi.createSession("steve", null, null, "foo", "test", new Properties()); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
		
		ssi.validateSession(id1);
		
		assertEquals(1, ssi.getActiveSessionsCount());
		assertEquals(0, ssi.getActiveConnectionsCountForProduct("x")); //$NON-NLS-1$
		assertEquals(1, ssi.getActiveConnectionsCountForProduct("test")); //$NON-NLS-1$
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
