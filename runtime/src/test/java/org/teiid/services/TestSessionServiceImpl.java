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
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.client.security.InvalidSessionException;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.service.SessionServiceException;
import org.teiid.security.Credentials;
import org.teiid.security.SecurityHelper;
import org.teiid.services.TeiidLoginContext;
import org.teiid.services.SessionServiceImpl;

@SuppressWarnings("nls")
public class TestSessionServiceImpl {
	
	public void validateSession(boolean securityEnabled) throws Exception {
		final TeiidLoginContext impl =  Mockito.mock(TeiidLoginContext.class);
		Mockito.stub(impl.getUserName()).toReturn("steve@somedomain");
		Mockito.stub(impl.getLoginContext()).toReturn(Mockito.mock(LoginContext.class));
		final ArrayList<String> domains = new ArrayList<String>();
		domains.add("somedomain");				

		SessionServiceImpl ssi = new SessionServiceImpl() {
			@Override
			protected TeiidLoginContext authenticate(String userName, Credentials credentials, String applicationName, List<String> domains, SecurityHelper helper, boolean passthough)
				throws LoginException {
				impl.authenticateUser(userName, credentials, applicationName, domains, passthough);
				return impl;
			}
		};
	
		ssi.setSecurityDomains("somedomain");
		
		try {
			ssi.validateSession(String.valueOf(1));
			fail("exception expected"); //$NON-NLS-1$
		} catch (InvalidSessionException e) {
			
		}
		
		SessionMetadata info = ssi.createSession("steve", null, "foo", new Properties(), false, true); //$NON-NLS-1$ //$NON-NLS-2$
		if (securityEnabled) {
			Mockito.verify(impl).authenticateUser("steve", null, "foo", domains, false); 
		}
		
		String id1 = info.getSessionId();
		ssi.validateSession(id1);
		
		assertEquals(1, ssi.getActiveSessionsCount());
		assertEquals(0, ssi.getSessionsLoggedInToVDB("a", 1).size()); //$NON-NLS-1$ 
		
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
	
	@Test
	public void testActiveVDBWithNoVersion() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("name");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		
		
		Mockito.stub(repo.getVDB("name")).toReturn(vdb);
		
		SessionServiceImpl ssi = new SessionServiceImpl();
		ssi.setVDBRepository(repo);
		
		ssi.getActiveVDB("name", null);
		
		Mockito.verify(repo, Mockito.times(1)).getVDB("name");
	}
	
	@Test
	public void testActiveVDBWithVersion() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("name");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		
		
		Mockito.stub(repo.getVDB("name", 1)).toReturn(vdb);
		
		SessionServiceImpl ssi = new SessionServiceImpl();
		ssi.setVDBRepository(repo);
		
		ssi.getActiveVDB("name", "1");
		
		Mockito.verify(repo, Mockito.times(1)).getVDB("name", 1);
	}
	
	
	@Test
	public void testActiveVDBNameWithVersion() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("name");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		
		
		Mockito.stub(repo.getVDB("name", 1)).toReturn(vdb);
		
		SessionServiceImpl ssi = new SessionServiceImpl();
		ssi.setVDBRepository(repo);
		
		ssi.getActiveVDB("name.1", null);
		
		Mockito.verify(repo, Mockito.times(1)).getVDB("name", 1);
	}
	
	@Test
	public void testActiveVDBNameWithVersionNonInteger() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("name");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		
		
		Mockito.stub(repo.getVDB("name", 1)).toReturn(vdb);
		
		SessionServiceImpl ssi = new SessionServiceImpl();
		ssi.setVDBRepository(repo);
		
		try {
			ssi.getActiveVDB("name.x", null);
			fail("must have failed with non integer version");
		} catch (SessionServiceException e) {
		}
	}
	
	@Test
	public void testActiveVDBNameWithVersionAndVersion() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("name");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		
		
		Mockito.stub(repo.getVDB("name", 1)).toReturn(vdb);
		
		SessionServiceImpl ssi = new SessionServiceImpl();
		ssi.setVDBRepository(repo);
		
		try {
			ssi.getActiveVDB("name.1", "1");
			fail("must have failed with ambigious version info");
		} catch (SessionServiceException e) {
		}
		
		try {
			ssi.getActiveVDB("name..1", null);
			fail("must have failed with ambigious version info");
		} catch (SessionServiceException e) {
		}
	}	
}
