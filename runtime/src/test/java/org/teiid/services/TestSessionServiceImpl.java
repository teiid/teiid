package org.teiid.services;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Properties;

import javax.security.auth.login.LoginException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.deployers.VDBRepository;
import org.teiid.dqp.service.SessionServiceException;
import org.teiid.net.TeiidURL;
import org.teiid.security.Credentials;

@SuppressWarnings("nls")
public class TestSessionServiceImpl {
	SessionServiceImpl ssi;
	@Before
	public void setup() {
		ssi = new SessionServiceImpl() {

			@Override
			protected TeiidLoginContext authenticate(String userName,
					Credentials credentials, String applicationName,
					List<String> domains)
					throws LoginException {
				return new TeiidLoginContext(userName, null, domains.iterator().next(), null);
			}
		};
	}
	
	@Test
	public void testActiveVDBWithNoVersion() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("name");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		
		
		Mockito.stub(repo.getLiveVDB("name")).toReturn(vdb);
		
		ssi.setVDBRepository(repo);
		
		ssi.getActiveVDB("name", null);
		
		Mockito.verify(repo, Mockito.times(1)).getLiveVDB("name");
	}
	
	@Test
	public void testActiveVDBWithVersion() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("name");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		
		
		Mockito.stub(repo.getLiveVDB("name", 1)).toReturn(vdb);
		
		ssi.setVDBRepository(repo);
		
		ssi.getActiveVDB("name", "1");
		
		Mockito.verify(repo, Mockito.times(1)).getLiveVDB("name", 1);
	}
	
	
	@Test
	public void testActiveVDBNameWithVersion() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("name");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		
		
		Mockito.stub(repo.getLiveVDB("name", 1)).toReturn(vdb);
		
		ssi.setVDBRepository(repo);
		
		ssi.getActiveVDB("name.1", null);
		
		Mockito.verify(repo, Mockito.times(1)).getLiveVDB("name", 1);
	}
	
	@Test
	public void testActiveVDBNameWithVersionNonInteger() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("name");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		
		
		Mockito.stub(repo.getLiveVDB("name", 1)).toReturn(vdb);
		
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
		
		
		Mockito.stub(repo.getLiveVDB("name", 1)).toReturn(vdb);
		
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
	
	@Test public void testSecurityDomain() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("name");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		vdb.addProperty(SessionServiceImpl.SECURITY_DOMAIN_PROPERTY, "domain");
		
		Mockito.stub(repo.getLiveVDB("name", 1)).toReturn(vdb);
		
		ssi.setVDBRepository(repo);
		
		Properties properties = new Properties();
		properties.setProperty(TeiidURL.JDBC.VDB_NAME, "name.1");
		SessionMetadata s = ssi.createSession("x", new Credentials(new char[] {'y'}), "z", properties, true);
		assertEquals("domain", s.getSecurityDomain());
	}	
	
    @Test public void testBaseUsername() throws Exception {
        
        assertEquals("foo@bar.com", SessionServiceImpl.getBaseUsername("foo\\@bar.com@foo")); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals("foo", SessionServiceImpl.getDomainName("me\\@bar.com@foo")); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals(null, SessionServiceImpl.getDomainName("@")); //$NON-NLS-1$
        
        assertEquals("@", SessionServiceImpl.getBaseUsername("@")); //$NON-NLS-1$ //$NON-NLS-2$
    }	
}
