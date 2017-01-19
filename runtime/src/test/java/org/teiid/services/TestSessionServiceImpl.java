package org.teiid.services;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.SessionMetadata;
import org.teiid.adminapi.impl.VDBMetaData;
import org.teiid.deployers.VDBRepository;
import org.teiid.net.TeiidURL;
import org.teiid.net.socket.AuthenticationType;
import org.teiid.runtime.DoNothingSecurityHelper;
import org.teiid.security.Credentials;

@SuppressWarnings("nls")
public class TestSessionServiceImpl {
	SessionServiceImpl ssi;
	@Before
	public void setup() {
		ssi = new SessionServiceImpl();
		ssi.setSecurityHelper(new DoNothingSecurityHelper());
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
	public void testActiveVDBWithSemanticVersion() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("name.1.2.3");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		
		Mockito.stub(repo.getLiveVDB("name.1.2.3")).toReturn(vdb);
		
		ssi.setVDBRepository(repo);
		
		ssi.getActiveVDB("name.1.2.3", null);
		Mockito.verify(repo, Mockito.times(1)).getLiveVDB("name.1.2.3");
	}
	
	@Test
	public void testActiveVDBWithVersion() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("name");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		
		
		Mockito.stub(repo.getLiveVDB("name", "1")).toReturn(vdb);
		
		ssi.setVDBRepository(repo);
		
		ssi.getActiveVDB("name", "1");
		
		Mockito.verify(repo, Mockito.times(1)).getLiveVDB("name", "1");
	}
	
	
	@Test
	public void testActiveVDBNameWithVersion() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("name");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		
		
		Mockito.stub(repo.getLiveVDB("name", "1")).toReturn(vdb);
		
		ssi.setVDBRepository(repo);
		
		ssi.getActiveVDB("name", "1");
		
		Mockito.verify(repo, Mockito.times(1)).getLiveVDB("name", "1");
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
		
		assertNull(ssi.getActiveVDB("name.x", null));
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
		
		assertNull(ssi.getActiveVDB("name.1", "1"));
		
		assertNull(ssi.getActiveVDB("name..1", null));
	}	
	
	@Test public void testSecurityDomain() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("name");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		vdb.addProperty(SessionServiceImpl.SECURITY_DOMAIN_PROPERTY, "domain");
		
		Mockito.stub(repo.getLiveVDB("name", "1")).toReturn(vdb);
		
		ssi.setVDBRepository(repo);
		
		Properties properties = new Properties();
		properties.setProperty(TeiidURL.JDBC.VDB_NAME, "name.1");
		SessionMetadata s = ssi.createSession("name", "1", AuthenticationType.USERPASSWORD, "x", new Credentials(new char[] {'y'}), "z", properties);
		assertEquals("domain", s.getSecurityDomain());
	}	
	
	@Test public void testLegacySecurityDomain() throws Exception {
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("name");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		
		Mockito.stub(repo.getLiveVDB("name", "1")).toReturn(vdb);
		
		ssi.setVDBRepository(repo);
		ssi.setSecurityDomain("sd");
		Properties properties = new Properties();
		properties.setProperty(TeiidURL.JDBC.VDB_NAME, "name.1");
		SessionMetadata s = ssi.createSession("name", "1", AuthenticationType.USERPASSWORD, "x@sd", new Credentials(new char[] {'y'}), "z", properties);
		assertEquals("sd", s.getSecurityDomain());
		
		s = ssi.createSession("name", "1", AuthenticationType.USERPASSWORD, "x", new Credentials(new char[] {'y'}), "z", properties);
		assertEquals("sd", s.getSecurityDomain());
		
		ssi.setAllowSecurityDomainQualifier(true);
		ssi.setSecurityDomain("sd");
		s = ssi.createSession("name", "1", AuthenticationType.USERPASSWORD, "x@sd", new Credentials(new char[] {'y'}), "z", properties);
		assertEquals("sd", s.getSecurityDomain());
		assertEquals("x", s.getUserName());
		
		ssi.setAllowSecurityDomainQualifier(false);
        s = ssi.createSession("name", "1", AuthenticationType.USERPASSWORD, "x@sd", new Credentials(new char[] {'y'}), "z", properties);
        assertEquals("sd", s.getSecurityDomain());
        assertEquals("x@sd", s.getUserName());
	}
	
	@Test public void testAuthenticationType() throws Exception {
		// this is same as "domain/ANY"
		VDBRepository repo = Mockito.mock(VDBRepository.class);
		VDBMetaData vdb = new VDBMetaData();
		vdb.setName("name");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		vdb.addProperty(SessionServiceImpl.SECURITY_DOMAIN_PROPERTY, "domain");
		vdb.addProperty(SessionServiceImpl.GSS_PATTERN_PROPERTY, "x");
		Mockito.stub(repo.getLiveVDB("name", "1")).toReturn(vdb);
		
		ssi.setVDBRepository(repo);
		ssi.setAuthenticationType(AuthenticationType.USERPASSWORD); // this is transport default

		assertEquals(AuthenticationType.GSS, ssi.getAuthenticationType("name", "1", "x"));
		assertEquals(AuthenticationType.USERPASSWORD, ssi.getAuthenticationType("name", "1", "y"));
		assertEquals(AuthenticationType.USERPASSWORD, ssi.getAuthenticationType("name", "1", "z"));
		
		// testing specific domain, enforcing 
		vdb = new VDBMetaData();
		vdb.setName("name1");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		vdb.addProperty(SessionServiceImpl.SECURITY_DOMAIN_PROPERTY, "domain");
		vdb.addProperty(SessionServiceImpl.AUTHENTICATION_TYPE_PROPERTY, "GSS");
		Mockito.stub(repo.getLiveVDB("name1", "1")).toReturn(vdb);

		assertEquals(AuthenticationType.GSS, ssi.getAuthenticationType("name1", "1", "x"));
		assertEquals(AuthenticationType.GSS, ssi.getAuthenticationType("name1", "1", "y"));
		
		// testing transport default
		vdb = new VDBMetaData();
		vdb.setName("name2");
		vdb.setVersion(1);
		vdb.setStatus(Status.ACTIVE);
		
		Mockito.stub(repo.getLiveVDB("name2", "1")).toReturn(vdb);

		assertEquals(AuthenticationType.USERPASSWORD, ssi.getAuthenticationType("name2", "1", "x"));
		assertEquals(AuthenticationType.USERPASSWORD, ssi.getAuthenticationType("name2", "1", "y"));		
		
		ssi.setAuthenticationType(AuthenticationType.GSS); // this is transport default
		assertEquals(AuthenticationType.GSS, ssi.getAuthenticationType("name2", "1", "x"));
		assertEquals(AuthenticationType.GSS, ssi.getAuthenticationType("name2", "1", "y"));		
		
	}	
	
    @Test public void testBaseUsername() throws Exception {
        
        assertEquals("foo@bar.com", SessionServiceImpl.getBaseUsername("foo\\@bar.com@foo")); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals("foo", SessionServiceImpl.getDomainName("me\\@bar.com@foo")); //$NON-NLS-1$ //$NON-NLS-2$
        
        assertEquals(null, SessionServiceImpl.getDomainName("@")); //$NON-NLS-1$
        
        assertEquals("@", SessionServiceImpl.getBaseUsername("@")); //$NON-NLS-1$ //$NON-NLS-2$
    }	
    
}
