package org.teiid.eclipselink.platform;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.resource.ResourceException;
import javax.resource.cci.ConnectionFactory;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.dqp.internal.datamgr.ConnectorManagerRepository.ConnectorManagerException;
import org.teiid.resource.adapter.file.FileManagedConnectionFactory;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.EmbeddedServer.ConnectionFactoryProvider;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.file.FileExecutionFactory;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.WireProtocol;

public class TestTeiidPlatform {
	
	static EmbeddedServer server;
	static EntityManagerFactory factory;
	
	
	@BeforeClass 
	public static void init() throws VirtualDatabaseException, ConnectorManagerException, TranslatorException, FileNotFoundException, IOException, ResourceException {
		
		server = new EmbeddedServer();
		
		FileExecutionFactory executionFactory = new FileExecutionFactory();
		server.addTranslator("file", executionFactory);
		
		FileManagedConnectionFactory fileManagedconnectionFactory = new FileManagedConnectionFactory();
		String path = TestTeiidPlatform.class.getClassLoader().getResource("file").getPath() ;
		fileManagedconnectionFactory.setParentDirectory("src/test/resources/file");
		ConnectionFactory connectionFactory = fileManagedconnectionFactory.createConnectionFactory();
		ConnectionFactoryProvider<ConnectionFactory> connectionFactoryProvider = new EmbeddedServer.SimpleConnectionFactoryProvider<ConnectionFactory>(connectionFactory);
		server.addConnectionFactoryProvider("java:/marketdata-file", connectionFactoryProvider);
		
		SocketConfiguration s = new SocketConfiguration();
		InetSocketAddress addr = new InetSocketAddress("localhost", 31000);
		s.setBindAddress(addr.getHostName());
		s.setPortNumber(addr.getPort());
		s.setProtocol(WireProtocol.teiid);
		EmbeddedConfiguration config = new EmbeddedConfiguration();
		config.addTransport(s);
		server.start(config);
		
		path = TestTeiidPlatform.class.getClassLoader().getResource("vdb").getPath() + File.separator + "marketdata-vdb.xml" ;
		server.deployVDB(new FileInputStream(new File(path)));
		
		factory = Persistence.createEntityManagerFactory("org.teiid.eclipselink.test");
	}
	
	@Test
	public void testSchema() {
		assertNotNull(server.getSchemaDdl("Marketdata", "Stocks"));
		assertNotNull(server.getSchemaDdl("Marketdata", "SYS"));
		assertNotNull(server.getSchemaDdl("Marketdata", "SYSADMIN"));
	}
	
	@Test
	public void testInit() throws Exception {
		assertNotNull(factory);
		EntityManager em = factory.createEntityManager();
		assertNotNull(em);
		em.close();
	}
	
	@Test
	public void testJPQLQuery() {
		EntityManager em = factory.createEntityManager();
		List list = em.createQuery("SELECT m FROM Marketdata m").getResultList();
		assertNotNull(list);
		assertEquals(10, list.size());
		em.close();
	}
	
	
	
	@AfterClass 
	public static void destory() {
		
		factory.close();
		server.stop();
	}

}
