package org.teiid.arquillian;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;

import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ArchivePaths;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.ByteArrayAsset;
import org.jboss.shrinkwrap.api.exporter.ZipExporter;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminFactory;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.Translator;
import org.teiid.adminapi.VDB;
import org.teiid.adminapi.VDB.ConnectionType;
import org.teiid.adminapi.VDB.Status;
import org.teiid.adminapi.impl.VDBTranslatorMetaData;
import org.teiid.core.util.UnitTestUtil;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
@Ignore
public class TestDeployment {

	@Test
	public void testVDBDeployment() throws Exception {
		boolean deployed = false;
		Admin admin = AdminFactory.getInstance().createAdmin("localhost", 9999,	null, null);
		try {
			Set<?> vdbs = admin.getVDBs();
			assertTrue(vdbs.isEmpty());
			
			Collection<String> dsNames = admin.getDataSourceNames();
			if (dsNames.contains("Oracle11_PushDS")) {
				admin.deleteDataSource("Oracle11_PushDS");
			}
			
			admin.deploy("bqt.vdb",new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));
			deployed = true;

			vdbs = admin.getVDBs();
			assertFalse(vdbs.isEmpty());

			VDB vdb = admin.getVDB("bqt", 1);
			assertFalse(vdb.isValid());
			assertTrue(vdb.getStatus().equals(Status.INACTIVE));

			Properties props = new Properties();
			props.setProperty("connection-url","jdbc:h2:mem:test;DB_CLOSE_DELAY=-1");
			props.setProperty("user-name", "sa");
			props.setProperty("password", "sa");
			
			admin.createDataSource("Oracle11_PushDS", "h2", props);
			Thread.sleep(2000);
			vdb = admin.getVDB("bqt", 1);
			assertTrue(vdb.isValid());
			assertTrue(vdb.getStatus().equals(Status.ACTIVE));
			
			dsNames = admin.getDataSourceNames();
			assertTrue(dsNames.contains("Oracle11_PushDS"));

			admin.deleteDataSource("Oracle11_PushDS");
			vdb = admin.getVDB("bqt", 1);
			assertFalse(vdb.isValid());
			assertTrue(vdb.getStatus().equals(Status.INACTIVE));
		} finally {
			if (deployed) {
				admin.undeploy("bqt.vdb");
			}
		}
		
		admin.close();
	}

	@Test
	public void testTraslators() throws Exception {
		Admin admin = AdminFactory.getInstance().createAdmin("localhost", 9999, null, null);
		
		Collection<? extends Translator> translators = admin.getTranslators();
		
		assertEquals(27, translators.size());

		JavaArchive jar = ShrinkWrap.create(JavaArchive.class, "orcl.jar")
				      .addClasses(SampleExecutionFactory.class)
				      .addAsManifestResource(new ByteArrayAsset(SampleExecutionFactory.class.getName().getBytes()),
				            ArchivePaths.create("services/org.teiid.translator.ExecutionFactory"));
		try {
			admin.deploy("orcl.jar", jar.as(ZipExporter.class).exportAsInputStream());
			
			VDBTranslatorMetaData t = (VDBTranslatorMetaData)admin.getTranslator("orcl");
			assertNotNull(t);
			assertEquals("ANY", t.getPropertyValue("SupportedJoinCriteria"));
			assertEquals("true", t.getPropertyValue("supportsSelectDistinct"));
		} finally {
			admin.undeploy("orcl.jar");	
		}

		VDBTranslatorMetaData t = (VDBTranslatorMetaData)admin.getTranslator("orcl");
		assertNull(t);
		
		admin.close();
	}

	@Test
	public void testVDBOperations() throws Exception {
		Admin admin = AdminFactory.getInstance().createAdmin("localhost", 9999,	null, null);
		try {
			admin.deploy("bqt.vdb",new FileInputStream(UnitTestUtil.getTestDataFile("bqt.vdb")));
			
			VDB vdb = admin.getVDB("bqt", 1);
			Model model = vdb.getModels().get(0);
			assertEquals(ConnectionType.BY_VERSION, vdb.getConnectionType());
			
			admin.assignToModel("bqt", 1, model.getName(), "Source", "h2", "java:jboss/datasources/ExampleDS");
			admin.changeVDBConnectionType("bqt", 1, ConnectionType.ANY);
			
			vdb = admin.getVDB("bqt", 1);
			model = vdb.getModels().get(0);
			assertEquals(model.getSourceConnectionJndiName("Source"), "java:jboss/datasources/ExampleDS");
			assertEquals(model.getSourceTranslatorName("Source"), "h2");
			assertEquals(ConnectionType.ANY, vdb.getConnectionType());
			
		} finally {
			admin.undeploy("bqt.vdb");
		}
		admin.close();
	}
}
