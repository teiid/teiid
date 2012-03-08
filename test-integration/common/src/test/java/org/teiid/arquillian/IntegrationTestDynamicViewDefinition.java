package org.teiid.arquillian;

import static org.junit.Assert.*;

import java.io.FileInputStream;
import java.sql.Connection;
import java.util.Properties;

import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminFactory;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.jdbc.TestMMDatabaseMetaData;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;

@RunWith(Arquillian.class)
@SuppressWarnings("nls")
public class IntegrationTestDynamicViewDefinition extends AbstractMMQueryTestCase {

	private Admin admin;
	
	@Before
	public void setup() throws Exception {
		admin = AdminFactory.getInstance().createAdmin("localhost", 9999,	"admin", "admin".toCharArray());
	}
	
	@After
	public void teardown() {
		admin.close();
	}
	
	@Test
    public void testViewDefinition() throws Exception {
		
		Properties props = new Properties();
		props.setProperty("ParentDirectory", "../docs/teiid/examples/dynamicvdb-portfolio/data");
		props.setProperty("AllowParentPaths", "true");
		admin.createDataSource("marketdata-file", "teiid-connector-file.rar", props);

		
		admin.deploy("dynamicview-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("dynamicview-vdb.xml")));
		
		this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:dynamic@mm://localhost:31000;user=user;password=user", null);
		
		execute("select * FROM Portfolio.Stock Where SchemaName = 'portfolio'"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet("TestDymamicImportedMetaData/columns", this.internalResultSet); 
		
    }

}
