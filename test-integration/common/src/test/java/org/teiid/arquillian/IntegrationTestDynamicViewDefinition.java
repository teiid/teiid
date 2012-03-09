package org.teiid.arquillian;

import java.io.FileInputStream;
import java.util.Properties;

import org.jboss.arquillian.junit.Arquillian;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.teiid.adminapi.Admin;
import org.teiid.adminapi.AdminFactory;
import org.teiid.adminapi.VDB;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.jdbc.TestMMDatabaseMetaData;

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
				
		admin.deploy("dynamicview-vdb.xml",new FileInputStream(UnitTestUtil.getTestDataFile("dynamicview-vdb.xml")));
		
		Properties props = new Properties();
		props.setProperty("ParentDirectory", "../docs/teiid/examples/dynamicvdb-portfolio/data");
		props.setProperty("AllowParentPaths", "true");
		
		admin.createDataSource("marketdata-file", "teiid-connector-file.rar", props);

		int count = 0;
		while (count < 10) {
			VDB vdb = admin.getVDB("dynamic", 1);
			if (vdb == null || vdb.getStatus() != VDB.Status.ACTIVE) {
				Thread.sleep(500);
				count++;
			}
			else {
				break;
			}
		}
		
		this.internalConnection =  TeiidDriver.getInstance().connect("jdbc:teiid:dynamic@mm://localhost:31000;user=user;password=user", null);
		
		execute("SELECT * FROM Sys.Columns WHERE tablename='stock'"); //$NON-NLS-1$
		//TestMMDatabaseMetaData.compareResultSet("TestDymamicImportedMetaData/columns", this.internalResultSet); 
	
		admin.undeploy("dynamicview-vdb.xml");
    }

}
