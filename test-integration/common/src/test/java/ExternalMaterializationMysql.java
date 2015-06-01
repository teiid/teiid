

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.DataSource;

import org.infinispan.transaction.tm.DummyTransactionManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.jdbc.mysql.MySQL5ExecutionFactory;

public class ExternalMaterializationMysql {
    
	/*static {
        UnitTestUtil.enableLogging(Level.FINE, "org.teiid");
    }*/
    
    static EmbeddedServer server = null;
    static Connection conn = null;
    
    static Logger logger = Logger.getLogger(ExternalMaterializationMysql.class.getName());
    
    @BeforeClass
    public static void startup() throws Exception {
    	try {
        logger.info("Start");
        
        server = new EmbeddedServer();
        
        
        
        //DataSource ds = EmbeddedHelper.newDataSource("com.mysql.jdbc.Driver", "jdbc:mysql://localhost:3306/test", "jdv_user", "jdv_pass");

        //Driver d = new Driver();
		//final Connection c = d.connect("jdbc:h2:mem:", null);

        com.mysql.jdbc.Driver d = new com.mysql.jdbc.Driver();
        Properties properties = new Properties();
        properties.setProperty("user", "root");

        final Connection c = d.connect("jdbc:mysql://localhost/accounts", properties);
        Statement s = c.createStatement();
        String schema = ObjectConverterUtil.convertToString(new FileInputStream(UnitTestUtil.getTestDataFile("schema.sql")));
        String[] parts = schema.split(";");
        for (String part : parts) {
        	s.execute(part.trim());
        }
        s = c.createStatement();
        
        MySQL5ExecutionFactory factory = new MySQL5ExecutionFactory() {
        	public Connection getConnection(DataSource ds) throws org.teiid.translator.TranslatorException {
        		obtainedConnection(c);
        		return c;
        	};
        	
        	public void closeConnection(Connection connection, DataSource factory) {};
        	
        	public boolean isSourceRequired() {
        		return false;
        	}
        	
        };
        factory.start();
        factory.setSupportsDirectQueryProcedure(true);
        server.addTranslator("translator-mysql", factory);
        server.addConnectionFactory("java:/accounts-ds", Mockito.mock(DataSource.class));
        EmbeddedConfiguration config = new EmbeddedConfiguration();
        config.setTransactionManager(new DummyTransactionManager());
        server.start(config);
        
        server.deployVDB(new FileInputStream(UnitTestUtil.getTestDataFile("mat-mysql-vdb.xml")));
        
        Properties info = new Properties();
        conn = server.getDriver().connect("jdbc:teiid:MatVDB", info);
        } catch (Exception e) {
        	e.printStackTrace();
        }
    	
    }
    
    @AfterClass
    public static void teardown() throws SQLException {
        conn.close();
        server.stop();
    }
  

    @Test
    public void test() throws Exception {
    	for (int i = 0 ; i < 20; i++) {
        executeQuery(conn, "select * from Product");
        executeQuery(conn, "select * from h2_test_mat");
        executeQuery(conn, "select * from mat_test_staging");
        executeQuery(conn, "select * from status");
        executeQuery(conn, "select * from MatView");
        
        Thread.currentThread().sleep(10 * 1000);
        
        executeQuery(conn, "select * from Product");
        executeQuery(conn, "select * from h2_test_mat");
        executeQuery(conn, "select * from mat_test_staging");
        executeQuery(conn, "select * from status");
        executeQuery(conn, "select * from MatView");
        
        executeQuery(conn, "INSERT INTO PRODUCT (ID,SYMBOL,COMPANY_NAME) VALUES(2000,'RHT','Red Hat Inc')");
        
        Thread.currentThread().sleep(10 * 1000);
        
        executeQuery(conn, "select * from Product");
        executeQuery(conn, "select * from h2_test_mat");
        executeQuery(conn, "select * from mat_test_staging");
        executeQuery(conn, "select * from status");
        executeQuery(conn, "select * from MatView");
        
        executeQuery(conn, "DELETE FROM PRODUCT  WHERE ID = 2000");
        
        Thread.currentThread().sleep(10 * 1000);
        
        executeQuery(conn, "select * from Product");
        executeQuery(conn, "select * from h2_test_mat");
        executeQuery(conn, "select * from mat_test_staging");
        executeQuery(conn, "select * from status");
        executeQuery(conn, "select * from MatView");
    	}
    }

	private void executeQuery(Connection conn2, String string) throws SQLException, IOException {
		System.out.println(string);
		Statement s = conn.createStatement();
		s.execute(string);
		ResultSet rs = s.getResultSet();
		if (rs != null) {
			rs.close();
		}
		s.close();
	}

   

}