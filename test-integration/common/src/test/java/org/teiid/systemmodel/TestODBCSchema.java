package org.teiid.systemmodel;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TestMMDatabaseMetaData;

@SuppressWarnings("nls")
public class TestODBCSchema extends AbstractMMQueryTestCase {
	private static final String VDB = "PartsSupplier"; //$NON-NLS-1$
	private static FakeServer server;

	public TestODBCSchema() {
		// this is needed because the result files are generated
		// with another tool which uses tab as delimiter
		super.DELIMITER = "\t"; //$NON-NLS-1$
	}
	
    @BeforeClass public static void oneTimeSetUp() throws Exception {
    	server = new FakeServer(true);
    	server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
   	}
    
    @AfterClass public static void oneTimeTearDown() {
    	server.stop();
    }
    
    @Before public void setUp() throws Exception {
    	this.internalConnection = server.createConnection("jdbc:teiid:" + VDB); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    @After public void tearDown() throws Exception {
    	if (this.internalConnection != null) {
    		this.internalConnection.close();
    	}
    }
   
	@Test public void test_PG_AM() throws Exception {
		execute("select * FROM pg_am"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}
	
	@Test public void test_PG_ATTRDEF()  throws Exception {
		execute("select * FROM pg_attrdef"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}

	@Test public void test_PG_ATTRIBUTE()  throws Exception {
		execute("select * FROM pg_attribute"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}

	@Test public void test_PG_CLASS()  throws Exception {
		execute("select * FROM pg_class"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}

	@Test public void test_PG_INDEX()  throws Exception {
		execute("select * FROM pg_index"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}

	@Test public void test_PG_NAMESPACE()  throws Exception {
		execute("select * FROM pg_namespace"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}

	@Test public void test_PG_TRIGGER()  throws Exception {
		execute("select * FROM pg_trigger"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}

	@Test public void test_PG_TYPE()  throws Exception {
		execute("select * FROM pg_type"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}

	@Test public void test_PG_DATABASE()  throws Exception {
		execute("select* FROM pg_database"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}

	@Test public void test_PG_USER()  throws Exception {
		execute("select * FROM pg_user"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}
	
	@Test public void testOBIEEColumnQuery() throws Exception {
		execute("select ta.attname, ia.attnum, ic.relname, n.nspname, tc.relname from pg_catalog.pg_attribute ta, pg_catalog.pg_attribute ia, pg_catalog.pg_class tc, pg_catalog.pg_index i, pg_catalog.pg_namespace n, pg_catalog.pg_class ic where tc.oid = i.indrelid AND n.oid = tc.relnamespace AND i.indisprimary = 't' AND ia.attrelid = i.indexrelid AND ta.attrelid = i.indrelid AND ta.attnum = i.indkey[ia.attnum-1] AND (NOT ta.attisdropped) AND (NOT ia.attisdropped) AND ic.oid = i.indexrelid order by ia.attnum, ta.attname, ic.relname");
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}
	
	@Test public void testOIDUniquness() throws Exception {
		for (String table : new String[] {"pg_type", "pg_attribute", "pg_namespace", "pg_index"}) {
			execute("select count(distinct oid), count(*) from "+table);
			internalResultSet.next();
			assertEquals(internalResultSet.getInt(2), internalResultSet.getInt(1));
		}
	}
}
