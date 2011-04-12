package org.teiid.systemmodel;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.AbstractMMQueryTestCase;
import org.teiid.jdbc.FakeServer;
import org.teiid.jdbc.TestMMDatabaseMetaData;

@SuppressWarnings("nls")
public class TestODBCSchema extends AbstractMMQueryTestCase {
	private static final String VDB = "PartsSupplier"; //$NON-NLS-1$

	public TestODBCSchema() {
		// this is needed because the result files are generated
		// with another tool which uses tab as delimiter
		super.DELIMITER = "\t"; //$NON-NLS-1$
	}
	
    @Before public void setUp() throws Exception {
    	FakeServer server = new FakeServer();
    	server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb");
    	this.internalConnection = server.createConnection("jdbc:teiid:" + VDB); //$NON-NLS-1$ //$NON-NLS-2$	
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
	
	@Test public void testOIDUniquness() throws Exception {
		for (String table : new String[] {"Tables", "Columns", "Schemas", "DataTypes", "Keys", "Procedures", "ProcedureParams", "Properties"}) {
			execute("select count(distinct oid), count(*) from SYS."+table);
			internalResultSet.next();
			assertEquals(internalResultSet.getInt(2), internalResultSet.getInt(1));
		}
	}
}
