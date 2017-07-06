package org.teiid.systemmodel;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
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
    	server.undeployVDB("x");
    }
   
	@Test public void test_PG_AM() throws Exception {
		execute("select * FROM pg_am"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}
	
	@Test public void test_PG_ATTRDEF()  throws Exception {
		execute("select * FROM pg_attrdef order by adrelid, adnum"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}

	@Test public void test_PG_ATTRIBUTE()  throws Exception {
		execute("select * FROM pg_attribute order by oid"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}
	
	@Test public void test_PG_ATTRIBUTE_overflow()  throws Exception {
		ModelMetaData mmd = new ModelMetaData();
		mmd.setName("x");
		mmd.setModelType(Type.VIRTUAL);
		mmd.addSourceMetadata("ddl", "create view t (c bigdecimal(2147483647,2147483647)) as select 1.0;");
		server.deployVDB("overflow", mmd);
		this.internalConnection = server.createConnection("jdbc:teiid:overflow"); //$NON-NLS-1$ //$NON-NLS-2$
		execute("select * FROM pg_attribute order by oid"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}

	@Test public void test_PG_CLASS()  throws Exception {
		execute("select * FROM pg_class order by oid"); //$NON-NLS-1$
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}
	
	@Test public void test_PG_INDEX()  throws Exception {
		execute("select * FROM pg_index order by oid"); //$NON-NLS-1$
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
	
	@Test public void testPGTableConflicts() throws Exception {
		execute("select name FROM tables where schemaname='pg_catalog'"); //$NON-NLS-1$
		ArrayList<String> names = new ArrayList<String>();
		while (internalResultSet.next()) {
			names.add(internalResultSet.getString(1));
		}
		ModelMetaData mmd = new ModelMetaData();
		mmd.setName("x");
		mmd.setModelType(Type.VIRTUAL);
		mmd.setSchemaSourceType("ddl");
		StringBuffer ddl = new StringBuffer();
		for (String name : names) {
			ddl.append("create view "+name+" as select 1;\n");
		}
		mmd.setSchemaText(ddl.toString());
		server.deployVDB("x", mmd);
		
		this.internalConnection.close();
		this.internalConnection = server.createConnection("jdbc:teiid:x"); //$NON-NLS-1$ //$NON-NLS-2$
		
		for (String name : names) {
			execute("select * from pg_catalog." + name);
		}
	}
	
	@Test public void testTypes() throws Exception {
		ModelMetaData mmd = new ModelMetaData();
		mmd.setName("x");
		mmd.setModelType(Type.VIRTUAL);
		mmd.addSourceMetadata("ddl", "create view v as select null, cast(null as xml), cast(null as boolean), cast(null as byte), cast(null as short), cast(null as integer), cast(null as long),"
				+ " cast(null as float), cast(null as double), cast(null as bigdecimal), cast(null as biginteger), cast(null as time), cast(null as date), cast(null as timestamp), cast(null as varbinary), "
				+ " cast(null as char), cast(null as string), cast(null as clob), cast(null as blob), "
				+ " cast(null as xml[]), cast(null as boolean[]), cast(null as byte[]), cast(null as short[]), cast(null as integer[]), cast(null as long[]), cast(null as bigdecimal[]), cast(null as biginteger[]), "
				+ " cast(null as float[]), cast(null as double[]), cast(null as time[]), cast(null as date[]), cast(null as timestamp[]), cast(null as varbinary[]), "
				+ " cast(null as char[]), cast(null as string[]), cast(null as clob[]), cast(null as blob[])");
		server.deployVDB("x", mmd);
		
		this.internalConnection.close();
		this.internalConnection = server.createConnection("jdbc:teiid:x"); //$NON-NLS-1$ //$NON-NLS-2$
		
		execute("select count(oid) = count(distinct oid) from pg_attribute"); //$NON-NLS-1$
	    this.internalResultSet.next();
	    assertTrue(this.internalResultSet.getBoolean(1));
		
		execute("select oid from pg_class where relname = 'v'"); //$NON-NLS-1$
        this.internalResultSet.next();
        int val = this.internalResultSet.getInt(1);
        
        String sql = "select n.nspname, c.relname, a.attname, a.atttypid, t.typname, a.attnum, a.attlen, a.atttypmod, a.attnotnull, c.relhasrules, c.relkind, c.oid, pg_get_expr(d.adbin, d.adrelid), case t.typtype when 'd' then t.typbasetype else 0 end, t.typtypmod, c.relhasoids "
		+ "from (((pg_catalog.pg_class c inner join pg_catalog.pg_namespace n on n.oid = c.relnamespace and "
		+ "c.oid = ?) inner join pg_catalog.pg_attribute a on (not a.attisdropped) and a.attnum > 0 and a.attrelid = c.oid) inner join pg_catalog.pg_type t on t.oid = a.atttypid) left outer join pg_attrdef d on a.atthasdef and d.adrelid = a.attrelid and d.adnum = a.attnum order by n.nspname, c.relname, attnum";	    
		
        execute(sql, new Object[] {val});
        
		TestMMDatabaseMetaData.compareResultSet(this.internalResultSet);
	}
	
}
