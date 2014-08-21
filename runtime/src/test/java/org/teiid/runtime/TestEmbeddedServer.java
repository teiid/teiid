/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.runtime;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.transaction.*;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.postgresql.Driver;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.deployers.VirtualDatabaseException;
import org.teiid.jdbc.SQLStates;
import org.teiid.jdbc.TeiidDriver;
import org.teiid.jdbc.TeiidSQLException;
import org.teiid.language.Command;
import org.teiid.language.Literal;
import org.teiid.language.QueryExpression;
import org.teiid.language.visitor.CollectorVisitor;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.query.sql.symbol.Reference;
import org.teiid.runtime.EmbeddedServer.ConnectionFactoryProvider;
import org.teiid.translator.DataNotAvailableException;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ExecutionFactory;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.UpdateExecution;
import org.teiid.transport.SocketConfiguration;
import org.teiid.transport.WireProtocol;

@SuppressWarnings("nls")
public class TestEmbeddedServer {
	public static final class MockTransactionManager implements TransactionManager {
		ThreadLocal<Transaction> txns = new ThreadLocal<Transaction>();
		List<Transaction> txnHistory = new ArrayList<Transaction>();

		@Override
		public Transaction suspend() throws SystemException {
			Transaction result = txns.get();
			txns.remove();
			return result;
		}

		@Override
		public void setTransactionTimeout(int seconds) throws SystemException {
		}

		@Override
		public void setRollbackOnly() throws IllegalStateException, SystemException {
			Transaction result = txns.get();
			if (result == null) {
				throw new IllegalStateException();
			}
			result.setRollbackOnly();
		}

		@Override
		public void rollback() throws IllegalStateException, SecurityException,
				SystemException {
			Transaction t = checkNull(false);
			txns.remove();
			t.rollback();
		}

		@Override
		public void resume(Transaction tobj) throws InvalidTransactionException,
				IllegalStateException, SystemException {
			checkNull(true);
			txns.set(tobj);
		}

		private Transaction checkNull(boolean isNull) {
			Transaction t = txns.get();
			if ((!isNull && t == null) || (isNull && t != null)) {
				throw new IllegalStateException();
			}
			return t;
		}

		@Override
		public Transaction getTransaction() throws SystemException {
			return txns.get();
		}

		@Override
		public int getStatus() throws SystemException {
			Transaction t = txns.get();
			if (t == null) {
				return Status.STATUS_NO_TRANSACTION;
			}
			return t.getStatus();
		}

		@Override
		public void commit() throws RollbackException, HeuristicMixedException,
				HeuristicRollbackException, SecurityException,
				IllegalStateException, SystemException {
			Transaction t = checkNull(false);
			txns.remove();
			t.commit();
		}

		@Override
		public void begin() throws NotSupportedException, SystemException {
			checkNull(true);
			Transaction t = Mockito.mock(Transaction.class);
			txnHistory.add(t);
			txns.set(t);
		}

		public void reset() {
			txnHistory.clear();
			txns = new ThreadLocal<Transaction>();
		}
	}

	EmbeddedServer es;
	
	@Before public void setup() {
		es = new EmbeddedServer();
	}
	
	@After public void teardown() {
		es.stop();
	}
	
	@Test public void testDeploy() throws Exception {
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		ec.setUseDisk(false);
		es.start(ec);
		
		es.addTranslator("y", new ExecutionFactory<AtomicInteger, Object> () {
			@Override
			public Object getConnection(AtomicInteger factory)
					throws TranslatorException {
				return factory.incrementAndGet();
			}
			
			@Override
			public void closeConnection(Object connection, AtomicInteger factory) {
				
			}
			
			@Override
			public void getMetadata(MetadataFactory metadataFactory, Object conn)
					throws TranslatorException {
				assertEquals(conn, Integer.valueOf(1));
				Table t = metadataFactory.addTable("my-table");
				t.setSupportsUpdate(true);
				Column c = metadataFactory.addColumn("my-column", TypeFacility.RUNTIME_NAMES.STRING, t);
				c.setUpdatable(true);
			}
			
			@Override
			public ResultSetExecution createResultSetExecution(
					QueryExpression command, ExecutionContext executionContext,
					RuntimeMetadata metadata, Object connection)
					throws TranslatorException {
				ResultSetExecution rse = new ResultSetExecution() {
					
					@Override
					public void execute() throws TranslatorException {
						
					}
					
					@Override
					public void close() {
						
					}
					
					@Override
					public void cancel() throws TranslatorException {
						
					}
					
					@Override
					public List<?> next() throws TranslatorException, DataNotAvailableException {
						return null;
					}
				};
				return rse;
			}
			
			@Override
			public UpdateExecution createUpdateExecution(Command command,
					ExecutionContext executionContext,
					RuntimeMetadata metadata, Object connection)
					throws TranslatorException {
				UpdateExecution ue = new UpdateExecution() {
					
					@Override
					public void execute() throws TranslatorException {
						
					}
					
					@Override
					public void close() {
						
					}
					
					@Override
					public void cancel() throws TranslatorException {
						
					}
					
					@Override
					public int[] getUpdateCounts() throws DataNotAvailableException,
							TranslatorException {
						return new int[] {2};
					}
				};
				return ue;
			}
		});
		final AtomicInteger counter = new AtomicInteger();
		ConnectionFactoryProvider<AtomicInteger> cfp = new EmbeddedServer.SimpleConnectionFactoryProvider<AtomicInteger>(counter);
		
		es.addConnectionFactoryProvider("z", cfp);
		
		ModelMetaData mmd = new ModelMetaData();
		mmd.setName("my-schema");
		mmd.addSourceMapping("x", "y", "z");

		ModelMetaData mmd1 = new ModelMetaData();
		mmd1.setName("virt");
		mmd1.setModelType(Type.VIRTUAL);
		mmd1.setSchemaSourceType("ddl");
		mmd1.setSchemaText("create view \"my-view\" OPTIONS (UPDATABLE 'true') as select * from \"my-table\"");

		es.deployVDB("test", mmd, mmd1);
		
		TeiidDriver td = es.getDriver();
		Connection c = td.connect("jdbc:teiid:test", null);
		Statement s = c.createStatement();
		ResultSet rs = s.executeQuery("select * from \"my-view\"");
		assertFalse(rs.next());
		assertEquals("my-column", rs.getMetaData().getColumnLabel(1));
		
		s.execute("update \"my-view\" set \"my-column\" = 'a'");
		assertEquals(2, s.getUpdateCount());
		
		es.deployVDB("empty");
		c = es.getDriver().connect("jdbc:teiid:empty", null);
		s = c.createStatement();
		s.execute("select * from tables");
		
		assertNotNull(es.getSchemaDdl("empty", "SYS"));
		assertNull(es.getSchemaDdl("empty", "xxx"));
	}
	
	@Test(expected=VirtualDatabaseException.class) public void testInvalidName() throws Exception {
		es.start(new EmbeddedConfiguration());
		ModelMetaData mmd1 = new ModelMetaData();
		mmd1.setName("virt.1");
		mmd1.setModelType(Type.VIRTUAL);
		mmd1.setSchemaSourceType("ddl");
		mmd1.setSchemaText("create view \"my-view\" as select 1");
		es.deployVDB("x", mmd1);
	}
	
	@Test public void testDeployZip() throws Exception {
		es.start(new EmbeddedConfiguration());
		
		File f = UnitTestUtil.getTestScratchFile("some.vdb");
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(f));
        out.putNextEntry(new ZipEntry("v1.ddl")); 
        out.write("CREATE VIEW helloworld as SELECT 'HELLO WORLD';".getBytes("UTF-8"));
        out.putNextEntry(new ZipEntry("META-INF/vdb.xml"));
        out.write("<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL-FILE\">/v1.ddl</metadata></model></vdb>".getBytes("UTF-8"));
        out.close();
		
		es.deployVDBZip(f.toURI().toURL());
		ResultSet rs = es.getDriver().connect("jdbc:teiid:test", null).createStatement().executeQuery("select * from helloworld");
		rs.next();
		assertEquals("HELLO WORLD", rs.getString(1));
	}
	
	@Test public void testDeployDesignerZip() throws Exception {
		es.start(new EmbeddedConfiguration());
		es.deployVDBZip(UnitTestUtil.getTestDataFile("matviews.vdb").toURI().toURL());
		ResultSet rs = es.getDriver().connect("jdbc:teiid:matviews", null).createStatement().executeQuery("select count(*) from tables where schemaname='test'");
		rs.next();
		assertEquals(4, rs.getInt(1));
	}
	
	@Test public void testXMLDeploy() throws Exception {
		es.start(new EmbeddedConfiguration());
		es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA[CREATE VIEW helloworld as SELECT 'HELLO WORLD';]]> </metadata></model></vdb>".getBytes()));
		ResultSet rs =es.getDriver().connect("jdbc:teiid:test", null).createStatement().executeQuery("select * from helloworld");
		rs.next();
		assertEquals("HELLO WORLD", rs.getString(1));
	}
	
	@Test public void testXMLDeployWithVDBImport() throws Exception {
		es.start(new EmbeddedConfiguration());
		es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA[CREATE VIEW helloworld as SELECT 'HELLO WORLD';]]> </metadata></model></vdb>".getBytes()));
		es.deployVDB(new ByteArrayInputStream("<vdb name=\"importer\" version=\"1\"><import-vdb name=\"test\" version=\"1\"/></vdb>".getBytes()));
		ResultSet rs =es.getDriver().connect("jdbc:teiid:importer", null).createStatement().executeQuery("select * from helloworld");
		rs.next();
		assertEquals("HELLO WORLD", rs.getString(1));
		es.deployVDB(new ByteArrayInputStream("<vdb name=\"importer1\" version=\"1\"><import-vdb name=\"importer\" version=\"1\"/></vdb>".getBytes()));
		rs =es.getDriver().connect("jdbc:teiid:importer1", null).createStatement().executeQuery("select * from helloworld");
		rs.next();
		assertEquals("HELLO WORLD", rs.getString(1));
	}
	
	@Test public void testRemoteJDBCTrasport() throws Exception {
		SocketConfiguration s = new SocketConfiguration();
		InetSocketAddress addr = new InetSocketAddress(0);
		s.setBindAddress(addr.getHostName());
		s.setPortNumber(addr.getPort());
		s.setProtocol(WireProtocol.teiid);
		EmbeddedConfiguration config = new EmbeddedConfiguration();
		config.addTransport(s);
		es.start(config);
		es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA[CREATE VIEW helloworld as SELECT 'HELLO WORLD';]]> </metadata></model></vdb>".getBytes()));
		Connection conn = null;
		try {
			TeiidDriver driver = new TeiidDriver();
			conn = driver.connect("jdbc:teiid:test@mm://"+addr.getHostName()+":"+es.transports.get(0).getPort(), null);
			ResultSet rs = conn.createStatement().executeQuery("select * from helloworld");
			rs.next();
			assertEquals("HELLO WORLD", rs.getString(1));
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}	
	
	@Test public void testRemoteODBCTrasport() throws Exception {
		SocketConfiguration s = new SocketConfiguration();
		InetSocketAddress addr = new InetSocketAddress(0);
		s.setBindAddress(addr.getHostName());
		s.setPortNumber(addr.getPort());
		s.setProtocol(WireProtocol.pg);
		EmbeddedConfiguration config = new EmbeddedConfiguration();
		config.addTransport(s);
		es.start(config);
		es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA[CREATE VIEW helloworld as SELECT 'HELLO WORLD';]]> </metadata></model></vdb>".getBytes()));
		Connection conn = null;
		try {
			Driver d = new Driver();
			Properties p = new Properties();
			p.setProperty("user", "testuser");
			p.setProperty("password", "testpassword");
			
			conn = d.connect("jdbc:postgresql://"+addr.getHostName()+":"+es.transports.get(0).getPort()+"/test", p);
			ResultSet rs = conn.createStatement().executeQuery("select * from helloworld");
			rs.next();
			assertEquals("HELLO WORLD", rs.getString(1));
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}	
	
	@Test(expected=VirtualDatabaseException.class) public void testXMLDeployFails() throws Exception {
		es.start(new EmbeddedConfiguration());
		es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA[CREATE VIEW helloworld as SELECT 'HELLO WORLD';]]> </metadata></model><translator name=\"foo\" type=\"h2\"></translator></vdb>".getBytes()));
	}
	
	/**
	 * Ensures schema validation is performed
	 * @throws Exception
	 */
	@Test(expected=VirtualDatabaseException.class) public void testXMLDeployFails1() throws Exception {
		es.start(new EmbeddedConfiguration());
		es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\"><source/></model><translator name=\"foo\" type=\"h2\"></translator></vdb>".getBytes()));
	}
	
	@Test(expected=VirtualDatabaseException.class) public void testDeploymentError() throws Exception {
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		ec.setUseDisk(false);
		es.start(ec);
		
		ModelMetaData mmd1 = new ModelMetaData();
		mmd1.setName("virt");
		mmd1.setModelType(Type.VIRTUAL);
		mmd1.setSchemaSourceType("ddl");
		mmd1.setSchemaText("create view \"my-view\" OPTIONS (UPDATABLE 'true') as select * from \"my-table\"");

		es.deployVDB("test", mmd1);
	}
	
	@Test public void testValidationOrder() throws Exception {
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		ec.setUseDisk(false);
		es.start(ec);
		
		ModelMetaData mmd1 = new ModelMetaData();
		mmd1.setName("b");
		mmd1.setModelType(Type.VIRTUAL);
		mmd1.setSchemaSourceType("ddl");
		mmd1.setSchemaText("create view v as select 1");

		ModelMetaData mmd2 = new ModelMetaData();
		mmd2.setName("a");
		mmd2.setModelType(Type.VIRTUAL);
		mmd2.setSchemaSourceType("ddl");
		mmd2.setSchemaText("create view v1 as select * from v");

		//We need mmd1 to validate before mmd2, reversing the order will result in an exception
		es.deployVDB("test", mmd1, mmd2);
		
		try {
			es.deployVDB("test2", mmd2, mmd1);
			fail();
		} catch (VirtualDatabaseException e) {
			
		}
	}
	
	@Test public void testTransactions() throws Exception {
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		MockTransactionManager tm = new MockTransactionManager();
		ec.setTransactionManager(tm);
		ec.setUseDisk(false);
		es.start(ec);
		
		ModelMetaData mmd1 = new ModelMetaData();
		mmd1.setName("b");
		mmd1.setModelType(Type.VIRTUAL);
		mmd1.setSchemaSourceType("ddl");
		mmd1.setSchemaText("create view v as select 1; " +
				"create virtual procedure proc () options (updatecount 2) as begin select * from v; end; " +
				"create virtual procedure proc1 () as begin atomic select * from v; end; " +
				"create virtual procedure proc2 (x integer) as begin atomic select 1; begin select 1/x; end exception e end; " +
				"create virtual procedure proc3 (x integer) as begin begin atomic select 1; end create local temporary table x (y string); begin atomic select 1; end end;");

		es.deployVDB("test", mmd1);
		
		TeiidDriver td = es.getDriver();
		Connection c = td.connect("jdbc:teiid:test", null);
		//local txn
		c.setAutoCommit(false);
		Statement s = c.createStatement();
		s.execute("select 1");
		c.setAutoCommit(true);
		assertEquals(1, tm.txnHistory.size());
		Transaction txn = tm.txnHistory.remove(0);
		Mockito.verify(txn).commit();
		
		//should be an auto-commit txn (could also force with autoCommitTxn=true)
		s.execute("call proc ()");
		
		assertEquals(1, tm.txnHistory.size());
		txn = tm.txnHistory.remove(0);
		Mockito.verify(txn).commit();
		
		//block txn
		s.execute("call proc1()");
		
		assertEquals(1, tm.txnHistory.size());
		txn = tm.txnHistory.remove(0);
		Mockito.verify(txn).commit();
		
		s.execute("set autoCommitTxn on");
		s.execute("set noexec on");
		s.execute("select 1");
		assertFalse(s.getResultSet().next());
		
		s.execute("set autoCommitTxn off");
		s.execute("set noexec off");
		s.execute("call proc2(0)");
		//verify that the block txn was committed because the exception was caught
		assertEquals(1, tm.txnHistory.size());
		txn = tm.txnHistory.remove(0);
		Mockito.verify(txn).rollback();

		//test detection
		tm.txnHistory.clear();
		tm.begin();
		try {
			c.setAutoCommit(false);
			s.execute("select 1"); //needed since we lazily start the transaction
			fail("should fail since we aren't allowing a nested transaction");
		} catch (TeiidSQLException e) {
		}
		txn = tm.txnHistory.remove(0);
		Mockito.verify(txn, Mockito.times(0)).commit();
		
		tm.commit();
		c.setAutoCommit(true);
		
		tm.txnHistory.clear();
		//ensure that we properly reset the txn context
		s.execute("call proc3(0)");
		assertEquals(2, tm.txnHistory.size());
		txn = tm.txnHistory.remove(0);
		Mockito.verify(txn, Mockito.times(0)).registerSynchronization((Synchronization) Mockito.any());
	}
	
	@Test public void testMultiSourcePreparedDynamicUpdate() throws Exception {
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		MockTransactionManager tm = new MockTransactionManager();
		ec.setTransactionManager(tm);
		ec.setUseDisk(false);
		es.start(ec);
		
		es.addTranslator("t", new ExecutionFactory<Void, Void>());
		
		ModelMetaData mmd1 = new ModelMetaData();
		mmd1.setName("b");
		mmd1.setSchemaSourceType("ddl");
		mmd1.setSchemaText("create view v (i integer) OPTIONS (UPDATABLE true) as select 1; " +
				"create trigger on v instead of update as for each row begin atomic " +
				"IF (CHANGING.i)\n" +
                "EXECUTE IMMEDIATE 'select \"new\".i'; " +
				"end; ");
		mmd1.setSupportsMultiSourceBindings(true);
		mmd1.addSourceMapping("x", "t", null);
		mmd1.addSourceMapping("y", "t", null);
		
		es.deployVDB("vdb", mmd1);
		
		Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);
		PreparedStatement ps = c.prepareStatement("update v set i = ? where i = ?");
		ps.setInt(1, 2);
		ps.setInt(2, 1);
		assertEquals(1, ps.executeUpdate());
		ps.setInt(1, 3);
		ps.setInt(2, 1);
		assertEquals(1, ps.executeUpdate());
	}
	
	@Test public void testMultiSourceMetadata() throws Exception {
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		MockTransactionManager tm = new MockTransactionManager();
		ec.setTransactionManager(tm);
		ec.setUseDisk(false);
		es.start(ec);
		
		es.addTranslator("t", new ExecutionFactory<Void, Void>());
		
		ModelMetaData mmd1 = new ModelMetaData();
		mmd1.setName("b");
		mmd1.setSchemaSourceType("ddl");
		mmd1.setSchemaText("create foreign table t (x string)");
		mmd1.setSupportsMultiSourceBindings(true);
		mmd1.addSourceMapping("x", "t", null);
		mmd1.addSourceMapping("y", "t", null);
		
		es.deployVDB("vdb", mmd1);
		
		Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);
		PreparedStatement ps = c.prepareStatement("select * from t");
		ResultSetMetaData metadata = ps.getMetaData();
		assertEquals(1, metadata.getColumnCount());
		
		mmd1.addProperty("multisource.addColumn", Boolean.TRUE.toString());
		
		es.undeployVDB("vdb");
		es.deployVDB("vdb", mmd1);
		
		c = es.getDriver().connect("jdbc:teiid:vdb", null);
		ps = c.prepareStatement("select * from t");
		metadata = ps.getMetaData();
		assertEquals(2, metadata.getColumnCount());
		
		mmd1.addProperty("multisource.columnName", "y");
		
		es.undeployVDB("vdb");
		es.deployVDB("vdb", mmd1);
		
		c = es.getDriver().connect("jdbc:teiid:vdb", null);
		ps = c.prepareStatement("select * from t");
		metadata = ps.getMetaData();
		assertEquals(2, metadata.getColumnCount());
		assertEquals("y", metadata.getColumnName(2));
	}
	
	/**
	 * Check that we'll consult each source
	 * @throws Exception
	 */
	@Test public void testMultiSourceMetadataMissingSource() throws Exception {
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		ec.setUseDisk(false);
		es.start(ec);
		
		es.addTranslator("t", new ExecutionFactory<Object, Object>() {
			@Override
			public Object getConnection(Object factory) throws TranslatorException {
				return factory;
			}
			@Override
			public void closeConnection(Object connection, Object factory) {
			}
			@Override
			public void getMetadata(MetadataFactory metadataFactory, Object conn)
					throws TranslatorException {
				assertNotNull(conn);
				Table t = metadataFactory.addTable("x");
				metadataFactory.addColumn("a", "string", t);
			}
		});
		es.addConnectionFactory("b", new Object());
		ModelMetaData mmd1 = new ModelMetaData();
		mmd1.setName("b");
		mmd1.setSupportsMultiSourceBindings(true);
		mmd1.addSourceMapping("x", "t", "a"); //a is missing
		mmd1.addSourceMapping("y", "t", "b");
		
		es.deployVDB("vdb", mmd1);		
	}
	
	@Test public void testDynamicUpdate() throws Exception {
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		MockTransactionManager tm = new MockTransactionManager();
		ec.setTransactionManager(tm);
		ec.setUseDisk(false);
		es.start(ec);
		
		es.addTranslator("t", new ExecutionFactory<Void, Void>() {
		
			@Override
			public boolean supportsCompareCriteriaEquals() {
				return true;
			}
			
			@Override
			public boolean isSourceRequired() {
				return false;
			}
			
			@Override
			public UpdateExecution createUpdateExecution(Command command,
					ExecutionContext executionContext,
					RuntimeMetadata metadata, Void connection)
					throws TranslatorException {
				Collection<Literal> values = CollectorVisitor.collectObjects(Literal.class, command);
				assertEquals(2, values.size());
				for (Literal literal : values) {
					assertFalse(literal.getValue() instanceof Reference);
				}
				return new UpdateExecution() {
					
					@Override
					public void execute() throws TranslatorException {
						
					}
					
					@Override
					public void close() {
						
					}
					
					@Override
					public void cancel() throws TranslatorException {
						
					}
					
					@Override
					public int[] getUpdateCounts() throws DataNotAvailableException,
							TranslatorException {
						return new int[] {1};
					}
				};
			}
		});
		
		ModelMetaData mmd1 = new ModelMetaData();
		mmd1.setName("accounts");
		mmd1.setSchemaSourceType("ddl");
		mmd1.setSchemaText(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("dynamic_update.sql")));
		mmd1.addSourceMapping("y", "t", null);
		
		es.deployVDB("vdb", mmd1);
		
		Connection c = es.getDriver().connect("jdbc:teiid:vdb", null);
		PreparedStatement ps = c.prepareStatement("update hello1 set SchemaName=? where Name=?");
		ps.setString(1,"test1223");
	    ps.setString(2,"Columns");
		assertEquals(1, ps.executeUpdate());
	}
	
	public static boolean started;
	
	public static class MyEF extends ExecutionFactory<Void, Void> {
		
		@Override
		public void start() throws TranslatorException {
			started = true;
		}
	}
	
	@Test public void testStart() throws TranslatorException {
		es.addTranslator(MyEF.class);
		assertTrue(started);
	}
	
	@Test public void testGlobalTempTables() throws Exception {
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		ec.setMaxResultSetCacheStaleness(0);
		MockTransactionManager tm = new MockTransactionManager();
		ec.setTransactionManager(tm);
		ec.setUseDisk(false);
		es.start(ec);
		
		es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA[CREATE global temporary table some_temp (col1 string, col2 time) options (updatable true);]]> </metadata></model></vdb>".getBytes()));
		
		Connection c = es.getDriver().connect("jdbc:teiid:test", null);
		
		PreparedStatement ps = c.prepareStatement("/*+ cache */ select * from some_temp");
		ResultSet rs = ps.executeQuery();
		assertFalse(rs.next());
		
		Connection c1 = es.getDriver().connect("jdbc:teiid:test", null);
		c1.createStatement().execute("insert into some_temp (col1) values ('a')");
		
		PreparedStatement ps1 = c1.prepareStatement("/*+ cache */ select * from some_temp");
		ResultSet rs1 = ps1.executeQuery();
		assertTrue(rs1.next()); //there's a result for the second session
		
		rs = ps.executeQuery();
		assertFalse(rs.next()); //still no result in the first session
		
		c.createStatement().execute("insert into some_temp (col1) values ('b')");
		
		rs = ps.executeQuery();
		assertTrue(rs.next()); //still no result in the first session
		
		//ensure without caching that we have the right results
		rs = c.createStatement().executeQuery("select * from some_temp");
		assertTrue(rs.next());
		assertEquals("b", rs.getString(1));
		
		rs = c1.createStatement().executeQuery("select * from some_temp");
		assertTrue(rs.next());
		assertEquals("a", rs.getString(1));
	}
	
	@Test public void testMaxRows() throws Exception {
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		ec.setMaxResultSetCacheStaleness(0);
		MockTransactionManager tm = new MockTransactionManager();
		ec.setTransactionManager(tm);
		ec.setUseDisk(false);
		es.start(ec);
		
		es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\" type=\"VIRTUAL\"><metadata type=\"DDL\"><![CDATA[CREATE virtual procedure proc (out col1 string result) returns TABLE (r1 string) as begin col1 = 'a'; select 'b' union all select 'c'; end;]]> </metadata></model></vdb>".getBytes()));
		
		Connection c = es.getDriver().connect("jdbc:teiid:test", null);
		
		CallableStatement cs = c.prepareCall("{? = call proc()}");
		ResultSet rs = cs.executeQuery();
		assertTrue(rs.next());
		assertTrue(rs.next());
		assertFalse(rs.next());
		assertEquals("a", cs.getString(1));
		
		//ensure that we don't drop the parameter row (which is last)
		cs.setMaxRows(1);
		rs = cs.executeQuery();
		assertTrue(rs.next());
		assertFalse(rs.next());
		assertEquals("a", cs.getString(1));
		
		//ensure that we can skip batches
		cs.setMaxRows(1);
		cs.setFetchSize(1);
		rs = cs.executeQuery();
		assertTrue(rs.next());
		assertFalse(rs.next());
		assertEquals("a", cs.getString(1));
		
		//cache should behave as expected when populated
		cs = c.prepareCall("/*+ cache */ {? = call proc()}");
		cs.setMaxRows(1);
		rs = cs.executeQuery();
		assertTrue(rs.next());
		assertFalse(rs.next());
		assertEquals("a", cs.getString(1));
		
		//accessing from cache without the max should still give us the full result
		cs.setMaxRows(0);
		rs = cs.executeQuery();
		assertTrue(rs.next());
		assertTrue(rs.next());
		assertFalse(rs.next());
		assertEquals("a", cs.getString(1));
		
		//accessing again with max should give the smaller result
		cs.setMaxRows(1);
		rs = cs.executeQuery();
		assertTrue(rs.next());
		assertFalse(rs.next());
		assertEquals("a", cs.getString(1));
	}
	
	@Test public void testSourceLobUnderTxn() throws Exception {
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		ec.setMaxResultSetCacheStaleness(0);
		MockTransactionManager tm = new MockTransactionManager();
		ec.setTransactionManager(tm);
		ec.setUseDisk(false);
		es.start(ec);
		
		final AtomicBoolean closed = new AtomicBoolean();
		es.addTranslator("foo", new ExecutionFactory() {
			
			@Override
			public boolean isSourceRequired() {
				return false;
			}
			
			@Override
			public ResultSetExecution createResultSetExecution(
					QueryExpression command, ExecutionContext executionContext,
					RuntimeMetadata metadata, Object connection)
					throws TranslatorException {
				return new ResultSetExecution() {
					
					private boolean returned;

					@Override
					public void execute() throws TranslatorException {
						
					}
					
					@Override
					public void close() {
						closed.set(true);
					}
					
					@Override
					public void cancel() throws TranslatorException {
						
					}
					
					@Override
					public List<?> next() throws TranslatorException, DataNotAvailableException {
						if (returned) {
							return null;
						}
						returned = true;
						ArrayList<Object> result = new ArrayList<Object>(1);
						result.add(new SQLXMLImpl(new InputStreamFactory() {
							
							@Override
							public InputStream getInputStream() throws IOException {
								//need to make it of a sufficient size to not be inlined
								return new ByteArrayInputStream(new byte[DataTypeManager.MAX_LOB_MEMORY_BYTES + 1]);
							}
						}));
						return result;
					}
				};
			}
		});
		es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\"><source name=\"foo\" translator-name=\"foo\"/><metadata type=\"DDL\"><![CDATA[CREATE foreign table x (y xml);]]> </metadata></model></vdb>".getBytes()));
		
		Connection c = es.getDriver().connect("jdbc:teiid:test", null);
		
		c.setAutoCommit(false);
		
		Statement s = c.createStatement();
		
		ResultSet rs = s.executeQuery("select * from x");
		
		rs.next();
		
		assertFalse(closed.get());
		
		s.close();
		
		assertTrue(closed.get());
	}
	
	@Test public void testQueryTimeout() throws Exception {
		es.start(new EmbeddedConfiguration());
		es.addTranslator("foo", new ExecutionFactory() {
			@Override
			public boolean isSourceRequired() {
				return false;
			}
			
			@Override
			public ResultSetExecution createResultSetExecution(
					QueryExpression command, ExecutionContext executionContext,
					RuntimeMetadata metadata, Object connection)
					throws TranslatorException {
				try {
					Thread.sleep(5000);
				} catch (InterruptedException e) {
				}
				return super.createResultSetExecution(command, executionContext, metadata,
						connection);
			}
			
		});
		es.deployVDB(new ByteArrayInputStream("<vdb name=\"test\" version=\"1\"><model name=\"test\"><source name=\"foo\" translator-name=\"foo\"/><metadata type=\"DDL\"><![CDATA[CREATE foreign table x (y xml);]]> </metadata></model></vdb>".getBytes()));
		Connection c = es.getDriver().connect("jdbc:teiid:test", null);
		Statement s = c.createStatement();
		s.setQueryTimeout(1);
		try {
			s.execute("select * from x");
			fail();
		} catch (SQLException e) {
			assertEquals(SQLStates.QUERY_CANCELED, e.getSQLState());
		}
		
	}

}
