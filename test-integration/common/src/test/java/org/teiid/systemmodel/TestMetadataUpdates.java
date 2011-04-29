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
package org.teiid.systemmodel;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.jdbc.FakeServer;
import org.teiid.metadata.MetadataRepository;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.Table;
import org.teiid.metadata.Table.TriggerOperation;

@SuppressWarnings("nls")
public class TestMetadataUpdates {
	
	static Connection connection;
    
    static final String VDB = "metadata";
    
	@BeforeClass public static void setUp() throws Exception {
    	FakeServer server = new FakeServer();
    	MetadataRepository repo = Mockito.mock(MetadataRepository.class);
    	server.setMetadataRepository(repo);
    	Mockito.stub(repo.getViewDefinition(Mockito.anyString(), Mockito.anyInt(), (Table)Mockito.anyObject())).toAnswer(new Answer<String>() {
    		@Override
    		public String answer(InvocationOnMock invocation) throws Throwable {
    			Table t = (Table)invocation.getArguments()[2];
    			if (t.getName().equals("vw")) {
    				return "select '2011'";
    			}
    			return null;
    		}
		});
    	Mockito.stub(repo.getProcedureDefinition(Mockito.anyString(), Mockito.anyInt(), (Procedure)Mockito.anyObject())).toAnswer(new Answer<String>() {
    		@Override
    		public String answer(InvocationOnMock invocation) throws Throwable {
    			Procedure t = (Procedure)invocation.getArguments()[2];
    			if (t.getName().equals("proc")) {
    				return "create virtual procedure begin select '2011'; end";
    			}
    			return null;
    		}
		});
    	Mockito.stub(repo.getInsteadOfTriggerDefinition(Mockito.anyString(), Mockito.anyInt(), (Table)Mockito.anyObject(), (Table.TriggerOperation) Mockito.anyObject())).toAnswer(new Answer<String>() {
    		@Override
    		public String answer(InvocationOnMock invocation) throws Throwable {
				return "for each row select 1/0; begin end";
    		}
		});
    	server.deployVDB(VDB, UnitTestUtil.getTestDataPath() + "/metadata.vdb");
    	connection = server.createConnection("jdbc:teiid:" + VDB); //$NON-NLS-1$ //$NON-NLS-2$		
    }
    
    @AfterClass public static void tearDown() throws SQLException {
    	connection.close();
    }

    @Test public void testViewMetadataRepositoryMerge() throws Exception {
    	Statement s = connection.createStatement();
    	ResultSet rs = s.executeQuery("select * from vw");
    	rs.next();
    	assertEquals(2011, rs.getInt(1));
    }
    
    @Test(expected=SQLException.class) public void testViewUpdateMetadataRepositoryMerge() throws Exception {
    	Statement s = connection.createStatement();
    	s.execute("delete from vw");
    }
    
    @Test public void testProcMetadataRepositoryMerge() throws Exception {
    	Statement s = connection.createStatement();
    	ResultSet rs = s.executeQuery("call proc(1)");
    	rs.next();
    	assertEquals(2011, rs.getInt(1));
    }
    
}
