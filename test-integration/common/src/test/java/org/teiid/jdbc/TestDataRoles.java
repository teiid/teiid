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

package org.teiid.jdbc;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.teiid.deployers.VDBRepository;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.runtime.MaterializationManager;

@SuppressWarnings("nls")
public class TestDataRoles {

	private static final class ExtendedEmbeddedServer extends EmbeddedServer {
		//@Override
		//public MaterializationManager getMaterializationManager() {
		//	return super.getMaterializationManager();
		//}
		
		//@Override
		//public VDBRepository getVDBRepository() {
		//	return super.getVDBRepository();
		//}
	}

        @After public void tearDown() {
		es.stop();
	}

	private ExtendedEmbeddedServer es;
	
	@Test public void testExecuteImmediate() throws Exception {
		es = new ExtendedEmbeddedServer();
		EmbeddedConfiguration ec = new EmbeddedConfiguration();
		es.start(ec);
		es.deployVDB(new ByteArrayInputStream(new String("<vdb name=\"role-1\" version=\"1\">"
				+ "<model name=\"myschema\" type=\"virtual\">"
				+ "<metadata type = \"DDL\"><![CDATA[CREATE VIEW vw as select 'a' as col;]]></metadata></model>"
				+ "<data-role name=\"y\" any-authenticated=\"true\"/></vdb>").getBytes()));
    	Connection c = es.getDriver().connect("jdbc:teiid:role-1", null);
    	Statement s = c.createStatement();
    	s.execute("set autoCommitTxn off");
    	
    	try {
    		s.execute("begin execute immediate 'select * from vw'; end");
    		fail();
    	} catch (TeiidSQLException e) {
    		
    	}
    	
    	//should be valid
    	s.execute("begin execute immediate 'select 1'; end");
    	
    	//no temp permission
    	try {
    		s.execute("begin execute immediate 'select 1' as x integer into #temp; end");
    		fail();
    	} catch (TeiidSQLException e) {
    		
    	}
    	
    	//nested should not pass either
    	try {
	    	s.execute("begin execute immediate 'begin execute immediate ''select * from vw''; end'; end");
	    	fail();
    	} catch (TeiidSQLException e) {
    		
    	}
	}
	
}
