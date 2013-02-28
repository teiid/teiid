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

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.Model;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.adminapi.impl.SourceMappingMetadata;
import org.teiid.query.sql.lang.From;
import org.teiid.query.sql.lang.Query;
import org.teiid.query.sql.lang.Select;
import org.teiid.query.sql.symbol.ElementSymbol;
import org.teiid.query.sql.symbol.GroupSymbol;
import org.teiid.runtime.EmbeddedConfiguration;
import org.teiid.runtime.EmbeddedConnection;
import org.teiid.runtime.EmbeddedRequestOptions;
import org.teiid.runtime.EmbeddedServer;
import org.teiid.translator.loopback.LoopbackExecutionFactory;

@SuppressWarnings("nls")
public class TestASTQueries {

	private static EmbeddedServer server;
	
	@BeforeClass public static void setUp() throws Exception {
    	server = new EmbeddedServer();
    	server.start(new EmbeddedConfiguration());
    	LoopbackExecutionFactory loopy = new LoopbackExecutionFactory();
    	loopy.setRowCount(10);
    	loopy.start();
    	server.addTranslator("l", loopy);
    	
    	String DDL = "CREATE FOREIGN TABLE G1 (e1 string, e2 integer);";
    	ModelMetaData model = new ModelMetaData();
    	model.setName("PM1");
    	model.setModelType(Model.Type.PHYSICAL);
    	model.setSchemaSourceType("DDL");
    	model.setSchemaText(DDL);
    	SourceMappingMetadata sm = new SourceMappingMetadata();
    	sm.setName("loopy");
    	sm.setTranslatorName("l");
    	model.addSourceMapping(sm);
    	server.deployVDB("test", model);
    }
	
	@AfterClass public static void tearDown() throws Exception {
		server.stop();
	}

	@Test public void testAST() throws Exception {
		TeiidDriver td = server.getDriver();
		Connection c = td.connect("jdbc:teiid:test", new Properties());
		EmbeddedConnection ec = c.unwrap(EmbeddedConnection.class);
		TeiidPreparedStatement tps = ec.prepareStatement(sampleQuery(), new EmbeddedRequestOptions());
		ResultSet rs = tps.executeQuery();
		assertNotNull(rs);
		int count = 0;
		while (rs.next()) {
			count++;
		}
		assertEquals(10, count);
		rs.close();
	}
	
    private Query sampleQuery() {
        List<ElementSymbol> symbols = new ArrayList<ElementSymbol>();
        symbols.add(new ElementSymbol("e1"));  //$NON-NLS-1$
        symbols.add(new ElementSymbol("e2"));  //$NON-NLS-1$
        Select select = new Select(symbols);
           
        From from = new From();
        from.addGroup(new GroupSymbol("G1")); //$NON-NLS-1$
        
        Query query = new Query();
        query.setSelect(select);
        query.setFrom(from);
        return query;
    }	
}
