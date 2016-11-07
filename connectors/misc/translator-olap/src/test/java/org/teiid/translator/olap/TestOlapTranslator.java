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

package org.teiid.translator.olap;

import static org.junit.Assert.*;

import java.sql.Connection;

import org.junit.Test;
import org.mockito.Mockito;
import org.olap4j.OlapConnection;
import org.olap4j.OlapStatement;
import org.olap4j.OlapWrapper;
import org.teiid.cdk.CommandBuilder;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;

@SuppressWarnings("nls")
public class TestOlapTranslator {

	@Test public void testCannedProcedure() throws Exception {
		String ddl = "create foreign procedure proc(arg integer, arg1 date) returns table (x string) options (\"teiid_rel:native-query\" '$2 $1 something')";
		String query = "exec proc(2, {d'1970-01-01'})";
		
		TransformationMetadata tm = RealMetadataFactory.fromDDL(ddl, "x", "phy");
		
		CommandBuilder commandBuilder = new CommandBuilder(tm);
        Command obj = commandBuilder.getCommand(query);
	        
		OlapExecutionFactory oef = new OlapExecutionFactory();
		Connection mock = Mockito.mock(java.sql.Connection.class);
		OlapWrapper mock2 = Mockito.mock(OlapWrapper.class);
		OlapConnection mock3 = Mockito.mock(OlapConnection.class);
		OlapStatement mock4 = Mockito.mock(OlapStatement.class);
		Mockito.stub(mock4.executeOlapQuery(Mockito.anyString())).toThrow(new TeiidRuntimeException());
		Mockito.stub(mock3.createStatement()).toReturn(mock4);
		Mockito.stub(mock2.unwrap(OlapConnection.class)).toReturn(mock3);
		Mockito.stub(mock.unwrap(OlapWrapper.class)).toReturn(mock2);
		ProcedureExecution pe = oef.createProcedureExecution((Call)obj, Mockito.mock(ExecutionContext.class), new RuntimeMetadataImpl(tm), mock);
		try {
			pe.execute();
			fail();
		} catch (TeiidRuntimeException e) {
			Mockito.verify(mock4).executeOlapQuery("'1970-01-01' 2 something");
		}
	}
	
}
