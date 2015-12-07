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

package org.teiid.translator.cassandra;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.Execution;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;

@SuppressWarnings("nls")
public class TestNativeCassandra {

	@Test public void testDirect() throws TranslatorException {
		CassandraExecutionFactory cef = new CassandraExecutionFactory();
		cef.setSupportsDirectQueryProcedure(true);
		
		String input = "call native('select $1', 'a')";
		
        TranslationUtility util = FakeTranslationFactory.getInstance().getExampleTranslationUtility();
        Command command = util.parseCommand(input);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        CassandraConnection connection = Mockito.mock(CassandraConnection.class);

        ResultSetFuture rsf = Mockito.mock(ResultSetFuture.class);
        Mockito.stub(rsf.isDone()).toReturn(true);
        ResultSet rs = Mockito.mock(ResultSet.class);
        Mockito.stub(rsf.getUninterruptibly()).toReturn(rs);
        Row row = Mockito.mock(Row.class);
        ColumnDefinitions cd = Mockito.mock(ColumnDefinitions.class);
        Mockito.stub(row.getColumnDefinitions()).toReturn(cd);
        Mockito.stub(rs.one()).toReturn(row).toReturn(null);
        
        Mockito.stub(connection.executeQuery("select 'a'")).toReturn(rsf);
        
		ResultSetExecution execution = (ResultSetExecution)cef.createExecution(command, ec, rm, connection);
        execution.execute();

        List<?> vals = execution.next();
        assertTrue(vals.get(0) instanceof Object[]);
	}
	
	@Test public void testNativeQuery() throws Exception {
		CassandraExecutionFactory cef = new CassandraExecutionFactory();
		cef.setSupportsDirectQueryProcedure(true);
		
		String input = "call proc('a', 1)";
		
        TransformationMetadata metadata = RealMetadataFactory.fromDDL("create foreign procedure proc (in x string, in y integer) options (\"teiid_rel:native-query\" 'delete from $1 where $2')", "x", "y");
		TranslationUtility util = new TranslationUtility(metadata);
        Command command = util.parseCommand(input);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        CassandraConnection connection = Mockito.mock(CassandraConnection.class);

        ResultSetFuture rsf = Mockito.mock(ResultSetFuture.class);
        Mockito.stub(connection.executeQuery("delete from 'a' where 1")).toReturn(rsf);
        
		Execution execution = cef.createExecution(command, ec, rm, connection);
        execution.execute();

        Mockito.verify(connection).executeQuery("delete from 'a' where 1");
	}
	
}
