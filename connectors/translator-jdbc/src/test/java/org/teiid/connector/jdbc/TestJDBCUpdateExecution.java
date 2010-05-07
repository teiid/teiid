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

package org.teiid.connector.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.connector.language.Command;
import org.teiid.connector.language.ExpressionValueSource;
import org.teiid.connector.language.Insert;
import org.teiid.connector.language.Literal;
import org.teiid.resource.adapter.jdbc.JDBCExecutionFactory;
import org.teiid.resource.adapter.jdbc.JDBCUpdateExecution;
import org.teiid.resource.cci.ExecutionContext;
import org.teiid.translator.jdbc.Translator;

public class TestJDBCUpdateExecution {

	@Test public void testBulkUpdate() throws Exception {
		Command command = TranslationHelper.helpTranslate(TranslationHelper.BQT_VDB, "insert into BQT1.SmallA (IntKey, IntNum) values (1, 2)"); //$NON-NLS-1$
		Literal value = ((Literal)((ExpressionValueSource)((Insert)command).getValueSource()).getValues().get(0));
		Literal value1 = ((Literal)((ExpressionValueSource)((Insert)command).getValueSource()).getValues().get(1));
		value.setMultiValued(true);
		value.setBindValue(true);
		value.setValue(Arrays.asList(1, 2));
		value1.setMultiValued(true);
		value1.setBindValue(true);
		value1.setValue(Arrays.asList(2, 3));
		Connection connection = Mockito.mock(Connection.class);
		PreparedStatement p = Mockito.mock(PreparedStatement.class);
		Mockito.stub(p.executeBatch()).toReturn(new int [] {1, 1});
		Mockito.stub(connection.prepareStatement("INSERT INTO SmallA (IntKey, IntNum) VALUES (?, ?)")).toReturn(p); //$NON-NLS-1$
		
		JDBCExecutionFactory config = Mockito.mock(JDBCExecutionFactory.class);
		Mockito.stub(config.getTranslator()).toReturn(new Translator());
		
		JDBCUpdateExecution updateExecution = new JDBCUpdateExecution(command, connection, Mockito.mock(ExecutionContext.class), config, config.getTranslator());
		updateExecution.execute();
		Mockito.verify(p, Mockito.times(2)).addBatch();
	}
	
}
