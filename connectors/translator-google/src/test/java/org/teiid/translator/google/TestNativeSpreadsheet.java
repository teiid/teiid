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

package org.teiid.translator.google;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.language.Command;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.resource.adapter.google.GoogleSpreadsheetConnection;
import org.teiid.resource.adapter.google.common.SheetRow;
import org.teiid.resource.adapter.google.result.RowsResult;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;

public class TestNativeSpreadsheet {
	
	@Test public void testDirect() throws TranslatorException {
		SpreadsheetExecutionFactory sef = new SpreadsheetExecutionFactory();
		sef.setSupportsDirectQueryProcedure(true);
		
		String input = "call native('worksheet=x;query=$1 foo;limit=2', 'a')";
		
        TranslationUtility util = FakeTranslationFactory.getInstance().getExampleTranslationUtility();
        Command command = util.parseCommand(input);
        ExecutionContext ec = Mockito.mock(ExecutionContext.class);
        RuntimeMetadata rm = Mockito.mock(RuntimeMetadata.class);
        GoogleSpreadsheetConnection connection = Mockito.mock(GoogleSpreadsheetConnection.class);

        RowsResult result = Mockito.mock(RowsResult.class);
        Mockito.stub(result.iterator()).toReturn(Arrays.asList(new SheetRow()).iterator());
        Mockito.stub(connection.executeQuery("x", "'a' foo", null, 2, 0)).toReturn(result);
        
		ResultSetExecution execution = (ResultSetExecution)sef.createExecution(command, ec, rm, connection);
        execution.execute();

        List<?> vals = execution.next();
        assertTrue(vals.get(0) instanceof Object[]);
	}

}
