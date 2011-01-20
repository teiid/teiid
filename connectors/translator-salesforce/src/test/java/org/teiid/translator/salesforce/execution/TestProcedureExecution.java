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

package org.teiid.translator.salesforce.execution;

import static org.junit.Assert.*;

import java.util.Arrays;

import javax.xml.datatype.XMLGregorianCalendar;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Call;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.execution.visitors.TestVisitors;

@SuppressWarnings("nls")
public class TestProcedureExecution {
	
	private static TranslationUtility translationUtility = new TranslationUtility(TestVisitors.exampleSalesforce());

	@Test public void testProcedureName() throws Exception {
		Call command = (Call)translationUtility.parseCommand("exec getupdated('foo', {d '1970-01-01'}, {d '1990-01-01'})"); //$NON-NLS-1$
		SalesforceConnection sfc = Mockito.mock(SalesforceConnection.class);
		UpdatedResult ur = new UpdatedResult();
		ur.setIDs(Arrays.asList("1", "2"));
		Mockito.stub(sfc.getUpdated(Mockito.eq("foo"), (XMLGregorianCalendar)Mockito.anyObject(), (XMLGregorianCalendar)Mockito.anyObject())).toReturn(ur);
		ProcedureExecutionParentImpl pepi = new ProcedureExecutionParentImpl(command, sfc, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class));
		pepi.execute();
		assertNotNull(pepi.next());
		assertNotNull(pepi.next());
		assertNull(pepi.next());
		pepi.close();
	}
	
}
