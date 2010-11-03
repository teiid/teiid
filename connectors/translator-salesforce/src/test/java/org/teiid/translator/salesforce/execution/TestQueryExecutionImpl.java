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

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.language.Select;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.salesforce.SalesforceConnection;
import org.teiid.translator.salesforce.execution.visitors.TestVisitors;
import org.w3c.dom.Element;

import com.sforce.soap.partner.QueryResult;
import com.sforce.soap.partner.sobject.SObject;

@SuppressWarnings("nls")
public class TestQueryExecutionImpl {
	
	private static TranslationUtility translationUtility = new TranslationUtility(TestVisitors.exampleSalesforce());

	@Test public void testBatching() throws Exception {
		Select command = (Select)translationUtility.parseCommand("select Name from Account"); //$NON-NLS-1$
		SalesforceConnection sfc = Mockito.mock(SalesforceConnection.class);
		QueryResult qr = new QueryResult();
		SObject so = new SObject();
		so.setType("Account");
		Element elem = Mockito.mock(Element.class);
		Mockito.stub(elem.getLocalName()).toReturn("AccountName");
		so.getAny().add(elem);
		qr.getRecords().add(so);
		qr.setDone(false);
		QueryResult finalQr = new QueryResult();
		so.getAny().add(elem);
		finalQr.getRecords().add(so);
		finalQr.setDone(true);
		Mockito.stub(sfc.query("SELECT Account.AccountName FROM Account", 0, false)).toReturn(qr);
		Mockito.stub(sfc.queryMore(null, 0)).toReturn(finalQr);
		QueryExecutionImpl qei = new QueryExecutionImpl(command, sfc, Mockito.mock(RuntimeMetadata.class), Mockito.mock(ExecutionContext.class));
		qei.execute();
		assertNotNull(qei.next());
		assertNotNull(qei.next());
		assertNull(qei.next());
	}
	
}
