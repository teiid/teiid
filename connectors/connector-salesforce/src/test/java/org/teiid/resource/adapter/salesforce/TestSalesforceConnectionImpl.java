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

package org.teiid.resource.adapter.salesforce;

import static org.junit.Assert.*;

import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.translator.salesforce.execution.DeletedResult;

import com.sforce.soap.partner.DeletedRecord;
import com.sforce.soap.partner.GetDeletedResult;
import com.sforce.soap.partner.Soap;

@SuppressWarnings("nls")
public class TestSalesforceConnectionImpl {
	
	@Test public void testGetDeleted() throws Exception {
		Soap soap = Mockito.mock(Soap.class);
		GetDeletedResult gdr = new GetDeletedResult();
		XMLGregorianCalendar c = DatatypeFactory.newInstance().newXMLGregorianCalendar();
		gdr.setEarliestDateAvailable(c);
		gdr.setLatestDateCovered(c);
		DeletedRecord dr = new DeletedRecord();
		dr.setDeletedDate(c);
		dr.setId("id");
		gdr.getDeletedRecords().add(dr);
		Mockito.stub(soap.getDeleted("x", null, null)).toReturn(gdr);
		SalesforceConnectionImpl sfci = new SalesforceConnectionImpl(soap);
		DeletedResult result = sfci.getDeleted("x", null, null);
		assertEquals(1, result.getResultRecords().size());
	}

}
