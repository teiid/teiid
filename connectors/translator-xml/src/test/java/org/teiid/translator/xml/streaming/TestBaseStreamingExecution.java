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
package org.teiid.translator.xml.streaming;

import java.util.Map;

import javax.xml.ws.handler.MessageContext;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Select;
import org.teiid.metadata.index.VDBMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.xml.XMLExecutionFactory;

import com.metamatrix.cdk.api.TranslationUtility;

import static org.junit.Assert.*;

@SuppressWarnings("nls")
public class TestBaseStreamingExecution {

	@Test
	public void testXMLGeneration() throws Exception {
		
		XMLExecutionFactory factory  = new XMLExecutionFactory();
		
		String vdbPath = UnitTestUtil.getTestDataPath()+"/cis.vdb";
		TranslationUtility util = new TranslationUtility(VDBMetadataFactory.getVDBMetadata(vdbPath));
		
		String sql = "SELECT FullCountryInfoSoapRequest.ResponseOut FROM FullCountryInfoSoapRequest WHERE FullCountryInfoSoapRequest.sCountryISOCode = 'US'";
		
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		Mockito.stub(context.getExecutionCountIdentifier()).toReturn("1.1");
		
		ResultSetExecution exec = factory.createResultSetExecution((Select)util.parseCommand(sql), context, util.createRuntimeMetadata(), TestSoapExecution.getCF());
		assertTrue(exec instanceof BaseStreamingExecution);
		
		BaseStreamingExecution bse = (BaseStreamingExecution)exec;
		String soapRequest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><tns:FullCountryInfo xmlns:tns=\"http://www.oorsprong.org/websamples.countryinfo\"><tns:sCountryISOCode>US</tns:sCountryISOCode></tns:FullCountryInfo>";
		assertEquals(soapRequest, TestSoapExecution.getContent(bse.soapPayload));
		
		Map<String, Object> map = (Map)bse.dispatch.getRequestContext().get(MessageContext.INBOUND_MESSAGE_ATTACHMENTS);
		
		String xmlRequest = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\r\n"+
							"<SLASH xmlns:mmn0=\"http://www.oorsprong.org/websamples.countryinfo\" xmlns:tns=\"http://www.oorsprong.org/websamples.countryinfo\"><tns:FullCountryInfo><tns:sCountryISOCode>US</tns:sCountryISOCode></tns:FullCountryInfo></SLASH>";	
		assertEquals(xmlRequest, map.get("xml"));
		
		assertEquals("tns%3AFullCountryInfo%2Ftns%3AsCountryISOCode=US&", bse.dispatch.getRequestContext().get(MessageContext.QUERY_STRING));
		
		// response doc; nothing gets generated for this guy..
		sql = "SELECT * FROM FullCountryInfoSoapResponse WHERE FullCountryInfoSoapResponse.ResponseIn = '1.1'";
		exec = factory.createResultSetExecution((Select)util.parseCommand(sql), context, util.createRuntimeMetadata(), TestSoapExecution.getCF());
		assertTrue(exec instanceof BaseStreamingExecution);
		bse = (BaseStreamingExecution)exec;
		assertNull(bse.soapPayload);
		assertNull(bse.dispatch.getRequestContext().get(MessageContext.INBOUND_MESSAGE_ATTACHMENTS));
		assertNull(bse.dispatch.getRequestContext().get(MessageContext.QUERY_STRING));
	}
	

}
