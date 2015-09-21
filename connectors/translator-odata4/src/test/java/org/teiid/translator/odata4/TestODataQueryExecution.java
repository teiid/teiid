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
package org.teiid.translator.odata4;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.activation.DataSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.http.HTTPBinding;

import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.MetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.WSConnection;

@SuppressWarnings({"nls", "unused"})
public class TestODataQueryExecution {
	private ODataExecutionFactory translator;
	private TranslationUtility utility;
	
    @Before
    public void setUp() throws Exception {
        this.translator = new ODataExecutionFactory();
        this.translator.start();

        MetadataFactory mf = TestODataMetadataProcessor.tripPinMetadata();
        this.utility = new TranslationUtility(
                TestODataMetadataProcessor.getTransformationMetadata(mf,this.translator));
    }

    private ResultSetExecution helpExecute(String query,
            final String resultJson, String expectedURL) throws Exception {
        return helpExecute(query, resultJson, expectedURL, 200);
    }
	
    private ResultSetExecution helpExecute(String query,
            final String resultJson, String expectedURL, int responseCode)
            throws Exception {
		Command cmd = this.utility.parseCommand(query);
		ExecutionContext context = Mockito.mock(ExecutionContext.class);
		WSConnection connection = Mockito.mock(WSConnection.class);
		
		Map<String, Object> headers = new HashMap<String, Object>();
		headers.put(MessageContext.HTTP_REQUEST_HEADERS, new HashMap<String, List<String>>());
		headers.put(WSConnection.STATUS_CODE, new Integer(responseCode));
		
		Dispatch<DataSource> dispatch = Mockito.mock(Dispatch.class);
		Mockito.stub(dispatch.getRequestContext()).toReturn(headers);
		Mockito.stub(dispatch.getResponseContext()).toReturn(headers);
		
		Mockito.stub(connection.createDispatch(Mockito.eq(HTTPBinding.HTTP_BINDING), Mockito.anyString(), 
		        Mockito.eq(DataSource.class), Mockito.eq(Mode.MESSAGE))).toReturn(dispatch);
		
		DataSource ds = new DataSource() {
			@Override
			public OutputStream getOutputStream() throws IOException {
				return new ByteArrayOutputStream();
			}
			@Override
			public String getName() {
				return "result";
			}
			@Override
			public InputStream getInputStream() throws IOException {
				ByteArrayInputStream in = new ByteArrayInputStream(resultJson.getBytes());
				return in;
			}
			@Override
			public String getContentType() {
				return "application/xml";
			}
		};
		Mockito.stub(dispatch.invoke(Mockito.any(DataSource.class))).toReturn(ds);
		
        ResultSetExecution execution = this.translator
                .createResultSetExecution((QueryExpression) cmd, context,
                        this.utility.createRuntimeMetadata(), connection);
		execution.execute();
		
		ArgumentCaptor<String> endpoint = ArgumentCaptor.forClass(String.class);
		ArgumentCaptor<String> binding = ArgumentCaptor.forClass(String.class);
		
        Mockito.verify(connection).createDispatch(binding.capture(),
                endpoint.capture(), Mockito.eq(DataSource.class),
                Mockito.eq(Mode.MESSAGE));
		assertEquals(expectedURL, URLDecoder.decode(endpoint.getValue(), "utf-8"));
		return execution;
	}

	@Test
	public void testSimpleSelectNoAssosiations() throws Exception {
		String query = "SELECT UserName,FirstName,LastName FROM People";
		String expectedURL = "People?$select=UserName,FirstName,LastName";
		
		FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("people.json"));
		ResultSetExecution excution = helpExecute(query, ObjectConverterUtil.convertToString(reader), expectedURL);
		
		assertArrayEquals(new Object[] {"russellwhyte", "Russell", "Whyte"}, 
		        excution.next().toArray(new Object[3]));
		assertArrayEquals(new Object[] {"scottketchum", "Scott", "Ketchum"}, 
		        excution.next().toArray(new Object[3]));
		assertArrayEquals(new Object[] {"ronaldmundy", "Ronald", "Mundy"}, 
		        excution.next().toArray(new Object[3]));
		reader.close();
	}
	
    @Test
    public void testReadArray() throws Exception {
        String query = "SELECT Emails FROM People";
        String expectedURL = "People?$select=Emails";
        
        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("people.json"));
        ResultSetExecution excution = helpExecute(query, ObjectConverterUtil.convertToString(reader), expectedURL);

        assertArrayEquals(new String[] {"Russell@example.com", "Russell@contoso.com"}, 
                (String[])excution.next().get(0));        
        assertArrayEquals(new String[] {"Scott@example.com"}, 
                (String[])excution.next().get(0));
        assertArrayEquals(new String[] {"Ronald@example.com","Ronald@contoso.com"}, 
                (String[])excution.next().get(0));
        reader.close();
    }	

	@Test
	public void testComplexType() throws Exception {
		String query = "select p.UserName, pa.Address from People p JOIN People_AddressInfo pa "
		        + "ON p.UserName = pa.People_UserName";
		String expectedURL = "People?$select=UserName,AddressInfo";
		
		FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("people.json"));
		ResultSetExecution excution = helpExecute(query, ObjectConverterUtil.convertToString(reader), expectedURL);
		reader.close();

		assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln."}, 
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"scottketchum", "2817 Milton Dr."}, 
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"ronaldmundy", null}, 
                excution.next().toArray(new Object[2]));
	}
	
    @Test
    public void testComplexType2() throws Exception {
        String query = "select p.UserName, pa.Address, pc.Name from People p JOIN People_AddressInfo pa "
                + "ON p.UserName = pa.People_UserName JOIN People_AddressInfo_City pc "
                + "ON p.UserName = pc.People_UserName";
        String expectedURL = "People?$select=UserName,AddressInfo,AddressInfo/City";
        
        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("people.json"));
        ResultSetExecution excution = helpExecute(query, ObjectConverterUtil.convertToString(reader), expectedURL);
        reader.close();

        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln.", "Boise"}, 
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"scottketchum", "2817 Milton Dr.", "Albuquerque"}, 
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"ronaldmundy", null, null}, 
                excution.next().toArray(new Object[3]));
        
    }   
}
