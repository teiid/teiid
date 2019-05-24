/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.teiid.translator.odata4;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.activation.DataSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.http.HTTPBinding;

import org.apache.olingo.commons.api.edm.geo.Point;
import org.apache.olingo.commons.api.edm.provider.CsdlComplexType;
import org.apache.olingo.commons.api.edm.provider.CsdlReturnType;
import org.apache.olingo.commons.core.edm.primitivetype.EdmGeometryPoint;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.GeometryType;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.function.GeometryUtils;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.ws.WSConnection;

@SuppressWarnings({"nls", "unused"})
public class TestODataQueryExecution {

    private ResultSetExecution helpExecute(MetadataFactory mf, String query,
            final String resultJson, String expectedURL) throws Exception {
        return helpExecute(mf, query, resultJson, expectedURL, 200);
    }

    private ResultSetExecution helpExecute(MetadataFactory mf, String query,
            final String resultJson, String expectedURL, int responseCode)
            throws Exception {

        ODataExecutionFactory translator = new ODataExecutionFactory();
        translator.start();

        TranslationUtility utility = new TranslationUtility(
                TestODataMetadataProcessor.getTransformationMetadata(mf,translator));

        Command cmd = utility.parseCommand(query);
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

        ResultSetExecution execution = translator
                .createResultSetExecution((QueryExpression) cmd, context,
                        utility.createRuntimeMetadata(), connection);
        execution.execute();

        ArgumentCaptor<String> endpoint = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> binding = ArgumentCaptor.forClass(String.class);

        Mockito.verify(connection).createDispatch(binding.capture(),
                endpoint.capture(), Mockito.eq(DataSource.class),
                Mockito.eq(Mode.MESSAGE));
        assertEquals(expectedURL, URLDecoder.decode(endpoint.getValue(), "utf-8"));
        return execution;
    }

    private ProcedureExecution helpProcedureExecute(MetadataFactory mf, String query,
            final String resultJson, String expectedURL, int responseCode)
                    throws Exception {
        return helpProcedureExecute(mf, query, resultJson, expectedURL, responseCode, true);
    }

    private ProcedureExecution helpProcedureExecute(MetadataFactory mf, String query,
            final String resultJson, String expectedURL, int responseCode, boolean decode)
            throws Exception {

        ODataExecutionFactory translator = new ODataExecutionFactory();
        translator.start();

        TranslationUtility utility = new TranslationUtility(
                TestODataMetadataProcessor.getTransformationMetadata(mf,translator));

        Command cmd = utility.parseCommand(query);
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

        ProcedureExecution execution = translator
                .createProcedureExecution((Call) cmd, context,
                        utility.createRuntimeMetadata(), connection);
        execution.execute();

        ArgumentCaptor<String> endpoint = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> binding = ArgumentCaptor.forClass(String.class);

        Mockito.verify(connection).createDispatch(binding.capture(),
                endpoint.capture(), Mockito.eq(DataSource.class),
                Mockito.eq(Mode.MESSAGE));
        assertEquals(expectedURL, decode?URLDecoder.decode(endpoint.getValue(), "utf-8"):endpoint.getValue());
        return execution;
    }

    @Test
    public void testSimpleSelectNoAssosiations() throws Exception {
        String query = "SELECT UserName,FirstName,LastName FROM People";
        String expectedURL = "People?$select=UserName,FirstName,LastName";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("people.json"));
        ResultSetExecution excution = helpExecute(TestODataMetadataProcessor.tripPinMetadata(),
                query, ObjectConverterUtil.convertToString(reader), expectedURL);

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
        ResultSetExecution excution = helpExecute(TestODataMetadataProcessor.tripPinMetadata(),
                query, ObjectConverterUtil.convertToString(reader), expectedURL);

        assertArrayEquals(new String[] {"Russell@example.com", "Russell@contoso.com"},
                (String[])excution.next().get(0));
        assertArrayEquals(new String[] {"Scott@example.com"},
                (String[])excution.next().get(0));
        assertArrayEquals(new String[] {"Ronald@example.com","Ronald@contoso.com"},
                (String[])excution.next().get(0));
        reader.close();
    }

    @Test
    public void testComplexType_InnerJoin() throws Exception {
        String query = "select p.UserName, pa.Address from People p JOIN People_AddressInfo pa "
                + "ON p.UserName = pa.People_UserName";
        String expectedURL = "People?$select=UserName,AddressInfo";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("people.json"));
        ResultSetExecution excution = helpExecute(TestODataMetadataProcessor.tripPinMetadata(),
                query, ObjectConverterUtil.convertToString(reader), expectedURL);
        reader.close();

        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln."},
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"scottketchum", "2817 Milton Dr."},
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"javieralfred", "89 Jefferson Way Suite 2"},
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"vincentcalabrese", "55 Grizzly Peak Rd."},
                excution.next().toArray(new Object[2]));
        assertNull(excution.next());

    }

    @Test
    public void testComplexType_LeftOuterJoin() throws Exception {
        String query = "select p.UserName, pa.Address from People p LEFT JOIN People_AddressInfo pa "
                + "ON p.UserName = pa.People_UserName";
        String expectedURL = "People?$select=UserName,AddressInfo";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("people.json"));
        ResultSetExecution excution = helpExecute(TestODataMetadataProcessor.tripPinMetadata(),
                query, ObjectConverterUtil.convertToString(reader), expectedURL);
        reader.close();

        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln."},
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"scottketchum", "2817 Milton Dr."},
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"ronaldmundy", null},
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"javieralfred", "89 Jefferson Way Suite 2"},
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"willieashmore", null},
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"vincentcalabrese", "55 Grizzly Peak Rd."},
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"clydeguess", null},
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"keithpinckney", null},
                excution.next().toArray(new Object[2]));
        assertNull(excution.next());

    }

    @Test
    public void testComplexType_InnerJoin_3way_decendentChildren() throws Exception {
        String query = "select p.UserName, pa.Address, pc.Name from People p JOIN People_AddressInfo pa "
                + "ON p.UserName = pa.People_UserName JOIN People_AddressInfo_City pc "
                + "ON p.UserName = pc.People_UserName";
        String expectedURL = "People?$select=UserName,AddressInfo,AddressInfo/City";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("people.json"));
        ResultSetExecution excution = helpExecute(TestODataMetadataProcessor.tripPinMetadata(),
                query, ObjectConverterUtil.convertToString(reader), expectedURL);
        reader.close();

        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln.", "Boise"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"scottketchum", "2817 Milton Dr.", "Albuquerque"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"javieralfred", "89 Jefferson Way Suite 2", "Portland"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"vincentcalabrese", "55 Grizzly Peak Rd.", "Butte"},
                excution.next().toArray(new Object[3]));
        assertNull(excution.next());
    }

    @Test
    public void testComplexType_InnerJoin_3way_Sibiling() throws Exception {
        String query = "select p.UserName, pa.Address, pf.UserName from People p JOIN People_AddressInfo pa "
                + "ON p.UserName = pa.People_UserName JOIN People_Friends pf "
                + "ON p.UserName = pf.People_UserName";
        String expectedURL = "People?$select=UserName,AddressInfo&$expand=Friends($select=UserName)";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("people-friends.json"));
        ResultSetExecution excution = helpExecute(TestODataMetadataProcessor.tripPinMetadata(),
                query, ObjectConverterUtil.convertToString(reader), expectedURL);
        reader.close();

        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln.", "scottketchum"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln.", "ronaldmundy"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln.", "javieralfred"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln.", "angelhuffman"},
                excution.next().toArray(new Object[3]));

        assertArrayEquals(new Object[] {"scottketchum", "2817 Milton Dr.", "russellwhyte"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"scottketchum", "2817 Milton Dr.", "ronaldmundy"},
                excution.next().toArray(new Object[3]));

        assertArrayEquals(new Object[] {"javieralfred", "89 Jefferson Way Suite 2", "willieashmore"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"javieralfred", "89 Jefferson Way Suite 2", "vincentcalabrese"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"javieralfred", "89 Jefferson Way Suite 2", "georginabarlow"},
                excution.next().toArray(new Object[3]));
        assertNull(excution.next());
    }

    @Test
    public void testComplexType_3Way_MIXEDJoin_Sibiling() throws Exception {
        String query = "select p.UserName, pa.Address, pf.UserName from People p "
                + "LEFT OUTER JOIN People_AddressInfo pa "
                + "ON p.UserName = pa.People_UserName "
                + "INNER JOIN People_Friends pf "
                + "ON p.UserName = pf.People_UserName";
        String expectedURL = "People?$select=UserName,AddressInfo&$expand=Friends($select=UserName)";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("people-friends.json"));
        ResultSetExecution excution = helpExecute(TestODataMetadataProcessor.tripPinMetadata(),
                query, ObjectConverterUtil.convertToString(reader), expectedURL);
        reader.close();

        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln.", "scottketchum"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln.", "ronaldmundy"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln.", "javieralfred"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln.", "angelhuffman"},
                excution.next().toArray(new Object[3]));

        assertArrayEquals(new Object[] {"scottketchum", "2817 Milton Dr.", "russellwhyte"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"scottketchum", "2817 Milton Dr.", "ronaldmundy"},
                excution.next().toArray(new Object[3]));

        assertArrayEquals(new Object[] {"ronaldmundy", null, "russellwhyte"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"ronaldmundy", null, "scottketchum"},
                excution.next().toArray(new Object[3]));


        assertArrayEquals(new Object[] {"javieralfred", "89 Jefferson Way Suite 2", "willieashmore"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"javieralfred", "89 Jefferson Way Suite 2", "vincentcalabrese"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"javieralfred", "89 Jefferson Way Suite 2", "georginabarlow"},
                excution.next().toArray(new Object[3]));

        assertArrayEquals(new Object[] {"willieashmore", null, "javieralfred"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"willieashmore", null, "vincentcalabrese"},
                excution.next().toArray(new Object[3]));

        assertArrayEquals(new Object[] {"clydeguess", null, "keithpinckney"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"clydeguess", null, "ursulabright"},
                excution.next().toArray(new Object[3]));

        assertArrayEquals(new Object[] {"keithpinckney", null, "clydeguess"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"keithpinckney", null, "marshallgaray"},
                excution.next().toArray(new Object[3]));

        assertNull(excution.next());


        assertNull(excution.next());
    }

    @Test
    public void testComplexType_3Way_LeftOuterJoin() throws Exception {
        String query = "select p.UserName, pa.Address, pf.UserName from People p "
                + "LEFT OUTER JOIN People_AddressInfo pa "
                + "ON p.UserName = pa.People_UserName "
                + "LEFT OUTER JOIN People_Friends pf "
                + "ON p.UserName = pf.People_UserName";
        String expectedURL = "People?$select=UserName,AddressInfo&$expand=Friends($select=UserName)";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("people-friends.json"));
        ResultSetExecution excution = helpExecute(TestODataMetadataProcessor.tripPinMetadata(),
                query, ObjectConverterUtil.convertToString(reader), expectedURL);
        reader.close();

        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln.", "scottketchum"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln.", "ronaldmundy"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln.", "javieralfred"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln.", "angelhuffman"},
                excution.next().toArray(new Object[3]));

        assertArrayEquals(new Object[] {"scottketchum", "2817 Milton Dr.", "russellwhyte"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"scottketchum", "2817 Milton Dr.", "ronaldmundy"},
                excution.next().toArray(new Object[3]));

        assertArrayEquals(new Object[] {"ronaldmundy", null, "russellwhyte"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"ronaldmundy", null, "scottketchum"},
                excution.next().toArray(new Object[3]));


        assertArrayEquals(new Object[] {"javieralfred", "89 Jefferson Way Suite 2", "willieashmore"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"javieralfred", "89 Jefferson Way Suite 2", "vincentcalabrese"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"javieralfred", "89 Jefferson Way Suite 2", "georginabarlow"},
                excution.next().toArray(new Object[3]));

        assertArrayEquals(new Object[] {"willieashmore", null, "javieralfred"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"willieashmore", null, "vincentcalabrese"},
                excution.next().toArray(new Object[3]));

        // extra row from test from above
        assertArrayEquals(new Object[] {"vincentcalabrese", "55 Grizzly Peak Rd.", null},
                excution.next().toArray(new Object[3]));

        assertArrayEquals(new Object[] {"clydeguess", null, "keithpinckney"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"clydeguess", null, "ursulabright"},
                excution.next().toArray(new Object[3]));

        assertArrayEquals(new Object[] {"keithpinckney", null, "clydeguess"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"keithpinckney", null, "marshallgaray"},
                excution.next().toArray(new Object[3]));

        assertNull(excution.next());
    }

    @Test
    public void testExpandBasedInnerJoin() throws Exception {
        String query = "select p.UserName, pf.UserName from People p JOIN People_Friends pf "
                + "ON p.UserName = pf.People_UserName WHERE p.UserName= 'russellwhyte'";
        String expectedURL = "People?$select=UserName&$filter=UserName eq 'russellwhyte'"
                + "&$expand=Friends($select=UserName)";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("russel-friends.json"));
        ResultSetExecution excution = helpExecute(TestODataMetadataProcessor.tripPinMetadata(),
                query, ObjectConverterUtil.convertToString(reader), expectedURL);
        reader.close();

        assertArrayEquals(new Object[] {"russellwhyte", "scottketchum"},
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"russellwhyte", "ronaldmundy"},
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"russellwhyte", "javieralfred"},
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"russellwhyte", "angelhuffman"},
                excution.next().toArray(new Object[2]));
    }

    @Test
    public void testFunctionReturnsPrimitive() throws Exception {
        String query = "exec invoke(1, 'foo')";
        String expectedURL = "invoke?e1=1&e2='foo'";
        String response = "{\"value\":\"returnX\"}";

        CsdlReturnType returnType = new CsdlReturnType();
        returnType.setType("Edm.String");

        MetadataFactory mf = TestODataMetadataProcessor.functionMetadata("invoke", returnType, null);

        ProcedureExecution excution = helpProcedureExecute(mf, query, response, expectedURL, 200);

        assertArrayEquals(new Object[] {"returnX"},
                excution.getOutputParameterValues().toArray(new Object[1]));
    }

    @Test
    public void testFunctionReturnsPrimitiveEncoded() throws Exception {
        String query = "exec invoke(1, 'foo bar')";
        String expectedURL = "invoke?e1=1&e2=%27foo%20bar%27";
        String response = "{\"value\":\"returnX\"}";

        CsdlReturnType returnType = new CsdlReturnType();
        returnType.setType("Edm.String");

        MetadataFactory mf = TestODataMetadataProcessor.functionMetadata("invoke", returnType, null);

        ProcedureExecution excution = helpProcedureExecute(mf, query, response, expectedURL, 200, false);

        assertArrayEquals(new Object[] {"returnX"},
                excution.getOutputParameterValues().toArray(new Object[1]));
    }

    @Test
    public void testFunctionReturnsPrimitiveCollection() throws Exception {
        String query = "exec invoke(1, 'foo')";
        String expectedURL = "invoke?e1=1&e2='foo'";
        String response = "{\"value\": [\"returnX\", \"returnY\"]}";

        CsdlReturnType returnType = new CsdlReturnType();
        returnType.setType("Edm.String");
        returnType.setCollection(true);

        MetadataFactory mf = TestODataMetadataProcessor.functionMetadata("invoke", returnType, null);

        ProcedureExecution excution = helpProcedureExecute(mf, query, response, expectedURL, 200);

        assertArrayEquals(new Object[] {"returnX", "returnY"},
                ((List)excution.getOutputParameterValues().get(0)).toArray());

    }

    @Test
    public void testFunctionReturnsComplex() throws Exception {
        String query = "exec invoke(1, 'foo')";
        String expectedURL = "invoke?e1=1&e2='foo'";
        String response = "{\"value\":{\n" +
                "            \"street\":\"United States\",\n" +
                "            \"city\":\"Boise\",\n" +
                "            \"state\":\"ID\"\n" +
                "         }}";

        CsdlComplexType complex = TestODataMetadataProcessor.complexType("Address");
        CsdlReturnType returnType = new CsdlReturnType();
        returnType.setType("namespace.Address");
        MetadataFactory mf = TestODataMetadataProcessor.functionMetadata("invoke", returnType, complex);

        ProcedureExecution excution = helpProcedureExecute(mf, query, response, expectedURL, 200);

        assertArrayEquals(new Object[] {"United States", "Boise", "ID"},
                excution.next().toArray(new Object[3]));
        assertNull(excution.next());
    }
    @Test
    public void testFunctionReturnsComplexCollection() throws Exception {
        String query = "exec invoke(1, 'foo')";
        String expectedURL = "invoke?e1=1&e2='foo'";
        String response = "{\"value\":[{\n" +
                "            \"street\":\"United States\",\n" +
                "            \"city\":\"Boise\",\n" +
                "            \"state\":\"ID\"\n" +
                "           }," +
                "           {" +
                "            \"street\":\"China\",\n" +
                "            \"city\":\"Newyork\",\n" +
                "            \"state\":\"NY\"\n" +
                "         }]}";

        CsdlComplexType complex = TestODataMetadataProcessor.complexType("Address");
        CsdlReturnType returnType = new CsdlReturnType();
        returnType.setType("namespace.Address");
        MetadataFactory mf = TestODataMetadataProcessor.functionMetadata("invoke", returnType, complex);

        ProcedureExecution excution = helpProcedureExecute(mf, query, response, expectedURL, 200);

        assertArrayEquals(new Object[] {"United States", "Boise", "ID"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"China", "Newyork", "NY"},
                excution.next().toArray(new Object[3]));
        assertNull(excution.next());
    }

    @Test
    public void testActionReturnsComplexCollection() throws Exception {
        String query = "exec invoke(1, 'foo')";
        String expectedURL = "invoke";
        String response = "{\"value\":[{\n" +
                "            \"street\":\"United States\",\n" +
                "            \"city\":\"Boise\",\n" +
                "            \"state\":\"ID\"\n" +
                "           }," +
                "           {" +
                "            \"street\":\"China\",\n" +
                "            \"city\":\"Newyork\",\n" +
                "            \"state\":\"NY\"\n" +
                "         }]}";

        CsdlComplexType complex = TestODataMetadataProcessor.complexType("Address");
        CsdlReturnType returnType = new CsdlReturnType();
        returnType.setType("namespace.Address");
        MetadataFactory mf = TestODataMetadataProcessor.actionMetadata("invoke", returnType, complex);

        ProcedureExecution excution = helpProcedureExecute(mf, query, response, expectedURL, 200);

        assertArrayEquals(new Object[] {"United States", "Boise", "ID"},
                excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {"China", "Newyork", "NY"},
                excution.next().toArray(new Object[3]));
        assertNull(excution.next());
    }

    @Test
    public void testReadComplexType() throws Exception {
        String query = "select pa.People_UserName, pa.Address from People_AddressInfo pa ";
        String expectedURL = "People?$select=UserName,AddressInfo";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("people.json"));
        ResultSetExecution excution = helpExecute(TestODataMetadataProcessor.tripPinMetadata(),
                query, ObjectConverterUtil.convertToString(reader), expectedURL);
        reader.close();

        assertArrayEquals(new Object[] {"russellwhyte", "187 Suffolk Ln."},
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"scottketchum", "2817 Milton Dr."},
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"javieralfred", "89 Jefferson Way Suite 2"},
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] {"vincentcalabrese", "55 Grizzly Peak Rd."},
                excution.next().toArray(new Object[2]));
        assertNull(excution.next());

    }

    @Test
    public void testGeometry() throws Exception {
        String query = "SELECT * FROM Airports_Location";
        String expectedURL = "Airports?$select=IcaoCode,Location";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("airport-locations.json"));
        MetadataFactory mf = TestODataMetadataProcessor.tripPinMetadata();
        ResultSetExecution execution = helpExecute(TestODataMetadataProcessor.tripPinMetadata(),
                query, ObjectConverterUtil.convertToString(reader), expectedURL);

        List<?> row = execution.next();

        assertEquals("187 Suffolk Ln.", row.get(2));
        assertEquals("xyz", row.get(0));

        GeometryType gis = (GeometryType)row.get(1);
        assertEquals("SRID=4326;POINT (-48.23456 20.12345)", ClobType.getString((GeometryUtils.geometryToClob(gis, true))));
        //assertEquals(4326, gis.getSrid());

        row = execution.next();

        assertEquals("gso", row.get(0));

        gis = (GeometryType)row.get(1);
        assertEquals("SRID=4326;POINT (1 2)", ClobType.getString((GeometryUtils.geometryToClob(gis, true))));

        assertNull(execution.next());

        reader.close();

    }

    @Test
    public void testGeometryFilter() throws Exception {
        String query = "SELECT Loc FROM Airports_Location where st_distance(Loc, st_geomfromtext('point(1 2)')) < 2";
        String expectedURL = "Airports?$select=Location&$filter=geo.distance(Location/Loc,geometry'SRID=0;Point(1.0 2.0)') lt 2.0";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("airport-locations.json"));
        ResultSetExecution execution = helpExecute(TestODataMetadataProcessor.tripPinMetadata(),
                query, ObjectConverterUtil.convertToString(reader), expectedURL);

        //make sure the format is valid
        EdmGeometryPoint.getInstance().valueOfString("geometry'SRID=0;Point(1.0 2.0)'", false, 4000, 0, 0, true, Point.class);
    }

    @Test
    public void testSimpleAggregate() throws Exception {
        String query = "SELECT Count(*) FROM People";
        String expectedURL = "People/$count";

        ResultSetExecution excution = helpExecute(TestODataMetadataProcessor.tripPinMetadata(),
                query, "19", expectedURL);

        assertEquals(Arrays.asList(19),
                excution.next());
    }

}
