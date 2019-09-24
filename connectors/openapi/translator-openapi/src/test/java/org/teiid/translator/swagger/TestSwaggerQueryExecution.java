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
package org.teiid.translator.swagger;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import javax.activation.DataSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.http.HTTPBinding;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.ws.WSConnection;

@SuppressWarnings({ "nls", "unused" })
public class TestSwaggerQueryExecution {

    private ProcedureExecution helpProcedureExecute(String query,
            final String resultJson, String expectedURL, int responseCode)
            throws Exception {
        Map<String, Object> headers = new HashMap<String, Object>();

        return helpProcedureExecute(query, resultJson, expectedURL,
                responseCode, true, "GET", null, headers);
    }

    private ProcedureExecution helpProcedureExecute(String query,
            final String resultJson, String expectedURL, int responseCode,
            boolean decode, String expectedMethod, String expectedInput,
            Map<String, Object> userHeaders) throws Exception {

        userHeaders.put(MessageContext.HTTP_REQUEST_HEADERS,
                new HashMap<String, List<String>>());
        userHeaders.put(WSConnection.STATUS_CODE, new Integer(responseCode));
        userHeaders.put("Content-Type", Arrays.asList("application/json"));

        SwaggerExecutionFactory translator = new SwaggerExecutionFactory();
        translator.start();

        TranslationUtility utility = new TranslationUtility(
                TestSwaggerMetadataProcessor
                        .getTransformationMetadata(TestSwaggerMetadataProcessor
                                .petstoreMetadata(translator), translator));

        Command cmd = utility.parseCommand(query);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);
        WSConnection connection = Mockito.mock(WSConnection.class);

        Dispatch<DataSource> dispatch = Mockito.mock(Dispatch.class);
        Mockito.stub(dispatch.getRequestContext()).toReturn(userHeaders);
        Mockito.stub(dispatch.getResponseContext()).toReturn(userHeaders);

        Mockito.stub(connection.createDispatch(
                Mockito.eq(HTTPBinding.HTTP_BINDING), Mockito.anyString(),
                Mockito.eq(DataSource.class), Mockito.eq(Mode.MESSAGE)))
                .toReturn(dispatch);

        DataSource outputDS = new DataSource() {
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
                ByteArrayInputStream in = new ByteArrayInputStream(
                        resultJson.getBytes());
                return in;
            }

            @Override
            public String getContentType() {
                return "application/json";
            }
        };
        Mockito.stub(dispatch.invoke(Mockito.any(DataSource.class)))
                .toReturn(outputDS);

        ProcedureExecution execution = translator.createProcedureExecution(
                (Call) cmd, context, utility.createRuntimeMetadata(),
                connection);
        execution.execute();

        ArgumentCaptor<String> endpoint = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> binding = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<DataSource> input = ArgumentCaptor
                .forClass(DataSource.class);

        Mockito.verify(connection).createDispatch(binding.capture(),
                endpoint.capture(), Mockito.eq(DataSource.class),
                Mockito.eq(Mode.MESSAGE));
        Mockito.verify(dispatch).invoke(input.capture());

        assertEquals(expectedURL,
                decode ? URLDecoder.decode(endpoint.getValue(), "utf-8")
                        : endpoint.getValue());
        assertEquals(expectedMethod, dispatch.getRequestContext()
                .get(MessageContext.HTTP_REQUEST_METHOD));
        if (expectedInput != null) {
            assertEquals(expectedInput, ObjectConverterUtil
                    .convertToString(input.getValue().getInputStream()));
        }
        return execution;
    }

    @Test
    public void testSimpleInputArray() throws Exception {
        String query = "exec findPetsByStatus(status=>('available',));";
        String expectedURL = "http://petstore.swagger.io/v2/pet/findByStatus?status=available";
        String response = "[\n"
                + "  {\n" + "\"id\": 7,\n"
                + "    \"category\": {\n"
                + "         \"id\": 4,\n"
                + "         \"name\": \"Lions\"\n"
                + "    },\n"
                + "    \"name\": \"Lion 1\",\n"
                + "    \"photoUrls\": [\n"
                + "      \"url1\",\n"
                + "      \"url2\"\n"
                + "    ],\n"
                + "    \"tags\": [\n"
                + "      {\n"
                + "        \"id\": 1,\n"
                + "        \"name\": \"tag1\"\n"
                + "      },\n"
                + "      {\n"
                + "        \"id\": 2,\n"
                + "        \"name\": \"tag2\"\n"
                + "      }\n"
                + "    ],\n"
                + "    \"status\": \"available\"\n"
                + "  },\n"
                + "  {\n"
                + "    \"id\": 10008,\n"
                + "    \"category\": {\n"
                + "      \"id\": 0,\n"
                + "      \"name\": \"string\"\n"
                + "    },\n"
                + "    \"name\": \"doggie\",\n"
                + "    \"photoUrls\": [\n"
                + "      \"string\"\n"
                + "    ],\n"
                + "    \"tags\": [\n"
                + "      {\n"
                + "        \"id\": 0,\n"
                + "        \"name\": \"string\"\n"
                + "      }\n"
                + "    ],\n"
                + "    \"status\": \"available\"\n"
                + "  }]";

        ProcedureExecution excution = helpProcedureExecute(query, response,
                expectedURL, 200);

        assertArrayEquals(new Object[] { 7L, 4L, "Lions", "Lion 1",
                new String[] { "url1", "url2" }, 1L, "tag1", "available" },
                excution.next().toArray(new Object[8]));

        assertArrayEquals(new Object[] { 7L, 4L, "Lions", "Lion 1",
                new String[] { "url1", "url2" }, 2L, "tag2", "available" },
                excution.next().toArray(new Object[8]));

        assertArrayEquals(
                new Object[] { 10008L, 0L, "string", "doggie",
                        new String[] { "string" }, 0L, "string", "available" },
                excution.next().toArray(new Object[8]));

        assertNull(excution.next());
    }

    @Test
    public void testPostBasedQuery() throws Exception {
        String query = "exec addPet(id=>99, category_id=>0,category_name=>'canine',name=>'nikky',"
                + "photoUrls=>('photo1','photo2'),tags_tag_id=>0, tags_tag_name=>'doggie',"
                + "status=>'available');";
        String expectedURL = "http://petstore.swagger.io/v2/pet";
        String response = "{"
                    + "\"id\":99,"
                    + "\"name\":\"nikky\","
                    + "\"photoUrls\":[\"photo1\",\"photo2\"],"
                    + "\"status\":\"available\","
                    + "\"category\":{"
                        + "\"id\":0,"
                        + "\"name\":\"canine\""
                     + "},"
                    + "\"tags\":["
                        + "{\"id\":0,"
                        + "\"name\":\"doggie\""
                        + "}"
                    + "]}";

        ProcedureExecution excution = helpProcedureExecute(query, response,
                expectedURL, 200, true, "POST", response, getHeaders());
    }

    @Test
    public void testMapReturn() throws Exception {
        String query = "exec getInventory();";
        String expectedURL = "http://petstore.swagger.io/v2/store/inventory";
        String response = "{\n"
                + "  \"sold\": 6,\n"
                + "  \"string\": 7,\n"
                + "  \"pending\": 62,\n"
                + "  \"available\": 891,\n"
                + "  \"Live\": 7,\n"
                + "  \"fulfilled\": 1\n"
                + "}";

        ProcedureExecution excution = helpProcedureExecute(query, response,
                expectedURL, 200, true, "GET", null, getHeaders());
        assertArrayEquals(new Object[] { "sold", 6 },
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] { "string", 7 },
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] { "pending", 62 },
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] { "available", 891 },
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] { "Live", 7 },
                excution.next().toArray(new Object[2]));
        assertArrayEquals(new Object[] { "fulfilled", 1 },
                excution.next().toArray(new Object[2]));
        assertNull(excution.next());
    }

    @Test
    public void testHeadersInResponseWithReturn() throws Exception {
        String query = "exec loginUser(username=>'foo',password=>'bar');";
        String expectedURL = "http://petstore.swagger.io/v2/user/login?username=foo&password=bar";
        String response = "sucess";
        TimeZone tz = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
            ProcedureExecution excution = helpProcedureExecute(query, response,
                    expectedURL, 200, true, "GET", null, getHeaders());
            assertArrayEquals(new Object[] { "sucess", 1, new Timestamp(1460110463000L) },
                    excution.next().toArray(new Object[3]));
            assertNull(excution.next());
        } finally {
            TimeZone.setDefault(tz);
        }
    }

    @Test
    public void testParameterInPath() throws Exception {
        String query = "exec getPetById(petId=>687789);";
        String expectedURL = "http://petstore.swagger.io/v2/pet/687789";
        String response = "{\n" +
                "  \"id\": 687789,\n" +
                "  \"category\": {\n" +
                "    \"id\": 0,\n" +
                "    \"name\": \"Lions\"\n" +
                "  },\n" +
                "  \"name\": \"nikky\",\n" +
                "  \"photoUrls\": [\n" +
                "    \"url1\"\n" +
                "  ],\n" +
                "  \"tags\": [\n" +
                "    {\n" +
                "      \"id\": 1,\n" +
                "      \"name\": \"tag1\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"status\": \"sold\"\n" +
                "}";

        ProcedureExecution excution = helpProcedureExecute(query, response,
                expectedURL, 200, true, "GET", null, getHeaders());
        assertArrayEquals(new Object[] { 687789L, 0L, "Lions", "nikky",
                new String[] { "url1"}, 1L, "tag1", "sold" },
                excution.next().toArray(new Object[8]));
        assertNull(excution.next());
    }

    @Test(expected=TranslatorException.class)
    public void testErrorResponse() throws Exception {
        String query = "exec getPetById(petId=>687789);";
        String expectedURL = "http://petstore.swagger.io/v2/pet/687789";
        String response = "";

        ProcedureExecution excution = helpProcedureExecute(query, response,
                expectedURL, 400, true, "GET", null, getHeaders());
        assertNull(excution.next());
    }

    @Test
    public void testDefaultResponse() throws Exception {
        String query = "exec logoutUser();";
        String expectedURL = "http://petstore.swagger.io/v2/user/logout";
        String response = "";

        ProcedureExecution excution = helpProcedureExecute(query, response,
                expectedURL, 200, true, "GET", null, getHeaders());
        assertNull(excution.next());
    }

    @Test
    public void testScalarResponse() throws Exception {
        String query = "exec loginStatus('foo');";
        String expectedURL = "http://petstore.swagger.io/v2/user/loginStatus?username=foo";
        String response = "foo";

        ProcedureExecution excution = helpProcedureExecute(query, response,
                expectedURL, 200, true, "GET", null, getHeaders());
        assertEquals("foo", excution.getOutputParameterValues().get(0));
    }

    @Test(expected=TeiidRuntimeException.class)
    public void testNullParameter() throws Exception {
        String query = "exec loginStatus(null);";
        String expectedURL = null;
        String response = null;

        ProcedureExecution excution = helpProcedureExecute(query, response,
                expectedURL, 200, true, "GET", null, getHeaders());
    }

    @Test
    public void testOptionalParameter() throws Exception {
        String query = "exec updatePet(name=>'fido');";
        String expectedURL = "http://petstore.swagger.io/v2/pet";
        String response = "";

        ProcedureExecution excution = helpProcedureExecute(query, response,
                expectedURL, 200, true, "PUT", null, getHeaders());
    }

    private Map<String, Object> getHeaders() {
        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put(MessageContext.HTTP_REQUEST_HEADERS,
                new HashMap<String, List<String>>());
        headers.put(WSConnection.STATUS_CODE, new Integer(200));
        headers.put("Content-Type", "application/json");
        headers.put("X-Rate-Limit", "1");
        headers.put("X-Expires-After", "2016-04-08T10:14:23Z");
        return headers;
    }

    @Test
    public void testGetSerializer(){
        assertTrue(SwaggerProcedureExecution.getSerializer("application/json") instanceof JsonSerializer);
        assertTrue(SwaggerProcedureExecution.getSerializer("application/json;charset=utf-8") instanceof JsonSerializer);
    }

    @Test(expected=AssertionError.class)
    public void testGetSerializerFails() {
        SwaggerProcedureExecution.getSerializer("application/xml");
    }

}
