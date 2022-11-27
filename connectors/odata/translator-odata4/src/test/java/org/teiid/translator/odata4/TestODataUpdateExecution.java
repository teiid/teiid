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

import org.apache.olingo.commons.api.edm.provider.CsdlComplexType;
import org.apache.olingo.commons.api.edm.provider.CsdlReturnType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.types.InputStreamFactory.ClobInputStreamFactory;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Call;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.MetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ProcedureExecution;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.ws.WSConnection;

@SuppressWarnings({"nls", "unused"})
public class TestODataUpdateExecution {

    private UpdateExecution helpExecute(MetadataFactory mf, String query, String expectedPayload,
            final String resultJson, String expectedURL, String expectedMethod, int responseCode)
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
                return "application/json";
            }
        };
        ArgumentCaptor<DataSource> data = ArgumentCaptor.forClass(DataSource.class);
        Mockito.stub(dispatch.invoke(data.capture())).toReturn(ds);

        UpdateExecution execution = translator
                .createUpdateExecution(cmd, context,
                        utility.createRuntimeMetadata(), connection);
        execution.execute();

        ArgumentCaptor<String> endpoint = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> binding = ArgumentCaptor.forClass(String.class);


        Mockito.verify(connection).createDispatch(binding.capture(),
                endpoint.capture(), Mockito.eq(DataSource.class),
                Mockito.eq(Mode.MESSAGE));
        assertEquals(expectedURL, URLDecoder.decode(endpoint.getValue(), "utf-8"));
        String payload = new String(ObjectConverterUtil.convertToByteArray(
                ((ClobInputStreamFactory)data.getValue()).getInputStream()));
        assertEquals(expectedPayload, payload);
        assertEquals(expectedMethod, dispatch.getRequestContext().get(MessageContext.HTTP_REQUEST_METHOD));
        return execution;
    }


    @Test
    public void testInsertEntitySet() throws Exception {
        String query = "INSERT INTO People(UserName,FirstName,LastName, EMails, Gender, Concurrency) "
                + "values ('jdoe', 'John', 'Doe', ('jdoe@cantfind.ws',), 'Male', 1234)";
        String expectedURL = "People";
        String returnResponse = "{\n" +
                "   \"UserName\":\"russellwhyte\",\n" +
                "   \"FirstName\":\"Russell\",\n" +
                "   \"LastName\":\"Whyte\"\n" +
                "}";
        String expectedPayload = "{\"@odata.type\":\"#Microsoft.OData.SampleService.Models.TripPin.Person\","
                + "\"UserName@odata.type\":\"String\",\"UserName\":\"jdoe\","
                + "\"FirstName@odata.type\":\"String\",\"FirstName\":\"John\","
                + "\"LastName@odata.type\":\"String\",\"LastName\":\"Doe\","
                + "\"Emails@odata.type\":\"#Collection(String)\",\"Emails\":[\"jdoe@cantfind.ws\"],"
                + "\"Gender@odata.type\":\"#Microsoft.OData.SampleService.Models.TripPin.PersonGender\","
                + "\"Gender\":\"Male\","
                + "\"Concurrency@odata.type\":\"Int64\",\"Concurrency\":1234}";
        UpdateExecution excution = helpExecute(TestODataMetadataProcessor.tripPinMetadata(),
                query, expectedPayload, returnResponse, expectedURL, "POST", 201);

    }

    @Test
    public void testInsertComplexType() throws Exception {
        String query = "INSERT INTO Persons_address(street, city, state, Persons_ssn) "
                + "VALUES('sesame street', 'Newyork', 'NY', 1234)";
        String expectedURL = "Persons(1234)/address";

        String returnResponse = "{\n" +
                "   \"UserName\":\"russellwhyte\",\n" +
                "   \"FirstName\":\"Russell\",\n" +
                "   \"LastName\":\"Whyte\"\n" +
                "}";
        String expectedPayload = "{\"@odata.type\":\"#Edm.Address\","
                + "\"street@odata.type\":\"String\",\"street\":\"sesame street\","
                + "\"city@odata.type\":\"String\",\"city\":\"Newyork\","
                + "\"state@odata.type\":\"String\",\"state\":\"NY\"}";

        // single complex requires PATCH
        UpdateExecution excution = helpExecute(TestODataMetadataProcessor.getEntityWithComplexProperty(),
                query, expectedPayload, returnResponse, expectedURL, "PATCH", 201);

    }

    @Test
    public void testInsertComplexTypeTripPin() throws Exception {
        String query = "INSERT INTO People_AddressInfo(Address, People_UserName) "
                + "VALUES('sesame street', 'russel')";
        String expectedURL = "People('russel')/AddressInfo";

        String returnResponse = "{\n" +
                "   \"Address\":\"russellwhyte\",\n" +
                "   \"FirstName\":\"Russell\",\n" +
                "   \"LastName\":\"Whyte\"\n" +
                "}";
        String expectedPayload = "{\"@odata.type\":\"#Microsoft.OData.SampleService.Models.TripPin.Location\","
                + "\"value\":[{\"@odata.type\":\"#Microsoft.OData.SampleService.Models.TripPin.Location\","
                + "\"Address@odata.type\":\"String\","
                + "\"Address\":\"sesame street\"}]}";

        //collection needs PUT
        UpdateExecution excution = helpExecute(TestODataMetadataProcessor.tripPinMetadata(),
                query, expectedPayload, returnResponse, expectedURL, "PUT", 201);
    }

    @Test
    public void testInsertNavigation() throws Exception {
        String query = "INSERT INTO People_Friends(UserName, FirstName, LastName, People_UserName) "
                + "VALUES('jdoe', 'John', 'Doe', 'russel')";
        String expectedURL = "People('russel')/Friends";

        String returnResponse = "{\n" +
                "   \"UserName\":\"jdoe\",\n" +
                "   \"FirstName\":\"John\",\n" +
                "   \"LastName\":\"Doe\"\n" +
                "}";
        String expectedPayload = "{\"@odata.type\":\"#Microsoft.OData.SampleService.Models.TripPin.Person\","
                + "\"UserName@odata.type\":\"String\",\"UserName\":\"jdoe\","
                + "\"FirstName@odata.type\":\"String\",\"FirstName\":\"John\","
                + "\"LastName@odata.type\":\"String\",\"LastName\":\"Doe\"}";

        UpdateExecution excution = helpExecute(TestODataMetadataProcessor.tripPinMetadata(),
                query, expectedPayload, returnResponse, expectedURL, "POST", 201);
    }
}
