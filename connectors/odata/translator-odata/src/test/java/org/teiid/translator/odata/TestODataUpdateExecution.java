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
package org.teiid.translator.odata;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.activation.DataSource;
import javax.xml.ws.Dispatch;
import javax.xml.ws.Service.Mode;
import javax.xml.ws.handler.MessageContext;
import javax.xml.ws.http.HTTPBinding;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.format.xml.EdmxFormatParser;
import org.odata4j.stax2.util.StaxUtil;
import org.teiid.adminapi.Model.Type;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.PropertiesUtils;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.language.QueryExpression;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.ResultSetExecution;
import org.teiid.translator.UpdateExecution;
import org.teiid.translator.ws.WSConnection;

@SuppressWarnings({"nls", "unused"})
public class TestODataUpdateExecution {


    private String helpExecute(String query,
            final String resultXML, String expectedURL, int[] responseCode, int times)
            throws Exception {
        TransformationMetadata metadata = TestDataEntitySchemaBuilder.getNorthwindMetadataFromODataXML();
        return helpExecute(query, resultXML, expectedURL, responseCode, metadata, times);
    }

    private String helpExecute(String query,
            final String resultXML, String expectedURL, int[] responseCode,
            TransformationMetadata metadata, int times) throws Exception {
        ODataExecutionFactory translator = new ODataExecutionFactory();
        translator.start();
        TranslationUtility utility = new TranslationUtility(metadata);
        Command cmd = utility.parseCommand(query);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);
        WSConnection connection = Mockito.mock(WSConnection.class);

        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put(MessageContext.HTTP_REQUEST_HEADERS, new HashMap<String, List<String>>());
        headers.put(WSConnection.STATUS_CODE, new Integer(responseCode[0]));

        Dispatch<DataSource> dispatch = Mockito.mock(Dispatch.class);
        Mockito.stub(dispatch.getRequestContext()).toReturn(headers);
        Mockito.stub(dispatch.getResponseContext()).toReturn(headers);

        Mockito.stub(connection.createDispatch(Mockito.eq(HTTPBinding.HTTP_BINDING),
                Mockito.anyString(), Mockito.eq(DataSource.class),
                Mockito.eq(Mode.MESSAGE))).toReturn(dispatch);

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
                ByteArrayInputStream in = new ByteArrayInputStream(resultXML.getBytes());
                return in;
            }
            @Override
            public String getContentType() {
                return "application/xml";
            }
        };
        ArgumentCaptor<DataSource> payload = ArgumentCaptor.forClass(DataSource.class);
        Mockito.stub(dispatch.invoke(payload.capture())).toReturn(ds);

        UpdateExecution execution = translator.createUpdateExecution(cmd,
                context, utility.createRuntimeMetadata(), connection);
        execution.execute();

        ArgumentCaptor<String> endpoint = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> binding = ArgumentCaptor.forClass(String.class);

        Mockito.verify(connection, Mockito.times(times)).createDispatch(binding.capture(),
                endpoint.capture(), Mockito.eq(DataSource.class),
                Mockito.eq(Mode.MESSAGE));
        Mockito.verify(dispatch, Mockito.times(times)).invoke(payload.capture());
        assertEquals(expectedURL, URLDecoder.decode(endpoint.getValue(), "utf-8"));
        if (payload.getAllValues() != null) {
            List<DataSource> listDS = payload.getAllValues();
            InputStream in = null;
            if (times > 1) {
                in = listDS.get(1).getInputStream();
            } else {
                in = listDS.get(0).getInputStream();
            }
            return new String(ObjectConverterUtil.convertToByteArray(in));
        }
        return "";
    }

    @Test
    public void testSimpleInsert() throws Exception {
        String query = "INSERT INTO Categories(CategoryID, CategoryName, Description) values(1, 'catname', 'desc')";
        String expectedURL = "Categories";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("categories.xml"));
        String payload = helpExecute(query, ObjectConverterUtil.convertToString(reader), expectedURL, new int[] {201, 201}, 1);
        reader.close();

        String expected =  "<category term=\"NorthwindModel.Category\" "
                + "scheme=\"http://schemas.microsoft.com/ado/2007/08/dataservices/scheme\"></category>"
                + "<content type=\"application/xml\">"
                + "<m:properties><d:CategoryID m:type=\"Edm.Int32\">1</d:CategoryID>"
                + "<d:CategoryName>catname</d:CategoryName><d:Description>desc</d:Description>"
                + "</m:properties></content>"
                + "</entry>";
        assertTrue(expected, payload.endsWith(expected));
    }

    @Test
    public void testSimpleUpdate() throws Exception {
        String query = "Update Categories set CategoryName = 'catname' where CategoryID=1";
        String expectedURL = "Categories(1)";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("categories.xml"));
        String payload = helpExecute(query, ObjectConverterUtil.convertToString(reader), expectedURL, new int[] {200, 204}, 2);
        reader.close();

        String expected = "<category term=\"NorthwindModel.Category\" "
                + "scheme=\"http://schemas.microsoft.com/ado/2007/08/dataservices/scheme\">"
                + "</category>"
                + "<content type=\"application/xml\">"
                + "<m:properties>"
                + "<d:CategoryName>catname</d:CategoryName>"
                + "</m:properties></content>"
                + "</entry>";
        assertTrue(expected, payload.endsWith(expected));
    }

    @Test
    public void testArrayInsert() throws Exception {
        ModelMetaData model = new ModelMetaData();
        model.setName("nw");
        model.setModelType(Type.PHYSICAL);
        MetadataFactory mf = new MetadataFactory("northwind", 1, SystemMetadata.getInstance().getRuntimeTypeMap(), model);

        EdmDataServices edm = new EdmxFormatParser().parseMetadata(
                StaxUtil.newXMLEventReader(new FileReader(UnitTestUtil.getTestDataFile("arraytest.xml"))));
        ODataMetadataProcessor metadataProcessor = new ODataMetadataProcessor();
        PropertiesUtils.setBeanProperties(metadataProcessor, mf.getModelProperties(), "importer"); //$NON-NLS-1$
        metadataProcessor.getMetadata(mf, edm);

        Column c = mf.getSchema().getTable("G2").getColumnByName("e3");
        assertEquals("integer[]", c.getRuntimeType());

        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "northwind", "nw");

        String query = "insert into G2 (e1, e3) values(1, (1,2,3))";
        String expectedURL = "G2";

        String result = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<feed xmlns=\"http://www.w3.org/2005/Atom\" xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\" xml:base=\"http://localhost:8080/odata/loopy/\">\n" +
                "   <title type=\"text\">VM1.x</title>\n" +
                "   <id>http://localhost:8080/odata/loopy/VM1.x</id>\n" +
                "   <updated>2015-10-14T19:36:58Z</updated>\n" +
                "   <link rel=\"self\" title=\"VM1.x\" href=\"VM1.x\" />\n" +
                "   <entry>\n" +
                "      <id>http://localhost:8080/odata/loopy/VM1.x('x')</id>\n" +
                "      <title type=\"text\" />\n" +
                "      <updated>2015-10-14T19:36:58Z</updated>\n" +
                "      <author>\n" +
                "         <name />\n" +
                "      </author>\n" +
                "      <link rel=\"edit\" title=\"x\" href=\"VM1.x('x')\" />\n" +
                "      <category term=\"PM1.G2\" scheme=\"http://schemas.microsoft.com/ado/2007/08/dataservices/scheme\" />\n" +
                "      <content type=\"application/xml\">\n" +
                "         <m:properties>\n" +
                "            <d:e1>32</d:e1>\n" +
                "            <d:e3 m:type=\"Collection(Edm.Int32)\">\n" +
                "               <d:element>1</d:element>\n" +
                "               <d:element>2</d:element>\n" +
                "               <d:element>3</d:element>\n" +
                "            </d:e3>\n" +
                "         </m:properties>\n" +
                "      </content>\n" +
                "   </entry>\n" +
                "</feed>";

        String payload = helpExecute(query, result, expectedURL, new int[] {201, 201}, metadata, 1);

        String expected = "<category term=\"PM1.G2\" "
                + "scheme=\"http://schemas.microsoft.com/ado/2007/08/dataservices/scheme\">"
                + "</category>"
                + "<content type=\"application/xml\">"
                + "<m:properties>"
                + "<d:e1 m:type=\"Edm.Int32\">1</d:e1>"
                + "<d:e3 m:type=\"Collection(Edm.Int32)\">"
                + "<d:element>1</d:element>"
                + "<d:element>2</d:element>"
                + "<d:element>3</d:element>"
                + "</d:e3>"
                + "</m:properties>"
                + "</content>"
                + "</entry>";
        assertTrue(expected, payload.endsWith(expected));
    }

    @Test
    public void testArrayUpdate() throws Exception {
        ModelMetaData model = new ModelMetaData();
        model.setName("nw");
        model.setModelType(Type.PHYSICAL);
        MetadataFactory mf = new MetadataFactory("northwind", 1, SystemMetadata.getInstance().getRuntimeTypeMap(), model);

        EdmDataServices edm = new EdmxFormatParser().parseMetadata(
                StaxUtil.newXMLEventReader(new FileReader(UnitTestUtil.getTestDataFile("arraytest.xml"))));
        ODataMetadataProcessor metadataProcessor = new ODataMetadataProcessor();
        PropertiesUtils.setBeanProperties(metadataProcessor, mf.getModelProperties(), "importer"); //$NON-NLS-1$
        metadataProcessor.getMetadata(mf, edm);

        Column c = mf.getSchema().getTable("G2").getColumnByName("e3");
        assertEquals("integer[]", c.getRuntimeType());

        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "northwind", "nw");

        String query = "Update G2 set e3 = (1,2,3) where e1 = 1";
        String expectedURL = "G2(1)";

        String result = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<feed xmlns=\"http://www.w3.org/2005/Atom\" xmlns:d=\"http://schemas.microsoft.com/ado/2007/08/dataservices\" xmlns:m=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\" xml:base=\"http://localhost:8080/odata/loopy/\">\n" +
                "   <title type=\"text\">VM1.x</title>\n" +
                "   <id>http://localhost:8080/odata/loopy/VM1.x</id>\n" +
                "   <updated>2015-10-14T19:36:58Z</updated>\n" +
                "   <link rel=\"self\" title=\"VM1.x\" href=\"VM1.x\" />\n" +
                "   <entry>\n" +
                "      <id>http://localhost:8080/odata/loopy/VM1.x('x')</id>\n" +
                "      <title type=\"text\" />\n" +
                "      <updated>2015-10-14T19:36:58Z</updated>\n" +
                "      <author>\n" +
                "         <name />\n" +
                "      </author>\n" +
                "      <link rel=\"edit\" title=\"x\" href=\"VM1.x('x')\" />\n" +
                "      <category term=\"PM1.G2\" scheme=\"http://schemas.microsoft.com/ado/2007/08/dataservices/scheme\" />\n" +
                "      <content type=\"application/xml\">\n" +
                "         <m:properties>\n" +
                "            <d:e1>32</d:e1>\n" +
                "            <d:e3 m:type=\"Collection(Edm.Int32)\">\n" +
                "               <d:element>1</d:element>\n" +
                "               <d:element>2</d:element>\n" +
                "               <d:element>3</d:element>\n" +
                "            </d:e3>\n" +
                "         </m:properties>\n" +
                "      </content>\n" +
                "   </entry>\n" +
                "</feed>";

        String payload = helpExecute(query, result, expectedURL, new int[] {200, 204}, metadata, 2);

        String expected = "<category term=\"PM1.G2\" "
                + "scheme=\"http://schemas.microsoft.com/ado/2007/08/dataservices/scheme\">"
                + "</category>"
                + "<content type=\"application/xml\">"
                + "<m:properties><d:e3 m:type=\"Collection(Edm.Int32)\">"
                + "<d:element>1</d:element>"
                + "<d:element>2</d:element>"
                + "<d:element>3</d:element>"
                + "</d:e3>"
                + "</m:properties>"
                + "</content>"
                + "</entry>";
        assertTrue(expected, payload.endsWith(expected));
    }
}
