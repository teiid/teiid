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

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.odata4j.core.OError;
import org.odata4j.edm.EdmDataServices;
import org.odata4j.format.FormatParser;
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
import org.teiid.translator.TranslatorException;
import org.teiid.translator.ws.WSConnection;

@SuppressWarnings({"nls", "unused"})
public class TestODataQueryExecution {

    private ResultSetExecution helpExecute(String query,
            final String resultXML, String expectedURL) throws Exception {
        TransformationMetadata metadata = TestDataEntitySchemaBuilder.getNorthwindMetadataFromODataXML();
        return helpExecute(query, resultXML, expectedURL, 200, metadata);
    }

    private ResultSetExecution helpExecute(String query,
            final String resultXML, String expectedURL, int responseCode)
            throws Exception {
        TransformationMetadata metadata = TestDataEntitySchemaBuilder.getNorthwindMetadataFromODataXML();
        return helpExecute(query, resultXML, expectedURL, responseCode, metadata);
    }

    private ResultSetExecution helpExecute(String query,
            final String resultXML, String expectedURL, int responseCode,
            TransformationMetadata metadata) throws Exception {
        ODataExecutionFactory translator = new ODataExecutionFactory();
        translator.start();
        TranslationUtility utility = new TranslationUtility(metadata);
        Command cmd = utility.parseCommand(query);
        ExecutionContext context = Mockito.mock(ExecutionContext.class);
        WSConnection connection = Mockito.mock(WSConnection.class);

        Map<String, Object> headers = new HashMap<String, Object>();
        headers.put(MessageContext.HTTP_REQUEST_HEADERS, new HashMap<String, List<String>>());
        headers.put(WSConnection.STATUS_CODE, new Integer(responseCode));

        Dispatch<DataSource> dispatch = Mockito.mock(Dispatch.class);
        Mockito.stub(dispatch.getRequestContext()).toReturn(headers);
        Mockito.stub(dispatch.getResponseContext()).toReturn(headers);

        Mockito.stub(connection.createDispatch(Mockito.eq(HTTPBinding.HTTP_BINDING), Mockito.anyString(), Mockito.eq(DataSource.class), Mockito.eq(Mode.MESSAGE))).toReturn(dispatch);

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
        Mockito.stub(dispatch.invoke(Mockito.any(DataSource.class))).toReturn(ds);

        ResultSetExecution execution = translator.createResultSetExecution((QueryExpression)cmd, context, utility.createRuntimeMetadata(), connection);
        execution.execute();

        ArgumentCaptor<String> endpoint = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> binding = ArgumentCaptor.forClass(String.class);

        Mockito.verify(connection).createDispatch(binding.capture(), endpoint.capture(), Mockito.eq(DataSource.class), Mockito.eq(Mode.MESSAGE));
        assertEquals(expectedURL, URLDecoder.decode(endpoint.getValue(), "utf-8"));
        return execution;
    }

    @Test
    public void testSimpleSelectNoAssosiations() throws Exception {
        String query = "SELECT CategoryID, CategoryName, Description FROM Categories";
        String expectedURL = "Categories?$select=CategoryID,CategoryName,Description";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("categories.xml"));
        ResultSetExecution excution = helpExecute(query, ObjectConverterUtil.convertToString(reader), expectedURL);

        assertArrayEquals(new Object[] {1, "Beverages", "Soft drinks, coffees, teas, beers, and ales"}, excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {2, "Condiments", "Sweet and savory sauces, relishes, spreads, and seasonings"}, excution.next().toArray(new Object[3]));
        assertArrayEquals(new Object[] {3, "Confections", "Desserts, candies, and sweet breads"}, excution.next().toArray(new Object[3]));
        reader.close();
    }

    @Test
    public void testSimpleSelectStar() throws Exception {
        String query = "SELECT * FROM Categories";
        String expectedURL = "Categories?$select=CategoryID,CategoryName,Description,Picture";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("categories.xml"));
        ResultSetExecution excution = helpExecute(query, ObjectConverterUtil.convertToString(reader), expectedURL);
        reader.close();
    }

    @Test
    public void testSimpleSelectEmbedded() throws Exception {
        String query = "SELECT * FROM Customers";
        String expectedURL = "Customers?$select=CustomerID,CompanyName,ContactName,ContactTitle,Mailing,Shipping";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("customer.xml"));
        ResultSetExecution excution = helpExecute(query, ObjectConverterUtil.convertToString(reader), expectedURL);
        reader.close();
        assertEquals(18, excution.next().size());
    }

    @Test
    public void testSimplePKWhere() throws Exception {
        String query = "SELECT * FROM Categories Where CategoryId = 3";
        String expectedURL = "Categories(3)?$select=CategoryID,CategoryName,Description,Picture";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("categories.xml"));
        ResultSetExecution excution = helpExecute(query, ObjectConverterUtil.convertToString(reader), expectedURL);
        reader.close();
    }

    @Test
    public void testSimpleWhere() throws Exception {
        String query = "SELECT * FROM Categories Where CategoryName = 'Beverages'";
        String expectedURL = "Categories?$filter=CategoryName eq 'Beverages'&$select=CategoryID,CategoryName,Description,Picture";

        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("categories.xml"));
        ResultSetExecution excution = helpExecute(query, ObjectConverterUtil.convertToString(reader), expectedURL);
        reader.close();
    }

    @Test
    public void testArrayType() throws Exception {
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

        Procedure p = mf.getSchema().getProcedure("ARRAYITERATE");
        assertEquals("varbinary[]", p.getParameters().get(0).getRuntimeType());
        assertEquals("varbinary",  p.getResultSet().getColumns().get(0).getRuntimeType());


        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        TransformationMetadata metadata = RealMetadataFactory.fromDDL(ddl, "northwind", "nw");

        String query = "SELECT * FROM G2";
        String expectedURL = "G2?$select=e1,e3";

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
        ResultSetExecution excution = helpExecute(query, result, expectedURL, 200, metadata);
        assertArrayEquals(new Object[] {32, new Integer[] {1,2,3}},
                excution.next().toArray(new Object[2]));

    }

    @Test(expected=TranslatorException.class)
    public void testError() throws Exception {
        String query = "SELECT * FROM Categories Where CategoryName = 'Beverages'";
        String expectedURL = "Categories?$filter=CategoryName eq 'Beverages'&$select=Picture,Description,CategoryName,CategoryID";
        String error = "<error xmlns=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\">\n" +
                "<code>005056A509B11EE1BB8AF4A65EC3CA20</code>\n" +
                "<message xml:lang=\"en\">\n" +
                "Invalid parametertype used at function '' (Position: 16)\n" +
                "</message>\n" +
                "</error>";
        ResultSetExecution excution = helpExecute(query, error, expectedURL, 400);
        excution.next();
    }

    @Test
    public void testNoResults() throws Exception {
        String query = "SELECT * FROM Categories Where CategoryName = 'Beverages'";
        String expectedURL = "Categories?$filter=CategoryName eq 'Beverages'&$select=CategoryID,CategoryName,Description,Picture";
        FileReader reader = new FileReader(UnitTestUtil.getTestDataFile("categories.xml"));
        ResultSetExecution excution = helpExecute(query, ObjectConverterUtil.convertToString(reader), expectedURL, 404);
        excution.execute();
        assertNull(excution.next());
        reader.close();

        reader = new FileReader(UnitTestUtil.getTestDataFile("categories.xml"));
        excution = helpExecute(query, ObjectConverterUtil.convertToString(reader), expectedURL, 204);
        excution.execute();
        assertNull(excution.next());
        reader.close();
    }


    @Test
    public void testErrorParsing() {
        String innerError = "<innererror>\n" +
                "      <transactionid>529E9BFBEDA868F2E1000000AC140C37</transactionid>\n" +
                "      <errordetails>\n" +
                "         <errordetail>\n" +
                "             <code>/IWBEP/CX_MGW_TECH_EXCEPTION</code>\n" +
                "             <message>Operation 'read feed' not supported for Entity Type 'Notification'.</message>\n" +
                "              <propertyref></propertyref>\n" +
                "              <severity>error</severity>\n" +
                "        </errordetail>\n" +
                "     </errordetails>\n" +
                "   </innererror>";

        String error = "<error xmlns=\"http://schemas.microsoft.com/ado/2007/08/dataservices/metadata\">\n" +
                "   <code>SY/530</code>\n" +
                "   <message xml:lang=\"en\"> Operation 'read feed' not supported for Entity Type 'Notification'.</message>\n" +
                innerError +
                "</error>";

        FormatParser<OError> parser =  new AtomErrorFormatParser();
        OError oerror = parser.parse(new StringReader(error)); //$NON-NLS-1$
        assertEquals(innerError, oerror.getInnerError());
    }
}
