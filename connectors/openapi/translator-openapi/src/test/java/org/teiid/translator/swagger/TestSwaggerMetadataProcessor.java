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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.junit.Test;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.metadata.Column;
import org.teiid.metadata.ColumnSet;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Procedure;
import org.teiid.metadata.ProcedureParameter;
import org.teiid.metadata.RestMetadataExtension;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.parser.QueryParser;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.ws.WSConnection;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.swagger.models.Swagger;
import io.swagger.parser.SwaggerParser;

@SuppressWarnings("nls")
public class TestSwaggerMetadataProcessor {

    public static TransformationMetadata getTransformationMetadata(
            MetadataFactory mf, SwaggerExecutionFactory ef) throws Exception {
        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(),
                "swagger", new FunctionTree("foo", new UDFSource(ef.getPushDownFunctions())));
        ValidatorReport report = new MetadataValidator().validate(metadata.getVdbMetaData(),
                metadata.getMetadataStore());
        if (report.hasItems()) {
            throw new RuntimeException(report.getFailureMessage());
        }
        return metadata;
    }

    static MetadataFactory swaggerMetadata(SwaggerExecutionFactory ef) throws TranslatorException {
        return getMetadata(ef, UnitTestUtil.getTestDataPath()+"/swagger.json");
    }

    static MetadataFactory petstoreMetadata(SwaggerExecutionFactory ef) throws TranslatorException {
        return getMetadata(ef, UnitTestUtil.getTestDataPath()+"/petstore.json");
    }

    private static MetadataFactory getMetadata(SwaggerExecutionFactory ef,
            final String file) throws TranslatorException {
        SwaggerMetadataProcessor processor = new SwaggerMetadataProcessor(ef) {
            protected Swagger getSchema(WSConnection conn) throws TranslatorException {
                File f = new File(file);
                ObjectMapper objectMapper = new ObjectMapper();
                try (FileInputStream fis = new FileInputStream(f)) {
                    JsonNode rootNode = objectMapper.readTree(fis);
                    return new SwaggerParser().read(rootNode, true);
                } catch (IOException e) {
                    throw new TranslatorException(e);
                }
            }
        };
        processor.setPreferredProduces("application/json");
        processor.setPreferredConsumes("application/json");
        processor.setPreferredScheme("http");
        Properties props = new Properties();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "swagger",
                SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        processor.process(mf, null);
        //String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        //System.out.println(ddl);

        return mf;
    }

    @Test
    public void testSchema() throws Exception {
        SwaggerExecutionFactory translator = new SwaggerExecutionFactory();
        translator.start();

        MetadataFactory mf = swaggerMetadata(translator);
        //TransformationMetadata metadata = getTransformationMetadata(mf, translator);
        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        //System.out.println(ddl);

        MetadataFactory mf2 = new MetadataFactory("vdb", 1, "x", SystemMetadata.getInstance().getRuntimeTypeMap(),
                new Properties(), null);
        QueryParser.getQueryParser().parseDDL(mf2, ddl);

        Set<String> procSet = new HashSet<String>();
        for(Procedure p : mf.getSchema().getProcedures().values()) {
            procSet.add(p.getName());
        }

        assertEquals(29, procSet.size());

        assertTrue(procSet.contains("addCustomer"));
        assertTrue(procSet.contains("addOneCustomer"));
        assertTrue(procSet.contains("addCustomerList"));

        assertTrue(procSet.contains("getCustomers"));
        assertTrue(procSet.contains("getCustomerList"));
        assertTrue(procSet.contains("getCustomerByCity"));
        assertTrue(procSet.contains("getCustomerByCountry"));
        assertTrue(procSet.contains("getCustomerByName"));
        assertTrue(procSet.contains("getCustomerByNumber"));
        assertTrue(procSet.contains("getByNumCityCountry"));
        assertTrue(procSet.contains("size"));

        assertTrue(procSet.contains("removeCustomer"));
        assertTrue(procSet.contains("removeCustomerByCity"));
        assertTrue(procSet.contains("removeCustomerByCountry"));
        assertTrue(procSet.contains("removeCustomerByName"));
        assertTrue(procSet.contains("removeCustomerByNumber"));
        assertTrue(procSet.contains("removeCustomerByNumCityCountry"));

        assertTrue(procSet.contains("updateCustomer"));
        assertTrue(procSet.contains("updateCustomerByCity"));
        assertTrue(procSet.contains("updateCustomerByCountry"));
        assertTrue(procSet.contains("updateCustomerByName"));
        assertTrue(procSet.contains("updateCustomerByNumber"));
        assertTrue(procSet.contains("updateCustomerByNumCityCountry"));

        // test preferences
        Procedure p = mf.getSchema().getProcedures().get("updateCustomer");
        assertEquals("application/json", p.getProperty(RestMetadataExtension.PRODUCES, false));
    }



    @Test
    public void testSwaggerINParameterTypes() throws TranslatorException {
        SwaggerExecutionFactory translator = new SwaggerExecutionFactory();
        translator.start();
        MetadataFactory mf = swaggerMetadata(translator);

        for(Procedure p : mf.getSchema().getProcedures().values()) {
            // multiple in parameter
            if(p.getName().equals("getByNumCityCountry")) {
                List<ProcedureParameter> params = p.getParameters();
                for(ProcedureParameter param : params) {
                    assertEquals(RestMetadataExtension.ParameterType.QUERY.name(),
                            param.getProperty(RestMetadataExtension.PARAMETER_TYPE, false).toUpperCase());
                }
                assertEquals(3, params.size());
                assertTrue(params.get(0).getName().equalsIgnoreCase("customernumber"));
                assertTrue(params.get(1).getName().equalsIgnoreCase("city"));
                assertTrue(params.get(2).getName().equalsIgnoreCase("country"));
            }
            // QueryParameter and  PathParameter
            else if (p.getName().equals("getCustomerByCity")
                    || p.getName().equals("getCustomerByCountry")
                    && p.getName().equals("getByNumCityCountry")) {
                for(ProcedureParameter param : p.getParameters()) {
                    assertEquals(RestMetadataExtension.ParameterType.QUERY.name(),
                            param.getProperty(RestMetadataExtension.PARAMETER_TYPE, false).toUpperCase());
                }
            } else if(p.getName().equals("getCustomerByNumber") || p.getName().equals("getCustomerByName")) {
                for(ProcedureParameter param : p.getParameters()) {
                    assertEquals(RestMetadataExtension.ParameterType.PATH.name(),
                            param.getProperty(RestMetadataExtension.PARAMETER_TYPE, false).toUpperCase());
                }
            }
            // Post parameter
            else if (p.getName().equals("addCustomer")
                    || p.getName().equals("addOneCustomer")
                    || p.getName().equals("addCustomerList")) {
                ProcedureParameter param = p.getParameters().get(0);
                assertEquals(RestMetadataExtension.ParameterType.BODY.name(),
                        param.getProperty(RestMetadataExtension.PARAMETER_TYPE, false).toUpperCase());
            }

            if (p.getName().equals("addCustomer")) {
                List<ProcedureParameter> params = p.getParameters();
                assertEquals(13, params.size());
                assertEquals("customernumber", params.get(0).getName());
                assertEquals("creditlimit", params.get(12).getName());
            }
        }
    }

    @Test
    public void testAnnotation() throws TranslatorException{
        SwaggerExecutionFactory translator = new SwaggerExecutionFactory();
        translator.start();
        MetadataFactory mf = swaggerMetadata(translator);

        for(Procedure p : mf.getSchema().getProcedures().values()){
            if(p.getName().equals("addCustomer")){
                assertEquals("Add a Customer", p.getAnnotation());
            } else if (p.getName().equals("getByNumCityCountry")){
                assertEquals("get customer by Number, City, Country as return xml/json", p.getAnnotation());
            }
        }
    }

    @Test
    public void testURI() throws TranslatorException{
        SwaggerExecutionFactory translator = new SwaggerExecutionFactory();
        translator.start();
        MetadataFactory mf = swaggerMetadata(translator);

        Procedure p = mf.getSchema().getProcedure("addCustomer");
        assertNotNull(p);
        assertEquals("POST", p.getProperty(RestMetadataExtension.METHOD, false).toUpperCase());
        assertEquals("http://localhost:8080/customer", p.getProperty(RestMetadataExtension.URI, false));
        assertNull(p.getResultSet());

        p = mf.getSchema().getProcedure("getCustomers");
        assertNotNull(p);
        assertEquals("GET", p.getProperty(RestMetadataExtension.METHOD, false).toUpperCase());
        assertEquals("http://localhost:8080/customer/customerList", p.getProperty(RestMetadataExtension.URI, false));
        assertEquals("application/xml", p.getProperty(RestMetadataExtension.PRODUCES, false));
        assertNull(p.getResultSet());
        assertEquals("return", p.getParameters().get(0).getName());
        assertEquals("string", p.getParameters().get(0).getRuntimeType());

        p = mf.getSchema().getProcedure("removeCustomer");
        assertNotNull(p);
        assertEquals("DELETE", p.getProperty(RestMetadataExtension.METHOD, false).toUpperCase());
        assertEquals("http://localhost:8080/customer/delete/{id}", p.getProperty(RestMetadataExtension.URI, false));
        assertEquals("application/json", p.getProperty(RestMetadataExtension.PRODUCES, false));
        assertNull(p.getResultSet());
        assertEquals("id", p.getParameters().get(0).getName());
        assertEquals(1, p.getParameters().size());
    }

    @Test
    public void testResultSets() throws TranslatorException{
        SwaggerExecutionFactory translator = new SwaggerExecutionFactory();
        translator.start();
        MetadataFactory mf = swaggerMetadata(translator);

        Procedure p = mf.getSchema().getProcedure("getCustomerList");
        assertNotNull(p);
        assertEquals("GET", p.getProperty(RestMetadataExtension.METHOD, false).toUpperCase());
        assertEquals("http://localhost:8080/customer/getAll", p.getProperty(RestMetadataExtension.URI, false));
        assertNotNull(p.getResultSet());
        ColumnSet<Procedure> results = p.getResultSet();
        assertEquals(13, results.getColumns().size());
        Column c = results.getColumnByName("customernumber");
        assertNotNull(c);
        assertEquals("string", c.getRuntimeType());

        c = results.getColumnByName("postalcode");
        assertNotNull(c);
        assertEquals("string", c.getRuntimeType());
    }


    @Test
    public void testTypes() throws TranslatorException{
        SwaggerExecutionFactory translator = new SwaggerExecutionFactory();
        translator.start();
        MetadataFactory mf = swaggerMetadata(translator);

        Procedure p = mf.getSchema().getProcedure("testReturnTypes");
        assertNotNull(p);
        assertEquals("GET", p.getProperty(RestMetadataExtension.METHOD, false).toUpperCase());
        assertEquals("http://localhost:8080/test/testReturnTypes", p.getProperty(RestMetadataExtension.URI, false));
        assertNotNull(p.getResultSet());
        ColumnSet<Procedure> results = p.getResultSet();

        assertEquals("byte", results.getColumnByName("a").getRuntimeType());
        assertEquals("integer", results.getColumnByName("b").getRuntimeType());
        assertEquals("integer", results.getColumnByName("c").getRuntimeType());
        assertEquals("long", results.getColumnByName("d").getRuntimeType());
        assertEquals("float", results.getColumnByName("e").getRuntimeType());
        assertEquals("double", results.getColumnByName("f").getRuntimeType());
        assertEquals("boolean", results.getColumnByName("g").getRuntimeType());
        assertEquals("string", results.getColumnByName("h").getRuntimeType());
        assertEquals("byte[]", results.getColumnByName("i").getRuntimeType());
        assertEquals("timestamp", results.getColumnByName("l").getRuntimeType());


        p = mf.getSchema().getProcedure("testTimeTypes");
        assertNotNull(p);
        assertEquals("GET", p.getProperty(RestMetadataExtension.METHOD, false).toUpperCase());
        assertEquals("http://localhost:8080/test/testTimeTypes", p.getProperty(RestMetadataExtension.URI, false));
        assertNotNull(p.getResultSet());
        results = p.getResultSet();

        assertEquals("timestamp", results.getColumnByName("date").getRuntimeType());
        assertEquals("timestamp", results.getColumnByName("sqlDate").getRuntimeType());

        // test types in parameters
        p = mf.getSchema().getProcedure("testTypes");
        assertNotNull(p);
        assertEquals("GET", p.getProperty(RestMetadataExtension.METHOD, false).toUpperCase());
        assertEquals("http://localhost:8080/test/testTypes", p.getProperty(RestMetadataExtension.URI, false));
        assertNotNull(p.getResultSet());
        List<ProcedureParameter> pp = p.getParameters();

        assertEquals("integer", pp.get(0).getRuntimeType());
        assertEquals("long", pp.get(1).getRuntimeType());
        assertEquals("float", pp.get(2).getRuntimeType());
        assertEquals("double", pp.get(3).getRuntimeType());
        assertEquals("string", pp.get(4).getRuntimeType());
        assertEquals("byte", pp.get(5).getRuntimeType());
        assertEquals("string[]", pp.get(6).getRuntimeType());
        assertEquals("boolean", pp.get(7).getRuntimeType());
        assertEquals("timestamp", pp.get(8).getRuntimeType());
    }

    @Test
    public void testOnetoOneEmbeddedReturn() throws TranslatorException{
        SwaggerExecutionFactory translator = new SwaggerExecutionFactory();
        translator.start();
        MetadataFactory mf = swaggerMetadata(translator);

        Procedure p = mf.getSchema().getProcedure("size");
        assertNotNull(p);
        assertEquals("GET", p.getProperty(RestMetadataExtension.METHOD, false).toUpperCase());
        assertEquals("http://localhost:8080/customer/status", p.getProperty(RestMetadataExtension.URI, false));
        assertNotNull(p.getResultSet());
        ColumnSet<Procedure> results = p.getResultSet();

        assertNotNull(results.getColumnByName("size"));
        assertNotNull(results.getColumnByName("heap_maxMemory"));
        assertNotNull(results.getColumnByName("heap_freeMemory"));
    }


    @Test
    public void testDateTime() throws TranslatorException{
        SwaggerExecutionFactory translator = new SwaggerExecutionFactory();
        translator.start();
        MetadataFactory mf = swaggerMetadata(translator);

        Procedure p = mf.getSchema().getProcedure("testTimeTypes");
        assertNotNull(p);
        assertEquals("GET", p.getProperty(RestMetadataExtension.METHOD, false).toUpperCase());
        assertEquals("http://localhost:8080/test/testTimeTypes", p.getProperty(RestMetadataExtension.URI, false));
        assertNotNull(p.getResultSet());

        List<Column> columns = p.getResultSet().getColumns();
        for (int i = 0; i < columns.size(); i++){
            Column column = columns.get(i);
            Class<?> type = column.getJavaType();
            assertEquals(java.sql.Timestamp.class, type);
        }
    }

    @Test
    public void testRefToProcedureParam() throws TranslatorException{
        SwaggerExecutionFactory translator = new SwaggerExecutionFactory();
        translator.start();
        MetadataFactory mf = petstoreMetadata(translator);

        Procedure p = mf.getSchema().getProcedure("addPet");
        assertNotNull(p);
        assertEquals("POST", p.getProperty(RestMetadataExtension.METHOD, false).toUpperCase());
        assertEquals("http://petstore.swagger.io/v2/pet", p.getProperty(RestMetadataExtension.URI, false));

        ProcedureParameter pa = p.getParameterByName("id");
        assertNull(pa.getNameInSource());
        assertEquals("body", pa.getProperty("teiid_rest:PARAMETER_TYPE", false));

        pa = p.getParameterByName("category_id");
        assertEquals("category/id", pa.getNameInSource());
        assertEquals("body", pa.getProperty("teiid_rest:PARAMETER_TYPE", false));

        pa = p.getParameterByName("tags_Tag_id");
        assertEquals("tags[]/Tag/id", pa.getNameInSource());
        assertEquals("body", pa.getProperty("teiid_rest:PARAMETER_TYPE", false));
        //TODO: the logic could support this as a long array as well
        assertEquals("long", pa.getRuntimeType());
    }

    @Test
    public void testReftoResponse() throws TranslatorException{
        SwaggerExecutionFactory translator = new SwaggerExecutionFactory();
        translator.start();
        MetadataFactory mf = petstoreMetadata(translator);

        Procedure p = mf.getSchema().getProcedure("findPetsByTags");
        assertNotNull(p);
        assertEquals("GET", p.getProperty(RestMetadataExtension.METHOD, false).toUpperCase());
        assertEquals("http://petstore.swagger.io/v2/pet/findByTags", p.getProperty(RestMetadataExtension.URI, false));

        assertNotNull(p.getResultSet());

        List<Column> columns = p.getResultSet().getColumns();
        assertEquals(8, columns.size());

        Column pa = p.getResultSet().getColumnByName("id");
        assertNull(pa.getNameInSource());

        pa = p.getResultSet().getColumnByName("category_id");
        assertEquals("category/id", pa.getNameInSource());

        pa = p.getResultSet().getColumnByName("tags_Tag_id");
        assertEquals("tags[]/Tag/id", pa.getNameInSource());
    }

    @Test
    public void testBodyandPathProcedure() throws Exception {
        SwaggerExecutionFactory translator = new SwaggerExecutionFactory();
        translator.start();
        MetadataFactory mf = swaggerMetadata(translator);

        Procedure p = mf.getSchema().getProcedure("executeOperation");
        assertNotNull(p);
        assertEquals("PUT", p.getProperty(RestMetadataExtension.METHOD, false).toUpperCase());
        assertEquals("http://localhost:8080/operation/{operationId}", p.getProperty(RestMetadataExtension.URI, false));
        assertNotNull(p.getResultSet());

        assertNotNull(p.getParameterByName("operationId"));
        assertNotNull(p.getParameterByName("id"));
        assertNotNull(p.getParameterByName("name"));
        assertNotNull(p.getParameterByName("resourceId"));
        assertNotNull(p.getParameterByName("definitionId"));
        assertNotNull(p.getParameterByName("readyToSubmit"));
        assertNotNull(p.getParameterByName("params_arguments"));

        List<Column> columns = p.getResultSet().getColumns();
        assertEquals(2, columns.size());
    }

    @Test
    public void testRefParameter() throws Exception {
        SwaggerExecutionFactory translator = new SwaggerExecutionFactory();
        translator.start();
        MetadataFactory mf = getMetadata(translator, UnitTestUtil.getTestDataPath()+"/redis-swagger.json");

        Procedure p = mf.getSchema().getProcedure("Operations_List");
        assertNotNull(p);
        assertEquals("GET", p.getProperty(RestMetadataExtension.METHOD, false).toUpperCase());
        assertEquals("https://management.azure.com/providers/Microsoft.Cache/operations", p.getProperty(RestMetadataExtension.URI, false));
        assertNotNull(p.getResultSet());

        assertNotNull(p.getParameterByName("api-version"));
    }

    @Test
    public void testEndpointAsName() throws Exception {
        SwaggerExecutionFactory translator = new SwaggerExecutionFactory();
        translator.start();
        MetadataFactory mf = getMetadata(translator, UnitTestUtil.getTestDataPath()+"/fahrplan-swagger.json");

        Procedure p = mf.getSchema().getProcedure("arrivalBoard/id");
        assertNotNull(p);
        assertEquals("GET", p.getProperty(RestMetadataExtension.METHOD, false).toUpperCase());
        assertEquals("http://api.deutschebahn.com/freeplan/v1/arrivalBoard/{id}", p.getProperty(RestMetadataExtension.URI, false));
        assertNotNull(p.getResultSet());
    }

    @Test
    public void testObjectArrayTypes() throws Exception {
        SwaggerExecutionFactory translator = new SwaggerExecutionFactory();
        translator.start();
        MetadataFactory mf = getMetadata(translator, UnitTestUtil.getTestDataPath()+"/doubleclick-swagger.json");

       Procedure p = mf.getSchema().getProcedure("doubleclicksearch.reports.request");
       ProcedureParameter param = p.getParameterByName("filters_values");
       assertEquals("string[]", param.getRuntimeType());
    }

    @Test
    public void testRecursiveProperty() throws Exception {
        SwaggerExecutionFactory translator = new SwaggerExecutionFactory();
        translator.start();
        MetadataFactory mf = getMetadata(translator, UnitTestUtil.getTestDataPath()+"/magento-swagger.json");

        assertEquals(ObjectConverterUtil.convertFileToString(UnitTestUtil.getTestDataFile("magento.ddl")), DDLStringVisitor.getDDLString(mf.getSchema(), null, null));
    }
}
