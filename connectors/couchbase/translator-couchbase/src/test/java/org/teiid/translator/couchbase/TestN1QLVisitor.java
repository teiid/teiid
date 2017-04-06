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
package org.teiid.translator.couchbase;

import static org.teiid.translator.couchbase.TestCouchbaseMetadataProcessor.*;
import static org.junit.Assert.*;
import static org.teiid.translator.couchbase.TestN1QLVisitor.N1QL.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map.Entry;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.CoreConstants;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
import org.teiid.language.Command;
import org.teiid.metadata.MetadataException;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.RuntimeMetadata;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.couchbase.CouchbaseMetadataProcessor.Dimension;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

@SuppressWarnings("nls")
public class TestN1QLVisitor {
    
    private static Path N1QL_PATH = Paths.get("src/test/resources", "N1QL.properties");
    private static Properties N1QL = new Properties();
    
    private static final Boolean PRINT_TO_CONSOLE = Boolean.FALSE;
    private static final Boolean REPLACE_EXPECTED = Boolean.FALSE;
    
    private static TransformationMetadata queryMetadataInterface() {
        try {
            ModelMetaData mmd = new ModelMetaData();
            mmd.setName("couchbase");

            CouchbaseMetadataProcessor mp = new CouchbaseMetadataProcessor();  
            MetadataFactory mf = new MetadataFactory("couchbase", 1, SystemMetadata.getInstance().getRuntimeTypeMap(), mmd);
            Table customer = createTable(mf, KEYSPACE, "Customer");
            mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, formCustomer(), mf, customer, customer.getName(), false, new Dimension());
            Table order = createTable(mf, KEYSPACE, "Oder");
            mp.scanRow(KEYSPACE, KEYSPACE_SOURCE, formOder(), mf, order, order.getName(), false, new Dimension());
            Table t2 = createTable(mf, "T2", "T2");
            mp.scanRow("T2", "`T2`", formDataTypeJson(), mf, t2, t2.getName(), false, new Dimension());
            Table t3 = createTable(mf, "T3", "T3");
            mp.scanRow("T3", "`T3`", nestedJson(), mf, t3, t3.getName(), false, new Dimension());
            mp.scanRow("T3", "`T3`", nestedArray(), mf, t3, t3.getName(), false, new Dimension());
            mp.addProcedures(mf, null);

            TransformationMetadata tm = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "x");
            ValidatorReport report = new MetadataValidator().validate(tm.getVdbMetaData(), tm.getMetadataStore());
            if (report.hasItems()) {
                throw new RuntimeException(report.getFailureMessage());
            }
            return tm;
        } catch (MetadataException e) {
            throw new RuntimeException(e);
        }
    }
    
    static TranslationUtility translationUtility = new TranslationUtility(queryMetadataInterface());
    static RuntimeMetadata runtimeMetadata = new RuntimeMetadataImpl(queryMetadataInterface());
    
    private static CouchbaseExecutionFactory TRANSLATOR;
    
    @BeforeClass
    public static void init() throws TranslatorException {
        TRANSLATOR = new CouchbaseExecutionFactory();
        TRANSLATOR.start();
        translationUtility.addUDF(CoreConstants.SYSTEM_MODEL, TRANSLATOR.getPushDownFunctions());
        
        try {
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            DocumentBuilder db = dbf.newDocumentBuilder();
            Document doc = db.parse(Files.newInputStream(N1QL_PATH));
            doc.getDocumentElement().normalize();
            
            NodeList list = doc.getElementsByTagName("entry");
            for (int i = 0; i < list.getLength(); i++) {
                Node node = list.item(i);
                if(node.getNodeType() == Node.ELEMENT_NODE) {
                    Element element = (Element) node;
                    N1QL.put(element.getAttribute("key"), element.getTextContent());
                }  
            } 
        } catch (ParserConfigurationException | SAXException | IOException e1) {
            assert(false);
        }

    }
    
    @AfterClass
    public static void replaceProperties() {
        
        if(REPLACE_EXPECTED.booleanValue()) {
            OutputStream out = null;
            try {
                out = new FileOutputStream(N1QL_PATH.toFile());
                String encoding = "UTF-8";
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document doc = db.newDocument();
                Element properties =  (Element) doc.appendChild(doc.createElement("properties"));
                for (Entry<Object, Object> e : N1QL.entrySet()) {
                    final String key = (String) e.getKey();
                    final String value = (String) e.getValue();
                    if(key.startsWith("N1QL")){
                        Element entry = (Element)properties.appendChild(doc.createElement("entry"));
                        entry.setAttribute("key", key);
                        entry.appendChild(doc.createTextNode(value));
                    }
                }
                
                TransformerFactory tf = TransformerFactory.newInstance();
                Transformer t = tf.newTransformer();
                t.setOutputProperty(OutputKeys.INDENT, "yes");
                t.setOutputProperty(OutputKeys.METHOD, "xml");
                t.setOutputProperty(OutputKeys.ENCODING, encoding);
                DOMSource doms = new DOMSource(doc);
                StreamResult sr = new StreamResult(out);
                t.transform(doms, sr);
            } catch (IOException | ParserConfigurationException | TransformerException e) {
                assert(false);
            } finally {
                out = null;
            }
        }
    }
    
    private void helpTest(String sql, N1QL key) throws TranslatorException {

        Command command = translationUtility.parseCommand(sql);

        N1QLVisitor visitor = TRANSLATOR.getN1QLVisitor();
        visitor.append(command);
        String actual = visitor.toString();
        
        if(PRINT_TO_CONSOLE.booleanValue()) {
            System.out.println(actual);
        }
        
        if(REPLACE_EXPECTED.booleanValue()) {
            N1QL.put(key.toString(), actual);
        }
        
        assertEquals(key.name(), N1QL.getProperty(key.name(), ""), actual);
    }
    
    @Test
    public void testSelect() throws TranslatorException {
        
        String sql = "SELECT * FROM Customer";
        helpTest(sql, N1QL0101);
      
        sql = "SELECT * FROM Customer_SavedAddresses";
        helpTest(sql, N1QL0102);
        
        sql = "SELECT * FROM Oder";
        helpTest(sql, N1QL0103);
        
        sql = "SELECT * FROM Oder_Items";
        helpTest(sql, N1QL0104);
        
        sql = "SELECT DISTINCT Name FROM Customer";
        helpTest(sql, N1QL0105);
        
        sql = "SELECT ALL Name FROM Customer";
        helpTest(sql, N1QL0106);
        
        sql = "SELECT CreditCard_CardNumber, CreditCard_Type, CreditCard_CVN, CreditCard_Expiry FROM Oder";
        helpTest(sql, N1QL0107);
    }

    @Test
    public void testNestedJson() throws TranslatorException  {
        
        String sql = "SELECT * FROM T3";
        helpTest(sql, N1QL0201);
        
        sql = "SELECT nestedJson_nestedJson_nestedJson_Dimension FROM T3";
        helpTest(sql, N1QL0202);
    }
    
    @Test
    public void testNestedArray() throws TranslatorException {
        
        String sql = "SELECT * FROM T3";
        helpTest(sql, N1QL0301);
        
        sql = "SELECT * FROM T3_nestedArray";
        helpTest(sql, N1QL0302);
        
        sql = "SELECT * FROM T3_nestedArray_dim2";
        helpTest(sql, N1QL0303);
        
        sql = "SELECT * FROM T3_nestedArray_dim2_dim3";
        helpTest(sql, N1QL0304);
        
        sql = "SELECT * FROM T3_nestedArray_dim2_dim3_dim4";
        helpTest(sql, N1QL0305);
        
        sql = "SELECT T3_nestedArray_dim2_dim3_dim4_idx, T3_nestedArray_dim2_dim3_dim4 FROM T3_nestedArray_dim2_dim3_dim4";
        helpTest(sql, N1QL0306);
    }
    
    @Test
    public void testPKColumn() throws TranslatorException {
        
        String sql = "SELECT documentID FROM T3";
        helpTest(sql, N1QL0401);
        
        sql = "SELECT documentID FROM T3_nestedArray_dim2_dim3_dim4";
        helpTest(sql, N1QL0402);
    }
    
    @Test
    public void testLimitOffsetClause() throws TranslatorException {
        
        String sql = "SELECT Name FROM Customer LIMIT 2";
        helpTest(sql, N1QL0501);
        
        sql = "SELECT Name FROM Customer LIMIT 2, 2";
        helpTest(sql, N1QL0502);
        
        sql = "SELECT Name FROM Customer OFFSET 2 ROWS";
        helpTest(sql, N1QL0503);
    }
    
    @Test
    public void testOrderByClause() throws TranslatorException {
        
        String sql = "SELECT Name, type FROM Customer ORDER BY Name";
        helpTest(sql, N1QL0601);
        
        sql = "SELECT type FROM Customer ORDER BY Name"; //Unrelated
        helpTest(sql, N1QL0602);
        
        sql = "SELECT Name, type FROM Customer ORDER BY type"; //NullOrdering
        helpTest(sql, N1QL0603);
        
        sql = "SELECT Name, type FROM Customer ORDER BY Name ASC";
        helpTest(sql, N1QL0604);
        
        sql = "SELECT Name, type FROM Customer ORDER BY Name DESC";
        helpTest(sql, N1QL0605);
    }
    
    @Test
    public void testGroupByClause() throws TranslatorException {
        
        String sql = "SELECT Name, COUNT(*) FROM Customer GROUP BY Name";
        helpTest(sql, N1QL0701);
    }
    
    @Test
    public void testWhereClause() throws TranslatorException {
        
        String sql = "SELECT Name, type  FROM Customer WHERE Name = 'John Doe'";
        helpTest(sql, N1QL0801);
        
        sql = "SELECT Name, type  FROM Customer WHERE documentID = 'customer'";
        helpTest(sql, N1QL0802);
        
        sql = "SELECT Name, type  FROM Customer WHERE type = 'Customer'";
        helpTest(sql, N1QL0803);
        
        sql = "SELECT Name FROM Customer";
        helpTest(sql, N1QL0804);
        
        sql = "SELECT Name FROM Customer WHERE documentID = 'customer'";
        helpTest(sql, N1QL0805);
    }
    
    @Test
    public void testStringFunctions() throws TranslatorException {
        
        String sql = "SELECT LCASE(attr_string) FROM T2";
        helpTest(sql, N1QL0901);
        
        sql = "SELECT UCASE(attr_string) FROM T2";
        helpTest(sql, N1QL0902);
        
        sql = "SELECT TRANSLATE(attr_string, 'is', 'are') FROM T2";
        helpTest(sql, N1QL0903);
        
        sql = "SELECT couchbase.CONTAINS(attr_string, 'is') FROM T2";
        helpTest(sql, N1QL0904);
        
        sql = "SELECT couchbase.TITLE(attr_string) FROM T2";
        helpTest(sql, N1QL0905);
        
        sql = "SELECT couchbase.LTRIM(attr_string, 'This') FROM T2";
        helpTest(sql, N1QL0906);
        
        sql = "SELECT couchbase.TRIM(attr_string, 'is') FROM T2";
        helpTest(sql, N1QL0907);
        
        sql = "SELECT couchbase.RTRIM(attr_string, 'value') FROM T2";
        helpTest(sql, N1QL0908);
        
        sql = "SELECT couchbase.POSITION(attr_string, 'is') FROM T2";
        helpTest(sql, N1QL0909);
    }
    
    @Test
    public void testNumbericFunctions() throws TranslatorException {
        
        String sql = "SELECT CEILING(attr_double) FROM T2";
        helpTest(sql, N1QL1001); 
        
        sql = "SELECT LOG(attr_double) FROM T2";
        helpTest(sql, N1QL1002); 
        
        sql = "SELECT LOG10(attr_double) FROM T2";
        helpTest(sql, N1QL1003); 
        
        sql = "SELECT RAND(attr_integer) FROM T2";
        helpTest(sql, N1QL1004); 
    }
    
    @Test
    public void testConversionFunctions() throws TranslatorException {

        String sql = "SELECT convert(attr_long, string) FROM T2";
        helpTest(sql, N1QL1101);
    }
    
    @Test
    public void testDateFunctions() throws TranslatorException {
        
        String sql = "SELECT couchbase.CLOCK_MILLIS() FROM T2";
        helpTest(sql, N1QL1201); 
        
        sql = "SELECT couchbase.CLOCK_STR() FROM T2";
        helpTest(sql, N1QL1202); 
        
        sql = "SELECT couchbase.CLOCK_STR('2006-01-02') FROM T2";
        helpTest(sql, N1QL1203);
                
        sql = "SELECT couchbase.DATE_ADD_MILLIS(1488873653696, 2, 'century') FROM T2";
        helpTest(sql, N1QL1204); 
        
        sql = "SELECT couchbase.DATE_ADD_STR('2017-03-08', 2, 'century') FROM T2";
        helpTest(sql, N1QL1205); 
    }
    
    @Test
    public void testProcedures() throws TranslatorException {
       
        String sql = "call getTextDocuments('%e%', 'test')";
        helpTest(sql, N1QL1301);
        
        sql = "call getDocuments('customer', 'test')";
        helpTest(sql, N1QL1302);
        
        sql = "call getTextDocument('customer', 'test')";
        helpTest(sql, N1QL1303);
        
        sql = "call getDocument('customer', 'test')";
        helpTest(sql, N1QL1304);
        
        sql = "call saveDocument('k001', 'test', '{\"key\": \"value\"}')";
        helpTest(sql, N1QL1305);
        
        sql = "call deleteDocument('k001', 'test')";
        helpTest(sql, N1QL1306);
        
        sql = "call getTextMetadataDocument('test')";
        helpTest(sql, N1QL1307);
        
        sql = "call getMetadataDocument('test')";
        helpTest(sql, N1QL1308);
    }
    
    public static enum N1QL {
        N1QL0101,
        N1QL0102,
        N1QL0103,
        N1QL0104,
        N1QL0105,
        N1QL0106,
        N1QL0107,
        N1QL0201,
        N1QL0202,
        N1QL0301,
        N1QL0302,
        N1QL0303,
        N1QL0304,
        N1QL0305,
        N1QL0306,
        N1QL0401,
        N1QL0402,
        N1QL0403,
        N1QL0501,
        N1QL0502,
        N1QL0503,
        N1QL0601,
        N1QL0602,
        N1QL0603,
        N1QL0604,
        N1QL0605,
        N1QL0701,
        N1QL0702,
        N1QL0801,
        N1QL0802,
        N1QL0803,
        N1QL0804,
        N1QL0805,
        N1QL0806,
        N1QL0901,
        N1QL0902,
        N1QL0903,
        N1QL0904,
        N1QL0905,
        N1QL0906,
        N1QL0907,
        N1QL0908,
        N1QL0909,
        N1QL1001,
        N1QL1002,
        N1QL1003,
        N1QL1004,
        N1QL1005,
        N1QL1101,
        N1QL1102,
        N1QL1201,
        N1QL1202,
        N1QL1203,
        N1QL1204,
        N1QL1205,
        N1QL1301,
        N1QL1302,
        N1QL1303,
        N1QL1304,
        N1QL1305,
        N1QL1306,
        N1QL1307,
        N1QL1308,
        N1QL1401,
        N1QL1501
    }
    
}
