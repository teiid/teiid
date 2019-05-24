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
package org.teiid.translator.couchbase;

import static org.teiid.translator.couchbase.TestCouchbaseMetadataProcessor.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

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
import org.teiid.adminapi.impl.ModelMetaData;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.core.CoreConstants;
import org.teiid.dqp.internal.datamgr.RuntimeMetadataImpl;
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
public class TestVisitor {

    private static Path N1QL_PATH = Paths.get("src/test/resources", "N1QL.properties");

    static LinkedHashMap<String, String> N1QL = new LinkedHashMap<String, String>();

    static final Boolean PRINT_TO_CONSOLE = Boolean.FALSE;
    static final Boolean REPLACE_EXPECTED = Boolean.FALSE;

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

    static CouchbaseExecutionFactory TRANSLATOR;

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
        } catch (ParserConfigurationException | SAXException | IOException e) {
            e.printStackTrace();
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
                for (Entry<String, String> e : N1QL.entrySet()) {
                    final String key = e.getKey();
                    final String value = e.getValue();
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

}
