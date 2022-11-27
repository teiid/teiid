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
package org.teiid.translator.simpledb.visitors;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.function.FunctionTree;
import org.teiid.query.function.UDFSource;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.MetadataValidator;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.query.validator.ValidatorReport;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.simpledb.SimpleDBExecutionFactory;
import org.teiid.translator.simpledb.api.SimpleDBConnection;
import org.teiid.translator.simpledb.api.SimpleDBConnection.SimpleDBAttribute;

@SuppressWarnings ("nls")
public class TestSimpleDBMetadataProcessor {

    static String getDDL(Properties props) throws TranslatorException {
        SimpleDBExecutionFactory translator = new SimpleDBExecutionFactory();
        translator.start();

        MetadataFactory mf = new MetadataFactory("vdb", 1, "people", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);
        SimpleDBConnection connection = Mockito.mock(SimpleDBConnection.class);

        Mockito.stub(connection.getDomains()).toReturn(Arrays.asList("G1", "G2"));

        HashSet<SimpleDBAttribute> cols = new HashSet<SimpleDBConnection.SimpleDBAttribute>();
        cols.add(new SimpleDBAttribute("e1", false));
        cols.add(new SimpleDBAttribute("e2", false));
        Mockito.stub(connection.getAttributeNames("G1")).toReturn(cols);

        HashSet<SimpleDBAttribute> cols2 = new HashSet<SimpleDBConnection.SimpleDBAttribute>();
        cols2.add(new SimpleDBAttribute("e1", false));
        cols2.add(new SimpleDBAttribute("e2", true));
        Mockito.stub(connection.getAttributeNames("G2")).toReturn(cols2);

        translator.getMetadata(mf, connection);

        TransformationMetadata metadata = RealMetadataFactory.createTransformationMetadata(mf.asMetadataStore(), "vdb", new FunctionTree("foo", new UDFSource(translator.getPushDownFunctions())));
        ValidatorReport report = new MetadataValidator().validate(metadata.getVdbMetaData(), metadata.getMetadataStore());
        if (report.hasItems()) {
            throw new RuntimeException(report.getFailureMessage());
        }

        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        return ddl;
    }


    @Test
    public void testSchema() throws Exception {
        Properties props = new Properties();
        String ddl = getDDL(props);

        String expectedDDL = "CREATE FOREIGN TABLE G1 (\n" +
                "\tItemName string NOT NULL OPTIONS (NAMEINSOURCE 'itemName()'),\n" +
                "\te1 string,\n" +
                "\te2 string,\n" +
                "\tCONSTRAINT PK0 PRIMARY KEY(ItemName)\n" +
                ") OPTIONS (UPDATABLE TRUE);\n" +
                "\n" +
                "CREATE FOREIGN TABLE G2 (\n" +
                "\tItemName string NOT NULL OPTIONS (NAMEINSOURCE 'itemName()'),\n" +
                "\te1 string,\n" +
                "\te2 string[],\n" +
                "\tCONSTRAINT PK0 PRIMARY KEY(ItemName)\n" +
                ") OPTIONS (UPDATABLE TRUE);";

        assertEquals(expectedDDL, ddl);
    }
}
