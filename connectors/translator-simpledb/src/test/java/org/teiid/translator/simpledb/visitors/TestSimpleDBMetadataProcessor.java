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
package org.teiid.translator.simpledb.visitors;

import static org.junit.Assert.assertEquals;

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
import org.teiid.resource.adpter.simpledb.SimpleDBConnection;
import org.teiid.resource.adpter.simpledb.SimpleDBConnection.SimpleDBAttribute;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.simpledb.SimpleDBExecutionFactory;

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
