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
package org.teiid.translator.jpa;

import static org.junit.Assert.*;

import java.util.Properties;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.MetadataProcessor;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestJPAMetadataImport {

    private static JPA2ExecutionFactory TRANSLATOR;

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new JPA2ExecutionFactory();
        TRANSLATOR.start();
    }

    @Test public void testImport() throws Exception {
        EntityManagerFactory factory = Persistence.createEntityManagerFactory("org.teiid.translator.jpa.test");

        Properties props = new Properties();
        MetadataFactory mf = new MetadataFactory("vdb", 1, "market", SystemMetadata.getInstance().getRuntimeTypeMap(), props, null);

        MetadataProcessor<EntityManager> processor = TRANSLATOR.getMetadataProcessor();
        processor.process(mf, factory.createEntityManager());

        //no ordering is implied for the entities and can change, so we pull the ddl individually

        String ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, ".*Marketdata");
        assertEquals("CREATE FOREIGN TABLE Marketdata (\n" +
                "\tid string OPTIONS (NAMEINSOURCE 'id'),\n" +
                "\texchange_name string OPTIONS (NAMEINSOURCE 'name', \"teiid_jpa:assosiated_with_table\" 'market.Exchange', \"teiid_jpa:relation_key\" 'name', \"teiid_jpa:relation_property\" 'exchange'),\n" +
                "\tprice bigdecimal OPTIONS (NAMEINSOURCE 'price'),\n" +
                "\tstock_id string OPTIONS (NAMEINSOURCE 'id', \"teiid_jpa:assosiated_with_table\" 'market.Stock', \"teiid_jpa:relation_key\" 'id', \"teiid_jpa:relation_property\" 'stock'),\n" +
                "\tCONSTRAINT PK_Marketdata PRIMARY KEY(id),\n" +
                "\tCONSTRAINT FK_exchange FOREIGN KEY(exchange_name) REFERENCES Exchange  OPTIONS (NAMEINSOURCE 'exchange'),\n" +
                "\tCONSTRAINT FK_stock FOREIGN KEY(stock_id) REFERENCES Stock  OPTIONS (NAMEINSOURCE 'stock')\n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_jpa:entity_class\" 'org.teiid.translator.jpa.model.Marketdata');", ddl);

        ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, "Stock");
        assertEquals("CREATE FOREIGN TABLE Stock (\n" +
                "\tid string OPTIONS (NAMEINSOURCE 'id'),\n" +
                "\tcompanyName string OPTIONS (NAMEINSOURCE 'companyName'),\n" +
                "\tsymbol string OPTIONS (NAMEINSOURCE 'symbol'),\n" +
                "\tCONSTRAINT PK_Stock PRIMARY KEY(id)\n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_jpa:entity_class\" 'org.teiid.translator.jpa.model.Stock');", ddl);

        ddl = DDLStringVisitor.getDDLString(mf.getSchema(), null, "Exchange");
        assertEquals("CREATE FOREIGN TABLE Exchange (\n" +
                "\tname string OPTIONS (NAMEINSOURCE 'name'),\n" +
                "\tCONSTRAINT PK_Exchange PRIMARY KEY(name)\n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_jpa:entity_class\" 'org.teiid.translator.jpa.model.Exchange');", ddl);
    }
}
