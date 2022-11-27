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
package org.teiid.translator.mongodb;

import static org.junit.Assert.assertEquals;

import java.sql.Date;
import java.util.LinkedHashSet;
import java.util.Properties;

import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.metadata.MetadataFactory;
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.TranslatorException;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBRef;

@SuppressWarnings("nls")
public class TestMongoDBMetadataProcessor {

    @Test
    public void testMetadata() throws TranslatorException {
        MongoDBMetadataProcessor mp = new MongoDBMetadataProcessor();
        mp.setFullEmbeddedNames(true);

        MetadataFactory mf = processExampleMetadata(mp);

        String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        String expected = "CREATE FOREIGN TABLE \"table\" (\n" +
                "    \"_id\" integer,\n" +
                "    col2 double,\n" +
                "    col3 long,\n" +
                "    col5 boolean,\n" +
                "    col6 string,\n" +
                "    col7 string[] OPTIONS (SEARCHABLE 'Unsearchable'),\n" +
                "    col8 varbinary OPTIONS (NATIVE_TYPE 'org.bson.types.Binary'),\n" +
                "    CONSTRAINT PK0 PRIMARY KEY(\"_id\")\n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_rel:fqn\" 'collection=table');\n" +
                "\n" +
                "CREATE FOREIGN TABLE table_child (\n" +
                "    col1 string,\n" +
                "    col2 string,\n" +
                "    \"_id\" integer OPTIONS (UPDATABLE FALSE),\n" +
                "    CONSTRAINT PK0 PRIMARY KEY(\"_id\"),\n" +
                "    FOREIGN KEY(\"_id\") REFERENCES \"table\" \n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_mongo:MERGE\" 'table', \"teiid_rel:fqn\" 'collection=table/embedded=child');\n" +
                "\n" +
                "CREATE FOREIGN TABLE table_embedded (\n" +
                "    col1 integer,\n" +
                "    col2 varbinary,\n" +
                "    \"_id\" integer OPTIONS (UPDATABLE FALSE),\n" +
                "    CONSTRAINT PK0 PRIMARY KEY(\"_id\"),\n" +
                "    FOREIGN KEY(\"_id\") REFERENCES \"table\" \n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_mongo:MERGE\" 'table', \"teiid_rel:fqn\" 'collection=table/embedded=embedded');\n" +
                "\n" +
                "CREATE FOREIGN TABLE embedded (\n" +
                "    col1 string,\n" +
                "    col2 string\n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_rel:fqn\" 'collection=embedded');\n" +
                "\n" +
                "CREATE FOREIGN TABLE emptyFirst (\n" +
                "    \"_id\" string AUTO_INCREMENT OPTIONS (NATIVE_TYPE 'org.bson.types.ObjectId'),\n" +
                "    col2 double,\n" +
                "    col3 long,\n" +
                "    CONSTRAINT PK0 PRIMARY KEY(\"_id\")\n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_rel:fqn\" 'collection=emptyFirst');";
        assertEquals(expected, metadataDDL.replace("\t", "    "));
    }

    @Test
    public void testMetadataMoreSamples() throws TranslatorException {
        MongoDBMetadataProcessor mp = new MongoDBMetadataProcessor();
        mp.setSampleSize(2);

        MetadataFactory mf = processExampleMetadata(mp);

        String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        String expected = "CREATE FOREIGN TABLE \"table\" (\n" +
                "    \"_id\" integer,\n" +
                "    col2 double,\n" +
                "    col3 string OPTIONS (SEARCHABLE 'Unsearchable'),\n" +
                "    col5 boolean,\n" +
                "    col6 string,\n" +
                "    col7 string[] OPTIONS (SEARCHABLE 'Unsearchable'),\n" +
                "    col8 varbinary OPTIONS (NATIVE_TYPE 'org.bson.types.Binary'),\n" +
                "    col9 string,\n" +
                "    CONSTRAINT PK0 PRIMARY KEY(\"_id\")\n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_rel:fqn\" 'collection=table');\n" +
                "\n" +
                "CREATE FOREIGN TABLE child (\n" +
                "    col1 string,\n" +
                "    col2 string,\n" +
                "    \"_id\" integer OPTIONS (UPDATABLE FALSE),\n" +
                "    CONSTRAINT PK0 PRIMARY KEY(\"_id\"),\n" +
                "    FOREIGN KEY(\"_id\") REFERENCES \"table\" \n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_mongo:MERGE\" 'table', \"teiid_rel:fqn\" 'collection=table/embedded=child');\n" +
                "\n" +
                "CREATE FOREIGN TABLE embedded (\n" +
                "    col1 integer,\n" +
                "    col2 varbinary,\n" +
                "    \"_id\" integer OPTIONS (UPDATABLE FALSE),\n" +
                "    CONSTRAINT PK0 PRIMARY KEY(\"_id\"),\n" +
                "    FOREIGN KEY(\"_id\") REFERENCES \"table\" \n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_mongo:MERGE\" 'table', \"teiid_rel:fqn\" 'collection=table/embedded=embedded');\n" +
                "\n" +
                "CREATE FOREIGN TABLE embedded_1 (\n" +
                "    col1 string,\n" +
                "    col2 string\n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_rel:fqn\" 'collection=embedded');\n" +
                "\n" +
                "CREATE FOREIGN TABLE emptyFirst (\n" +
                "    \"_id\" string AUTO_INCREMENT OPTIONS (NATIVE_TYPE 'org.bson.types.ObjectId'),\n" +
                "    col2 double,\n" +
                "    col3 long,\n" +
                "    CONSTRAINT PK0 PRIMARY KEY(\"_id\")\n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_rel:fqn\" 'collection=emptyFirst');";
        assertEquals(expected, metadataDDL.replace("\t", "    "));
    }

    @Test
    public void testTableWithoutColumns() throws TranslatorException {
        MongoDBMetadataProcessor mp = new MongoDBMetadataProcessor();

        MetadataFactory mf = new MetadataFactory("vdb", 1, "mongodb", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        MongoDBConnection conn = Mockito.mock(MongoDBConnection.class);
        DBCollection tableDBCollection = Mockito.mock(DBCollection.class);
        LinkedHashSet<String> tables = new LinkedHashSet<String>();
        tables.add("table");

        DB db = Mockito.mock(DB.class);

        //no id, nor other real column
        BasicDBObject row = new BasicDBObject();
        row.append("col", new BasicDBObject());

        Mockito.stub(db.getCollectionNames()).toReturn(tables);
        Mockito.stub(db.getCollection(Mockito.eq("table"))).toReturn(tableDBCollection);

        DBCursor tableCursor = Mockito.mock(DBCursor.class);
        Mockito.when(tableCursor.hasNext()).thenReturn(true).thenReturn(false);
        Mockito.when(tableCursor.next()).thenReturn(row);
        Mockito.when(tableDBCollection.find()).thenReturn(tableCursor);

        Mockito.stub(conn.getDatabase()).toReturn(db);

        mp.process(mf, conn);

        String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        String expected = "";
        assertEquals(expected, metadataDDL);
    }

    private MetadataFactory processExampleMetadata(MongoDBMetadataProcessor mp)
            throws TranslatorException {
        MetadataFactory mf = new MetadataFactory("vdb", 1, "mongodb", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        mf.setRenameAllDuplicates(true);
        MongoDBConnection conn = Mockito.mock(MongoDBConnection.class);
        DBCollection tableDBCollection = Mockito.mock(DBCollection.class);
        DBCollection embeddedDBCollection = Mockito.mock(DBCollection.class);
        DBCollection emptyDBCollection = Mockito.mock(DBCollection.class);
        DBCollection emptyFirstDBCollection = Mockito.mock(DBCollection.class);
        LinkedHashSet<String> tables = new LinkedHashSet<String>();
        tables.add("table");
        tables.add("embedded");
        tables.add("empty");
        tables.add("emptyFirst");

        DB db = Mockito.mock(DB.class);

        BasicDBList array = new BasicDBList();
        array.add("one");
        array.add("two");

        BasicDBObject row = new BasicDBObject();
        row.append("_id", new Integer(1));
        row.append("col2", new Double(2.0));
        row.append("col3", new Long(3L));
        row.append("col5", Boolean.TRUE);
        row.append("col6", new Date(0L));
        row.append("col6", new DBRef(db.getName(), "ns", "one"));
        row.append("col7", array);
        row.append("col8", new Binary("binary".getBytes()));
        row.append("col9", new BasicDBList()); //empty list

        BasicDBObject child = new BasicDBObject();
        child.append("col1", "one");
        child.append("col2", "two");
        row.append("child", child);

        BasicDBObject emptyFirstRow = new BasicDBObject();
        emptyFirstRow.append("_id", new ObjectId("5835a598944716c40d2f26ae"));
        emptyFirstRow.append("col2", new Double(2.0));
        emptyFirstRow.append("col3", new Long(3L));

        BasicDBObject embedded = new BasicDBObject();
        embedded.append("col1", 1);
        embedded.append("col2", new byte[0]);
        row.append("embedded", embedded);

        Mockito.stub(db.getCollectionNames()).toReturn(tables);
        Mockito.stub(db.getCollection(Mockito.eq("table"))).toReturn(tableDBCollection);
        Mockito.stub(db.getCollection(Mockito.eq("embedded"))).toReturn(embeddedDBCollection);
        Mockito.stub(db.getCollection(Mockito.eq("empty"))).toReturn(emptyDBCollection);
        Mockito.stub(db.getCollection(Mockito.eq("emptyFirst"))).toReturn(emptyFirstDBCollection);

        BasicDBObject nextRow = new BasicDBObject();
        nextRow.append("_id", new Integer(2));
        nextRow.append("col2", new Double(3.0));
        nextRow.append("col3", "A");
        nextRow.append("col5", Boolean.TRUE);
        nextRow.append("col9", "another");

        DBCursor tableCursor = Mockito.mock(DBCursor.class);
        Mockito.when(tableCursor.numSeen()).thenReturn(1).thenReturn(2);
        Mockito.when(tableCursor.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(tableCursor.next()).thenReturn(row).thenReturn(nextRow);
        Mockito.when(tableDBCollection.find()).thenReturn(tableCursor);

        DBCursor embeddedCursor = Mockito.mock(DBCursor.class);
        Mockito.when(embeddedCursor.hasNext()).thenReturn(true).thenReturn(false);
        Mockito.when(embeddedCursor.next()).thenReturn(child);
        Mockito.when(embeddedDBCollection.find()).thenReturn(embeddedCursor);

        DBCursor emptyFirstCursor = Mockito.mock(DBCursor.class);
        Mockito.when(emptyFirstCursor.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(emptyFirstCursor.next()).thenReturn(null).thenReturn(emptyFirstRow);
        Mockito.when(emptyFirstDBCollection.find()).thenReturn(emptyFirstCursor);

        DBCursor emptyCursor = Mockito.mock(DBCursor.class);
        Mockito.when(emptyCursor.hasNext()).thenReturn(true).thenReturn(false);
        Mockito.when(emptyCursor.next()).thenReturn(null);
        Mockito.when(emptyDBCollection.find()).thenReturn(emptyCursor);

        Mockito.stub(conn.getDatabase()).toReturn(db);

        mp.process(mf, conn);
        return mf;
    }

    @Test
    public void testExclusion() throws TranslatorException {
        MongoDBMetadataProcessor mp = new MongoDBMetadataProcessor();
        mp.setExcludeTables("e.*");

        MetadataFactory mf = new MetadataFactory("vdb", 1, "mongodb", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        MongoDBConnection conn = Mockito.mock(MongoDBConnection.class);
        DBCollection tableDBCollection = Mockito.mock(DBCollection.class);
        DBCollection embeddedDBCollection = Mockito.mock(DBCollection.class);
        DBCollection emptyDBCollection = Mockito.mock(DBCollection.class);
        DBCollection emptyFirstDBCollection = Mockito.mock(DBCollection.class);
        LinkedHashSet<String> tables = new LinkedHashSet<String>();
        tables.add("table");
        tables.add("embedded");
        tables.add("empty");
        tables.add("emptyFirst");

        DB db = Mockito.mock(DB.class);

        BasicDBList array = new BasicDBList();
        array.add("one");
        array.add("two");

        BasicDBObject row = new BasicDBObject();
        row.append("_id", new Integer(1));
        row.append("col2", new Double(2.0));
        row.append("col3", new Long(3L));
        row.append("col5", Boolean.TRUE);
        row.append("col6", new Date(0L));
        row.append("col6", new DBRef(db.getName(), "ns", "one"));
        row.append("col7", array);
        row.append("col8", new Binary("binary".getBytes()));

        BasicDBObject child = new BasicDBObject();
        child.append("col1", "one");
        child.append("col2", "two");
        row.append("child", child);

        BasicDBObject emptyFirstRow = new BasicDBObject();
        emptyFirstRow.append("_id", new ObjectId("5835a598944716c40d2f26ae"));
        emptyFirstRow.append("col2", new Double(2.0));
        emptyFirstRow.append("col3", new Long(3L));

        BasicDBObject embedded = new BasicDBObject();
        embedded.append("col1", "one");
        embedded.append("col2", "two");
        row.append("embedded", embedded);

        Mockito.stub(db.getCollectionNames()).toReturn(tables);
        Mockito.stub(db.getCollection(Mockito.eq("table"))).toReturn(tableDBCollection);
        Mockito.stub(db.getCollection(Mockito.eq("embedded"))).toReturn(embeddedDBCollection);
        Mockito.stub(db.getCollection(Mockito.eq("empty"))).toReturn(emptyDBCollection);
        Mockito.stub(db.getCollection(Mockito.eq("emptyFirst"))).toReturn(emptyFirstDBCollection);

        DBCursor tableCursor = Mockito.mock(DBCursor.class);
        Mockito.when(tableCursor.hasNext()).thenReturn(true).thenReturn(false);
        Mockito.when(tableCursor.next()).thenReturn(row);
        Mockito.when(tableDBCollection.find()).thenReturn(tableCursor);

        DBCursor embeddedCursor = Mockito.mock(DBCursor.class);
        Mockito.when(embeddedCursor.hasNext()).thenReturn(true).thenReturn(false);
        Mockito.when(embeddedCursor.next()).thenReturn(child);
        Mockito.when(embeddedDBCollection.find()).thenReturn(embeddedCursor);

        DBCursor emptyFirstCursor = Mockito.mock(DBCursor.class);
        Mockito.when(emptyFirstCursor.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(emptyFirstCursor.next()).thenReturn(null).thenReturn(emptyFirstRow);
        Mockito.when(emptyFirstDBCollection.find()).thenReturn(emptyFirstCursor);

        DBCursor emptyCursor = Mockito.mock(DBCursor.class);
        Mockito.when(emptyCursor.hasNext()).thenReturn(true).thenReturn(false);
        Mockito.when(emptyCursor.next()).thenReturn(null);
        Mockito.when(emptyDBCollection.find()).thenReturn(emptyCursor);

        Mockito.stub(conn.getDatabase()).toReturn(db);

        mp.process(mf, conn);

        String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        String expected = "CREATE FOREIGN TABLE \"table\" (\n" +
                "    \"_id\" integer,\n" +
                "    col2 double,\n" +
                "    col3 long,\n" +
                "    col5 boolean,\n" +
                "    col6 string,\n" +
                "    col7 string[] OPTIONS (SEARCHABLE 'Unsearchable'),\n" +
                "    col8 varbinary OPTIONS (NATIVE_TYPE 'org.bson.types.Binary'),\n" +
                "    CONSTRAINT PK0 PRIMARY KEY(\"_id\")\n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_rel:fqn\" 'collection=table');\n" +
                "\n" +
                "CREATE FOREIGN TABLE child (\n" +
                "    col1 string,\n" +
                "    col2 string,\n" +
                "    \"_id\" integer OPTIONS (UPDATABLE FALSE),\n" +
                "    CONSTRAINT PK0 PRIMARY KEY(\"_id\"),\n" +
                "    FOREIGN KEY(\"_id\") REFERENCES \"table\" \n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_mongo:MERGE\" 'table', \"teiid_rel:fqn\" 'collection=table/embedded=child');\n" +
                "\n" +
                "CREATE FOREIGN TABLE embedded (\n" +
                "    col1 string,\n" +
                "    col2 string,\n" +
                "    \"_id\" integer OPTIONS (UPDATABLE FALSE),\n" +
                "    CONSTRAINT PK0 PRIMARY KEY(\"_id\"),\n" +
                "    FOREIGN KEY(\"_id\") REFERENCES \"table\" \n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_mongo:MERGE\" 'table', \"teiid_rel:fqn\" 'collection=table/embedded=embedded');";
        assertEquals(expected, metadataDDL.replace("\t", "    "));
    }

    @Test
    public void testEmbeddedList() throws TranslatorException {
        MongoDBMetadataProcessor mp = new MongoDBMetadataProcessor();

        MetadataFactory mf = new MetadataFactory("vdb", 1, "mongodb", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        MongoDBConnection conn = Mockito.mock(MongoDBConnection.class);
        DBCollection tableDBCollection = Mockito.mock(DBCollection.class);
        LinkedHashSet<String> tables = new LinkedHashSet<String>();
        tables.add("table");

        DB db = Mockito.mock(DB.class);

        BasicDBList list = new BasicDBList();

        BasicDBObject child = new BasicDBObject();
        child.append("col1", "one");
        child.append("col2", "two");
        list.add(child);

        BasicDBObject row = new BasicDBObject();
        row.append("_id", 1);
        row.append("col1", list);

        Mockito.stub(db.getCollectionNames()).toReturn(tables);
        Mockito.stub(db.getCollection(Mockito.eq("table"))).toReturn(tableDBCollection);

        DBCursor tableCursor = Mockito.mock(DBCursor.class);
        Mockito.when(tableCursor.hasNext()).thenReturn(true).thenReturn(false);
        Mockito.when(tableCursor.next()).thenReturn(row);
        Mockito.when(tableDBCollection.find()).thenReturn(tableCursor);

        Mockito.stub(conn.getDatabase()).toReturn(db);

        mp.process(mf, conn);

        String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        String expected = "CREATE FOREIGN TABLE \"table\" (\n" +
                "    \"_id\" integer,\n" +
                "    CONSTRAINT PK0 PRIMARY KEY(\"_id\")\n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_rel:fqn\" 'collection=table');\n" +
                "\n" +
                "CREATE FOREIGN TABLE col1 (\n" +
                "    col1 string,\n" +
                "    col2 string,\n" +
                "    table__id integer OPTIONS (UPDATABLE FALSE),\n" +
                "    FOREIGN KEY(table__id) REFERENCES \"table\" \n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_mongo:MERGE\" 'table', \"teiid_rel:fqn\" 'collection=table/embedded=col1');";
        assertEquals(expected, metadataDDL.replace("\t", "    "));
    }
}
