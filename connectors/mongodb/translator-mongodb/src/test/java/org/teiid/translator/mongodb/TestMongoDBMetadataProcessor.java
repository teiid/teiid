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

import static org.junit.Assert.*;

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
        String expected = "SET NAMESPACE 'http://www.teiid.org/translator/mongodb/2013' AS teiid_mongo;\n\n" +
                "CREATE FOREIGN TABLE \"table\" (\n" + 
                "\t\"_id\" integer,\n" + 
                "\tcol2 double,\n" + 
                "\tcol3 long,\n" + 
                "\tcol5 boolean,\n" + 
                "\tcol6 string,\n" + 
                "\tcol7 object[] OPTIONS (SEARCHABLE 'Unsearchable'),\n"+
                "\tcol8 varbinary OPTIONS (NATIVE_TYPE 'org.bson.types.Binary'),\n"+
                "\tCONSTRAINT PK0 PRIMARY KEY(\"_id\"),\n" + 
                "\tCONSTRAINT FK_col6 FOREIGN KEY(col6) REFERENCES ns \n" + 
                ") OPTIONS (UPDATABLE TRUE);\n" +
                "\n" + 
                "CREATE FOREIGN TABLE child (\n" + 
        		"\tcol1 string,\n" + 
        		"\tcol2 string,\n" + 
        	    "\t\"_id\" integer OPTIONS (UPDATABLE FALSE),\n"+
        	    "\tCONSTRAINT PK0 PRIMARY KEY(\"_id\"),\n"+       
        	    "\tFOREIGN KEY(\"_id\") REFERENCES \"table\" \n" +
        		") OPTIONS (UPDATABLE TRUE, \"teiid_mongo:MERGE\" 'table');\n" + 
        		"\n" + 
        		"CREATE FOREIGN TABLE embedded (\n" + 
        		"\tcol1 string,\n" + 
        		"\tcol2 string,\n" + 
        		"\t\"_id\" integer OPTIONS (UPDATABLE FALSE),\n" + 
        		"\tCONSTRAINT PK0 PRIMARY KEY(\"_id\"),\n" + 
        		"\tFOREIGN KEY(\"_id\") REFERENCES \"table\" \n" +
        		") OPTIONS (UPDATABLE TRUE, \"teiid_mongo:EMBEDDABLE\" 'true');\n" + 
        		"\n" +
        		"CREATE FOREIGN TABLE emptyFirst (\n" + 
        		"\t\"_id\" string AUTO_INCREMENT OPTIONS (NATIVE_TYPE 'org.bson.types.ObjectId'),\n" + 
        		"\tcol2 double,\n" + 
        		"\tcol3 long,\n" + 
        		"\tCONSTRAINT PK0 PRIMARY KEY(\"_id\")\n" + 
        		") OPTIONS (UPDATABLE TRUE);";
        assertEquals(expected, metadataDDL);
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
        String expected = "SET NAMESPACE 'http://www.teiid.org/translator/mongodb/2013' AS teiid_mongo;\n\n" +
                "CREATE FOREIGN TABLE \"table\" (\n" + 
                "\t\"_id\" integer,\n" + 
                "\tcol2 double,\n" + 
                "\tcol3 long,\n" + 
                "\tcol5 boolean,\n" + 
                "\tcol6 string,\n" + 
                "\tcol7 object[] OPTIONS (SEARCHABLE 'Unsearchable'),\n"+
                "\tcol8 varbinary OPTIONS (NATIVE_TYPE 'org.bson.types.Binary'),\n"+
                "\tCONSTRAINT PK0 PRIMARY KEY(\"_id\"),\n" + 
                "\tCONSTRAINT FK_col6 FOREIGN KEY(col6) REFERENCES ns \n" + 
                ") OPTIONS (UPDATABLE TRUE);\n" +
                "\n" + 
                "CREATE FOREIGN TABLE child (\n" + 
                "\tcol1 string,\n" + 
                "\tcol2 string,\n" + 
                "\t\"_id\" integer OPTIONS (UPDATABLE FALSE),\n"+
                "\tCONSTRAINT PK0 PRIMARY KEY(\"_id\"),\n"+       
                "\tFOREIGN KEY(\"_id\") REFERENCES \"table\" \n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_mongo:MERGE\" 'table');\n" + 
                "\n" + 
                "CREATE FOREIGN TABLE embedded (\n" + 
                "\tcol1 string,\n" + 
                "\tcol2 string,\n" + 
                "\t\"_id\" integer OPTIONS (UPDATABLE FALSE),\n" + 
                "\tCONSTRAINT PK0 PRIMARY KEY(\"_id\"),\n" + 
                "\tFOREIGN KEY(\"_id\") REFERENCES \"table\" \n" +
                ") OPTIONS (UPDATABLE TRUE, \"teiid_mongo:MERGE\" 'table');";
        assertEquals(expected, metadataDDL);
    }    
}
