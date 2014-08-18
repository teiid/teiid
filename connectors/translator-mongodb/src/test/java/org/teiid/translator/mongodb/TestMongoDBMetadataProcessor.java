package org.teiid.translator.mongodb;

import static org.junit.Assert.assertEquals;

import java.sql.Date;
import java.util.HashSet;
import java.util.Properties;

import org.bson.types.Binary;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.metadata.MetadataFactory;
import org.teiid.mongodb.MongoDBConnection;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.TranslatorException;

import com.mongodb.*;

@SuppressWarnings("nls")
public class TestMongoDBMetadataProcessor {

    @Test
    public void testMetadata() throws TranslatorException {
        MongoDBMetadataProcessor mp = new MongoDBMetadataProcessor();
        
        MetadataFactory mf = new MetadataFactory("vdb", 1, "mongodb", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), null);
        MongoDBConnection conn = Mockito.mock(MongoDBConnection.class);
        DBCollection dbCollection = Mockito.mock(DBCollection.class);
        HashSet<String> tables = new HashSet<String>();
        tables.add("table");
        tables.add("embedded");
        
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
        row.append("col6", new DBRef(db, "ns", "one"));
        row.append("col7", array);
        row.append("col8", new Binary("binary".getBytes()));
        
        BasicDBObject child = new BasicDBObject();
        child.append("col1", "one");
        child.append("col2", "two");
        row.append("child", child);
        
        BasicDBObject embedded = new BasicDBObject();
        embedded.append("col1", "one");
        embedded.append("col2", "two");
        row.append("embedded", embedded);
        
        Mockito.stub(db.getCollectionNames()).toReturn(tables);
        Mockito.stub(db.getCollection(Mockito.anyString())).toReturn(dbCollection);
        Mockito.stub(dbCollection.findOne()).toReturn(row);
        Mockito.stub(conn.getDatabase()).toReturn(db);
        
        mp.process(mf, conn);

        String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(), null, null);
        String expected = "SET NAMESPACE 'http://www.teiid.org/translator/mongodb/2013' AS teiid_mongo;\n\n" +
        		"CREATE FOREIGN TABLE child (\n" + 
        		"\tcol1 string,\n" + 
        		"\tcol2 string,\n" + 
        	    "\t\"_id\" integer OPTIONS (UPDATABLE FALSE),\n"+
        	    "\tCONSTRAINT PK0 PRIMARY KEY(\"_id\")\n"+        		
        		") OPTIONS (UPDATABLE TRUE, \"teiid_mongo:MERGE\" 'table');\n" + 
        		"\n" + 
        		"CREATE FOREIGN TABLE embedded (\n" + 
        		"\tcol1 string,\n" + 
        		"\tcol2 string,\n" + 
        		"\t\"_id\" integer OPTIONS (UPDATABLE FALSE),\n" + 
        		"\tCONSTRAINT PK0 PRIMARY KEY(\"_id\")\n" + 
        		") OPTIONS (UPDATABLE TRUE, \"teiid_mongo:EMBEDDABLE\" 'true');\n" + 
        		"\n" +
        		"CREATE FOREIGN TABLE \"table\" (\n" + 
        		"\t\"_id\" integer,\n" + 
        		"\tcol2 double,\n" + 
        		"\tcol3 long,\n" + 
        		"\tcol5 boolean,\n" + 
        		"\tcol6 string,\n" + 
        		"\tcol7 object[] OPTIONS (SEARCHABLE 'Unsearchable'),\n"+
        		"\tcol8 varbinary,\n"+
        		"\tCONSTRAINT PK0 PRIMARY KEY(\"_id\"),\n" + 
        		"\tCONSTRAINT FK_col6 FOREIGN KEY(col6) REFERENCES ns \n" + 
        		") OPTIONS (UPDATABLE TRUE);";
        assertEquals(expected, metadataDDL);
    }
}
