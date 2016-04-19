package org.teiid.translator.infinispan.dsl.metadata;

import static org.junit.Assert.*;

import java.util.Properties;

import org.jboss.as.quickstarts.datagrid.hotrod.query.domain.PersonCacheSource;
import org.junit.Before;
import org.junit.Test;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.infinispan.dsl.InfinispanExecutionFactory;
import org.teiid.translator.object.ObjectConnection;
import org.teiid.translator.object.ObjectExecutionFactory;

@SuppressWarnings("nls")
public class TestAnnotationMetadataProcessor {

	protected static ObjectExecutionFactory TRANSLATOR;

	
	@Before public void beforeEach() throws Exception{	
		 
		TRANSLATOR = new InfinispanExecutionFactory();
		TRANSLATOR.setSupportsSearchabilityUsingAnnotations(true);
		TRANSLATOR.start();    }
	
	@Test
	public void testPersonMetadata() throws Exception {

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);

		ObjectConnection conn = PersonCacheSource.createConnection(false);

		TRANSLATOR.getMetadataProcessor().process(mf, conn);

		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		System.out.println("Schema: " + metadataDDL);
		String expected = "CREATE FOREIGN TABLE Person (\n"
	    + "\tPersonObject object OPTIONS (NAMEINSOURCE 'this', SELECTABLE FALSE, UPDATABLE FALSE, SEARCHABLE 'Unsearchable', NATIVE_TYPE 'org.jboss.as.quickstarts.datagrid.hotrod.query.domain.Person'),\n"
	    + "\tid integer NOT NULL OPTIONS (NAMEINSOURCE 'id', SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),\n"
	    + "\temail string OPTIONS (NAMEINSOURCE 'email', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"
	    + "\tname string NOT NULL OPTIONS (NAMEINSOURCE 'name', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"	    
		+ "\tCONSTRAINT PK_ID PRIMARY KEY(id)\n"
		+ ") OPTIONS (UPDATABLE TRUE);"
//		+ "\n"
//		+ "CREATE FOREIGN TABLE PhoneNumber (\n"
//	    + "\tnumber string OPTIONS (NAMEINSOURCE 'phone.number', SEARCHABLE 'Searchable', NATIVE_TYPE 'java.lang.String'),\n"	    
//	    + "\ttype string OPTIONS (NAMEINSOURCE 'phone.type', SEARCHABLE 'Unsearchable', NATIVE_TYPE 'java.lang.Enum'),\n"
//	    + "\tid integer NOT NULL OPTIONS (NAMEINSOURCE 'id', SELECTABLE FALSE, UPDATABLE FALSE, SEARCHABLE 'Searchable', NATIVE_TYPE 'int'),\n"
//		+ "\tCONSTRAINT FK_PERSON FOREIGN KEY(id) REFERENCES Person (id) OPTIONS (NAMEINSOURCE 'phones')\n"
//		+ ") OPTIONS (UPDATABLE TRUE);"	;		
		
		;
		assertEquals(expected, metadataDDL);	

	}

}
