package org.teiid.translator.cassandra;

import static org.junit.Assert.*;

import java.util.Properties;

import org.junit.Test;
import org.teiid.cdk.CommandBuilder;
import org.teiid.language.Command;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.TypeFacility;

@SuppressWarnings("nls")
public class TestTeiidLanguageToCQL {
	
	private QueryMetadataInterface cassandraMetadata() {
			
		MetadataFactory factory = new MetadataFactory("", 1, "", SystemMetadata.getInstance().getRuntimeTypeMap(), new Properties(), "");
		createFakeMetadata(factory);
		return new TransformationMetadata(null, new CompositeMetadataStore(factory.asMetadataStore()), null, RealMetadataFactory.SFM.getSystemFunctions(), null);
	}
	
	private void createFakeMetadata(MetadataFactory factory) {
		Table person = factory.addTable("Person");
		Column id = factory.addColumn("id", TypeFacility.RUNTIME_NAMES.INTEGER, person);
		Column name = factory.addColumn("name", TypeFacility.RUNTIME_NAMES.STRING, person);
		Column age = factory.addColumn("age", TypeFacility.RUNTIME_NAMES.INTEGER, person);
		factory.addColumn("bday", TypeFacility.RUNTIME_NAMES.TIMESTAMP, person);
		factory.addColumn("employed", TypeFacility.RUNTIME_NAMES.BOOLEAN, person);
		factory.addColumn("custom", TypeFacility.RUNTIME_NAMES.VARBINARY, person);
		factory.addColumn("custom1", TypeFacility.RUNTIME_NAMES.BLOB, person);
	}


	private void testTranslation(String sql, String expectedCQL){
		Command c = getCommand(sql);
		
		CassandraSQLVisitor visitor = new CassandraSQLVisitor();
		visitor.translateSQL(c);
		assertEquals(expectedCQL, visitor.getTranslatedSQL());
	}
	
	public Command getCommand(String sql){
		CommandBuilder builder = new CommandBuilder(cassandraMetadata());
		return builder.getCommand(sql);
	}
	
	@Test
	public void testSelect() throws Exception{
		testTranslation("select id from Person", "SELECT id FROM Person");
		testTranslation("select name,age from Person", "SELECT name, age FROM Person");
		testTranslation("select * from Person", "SELECT id, name, age, bday, employed, custom, custom1 FROM Person");
		testTranslation("select count(*) from Person limit 10", "SELECT COUNT(*) FROM Person LIMIT 10");
		testTranslation("select id, name, age from Person where id=1 and age>=18 and age<=100", "SELECT id, name, age FROM Person WHERE id = 1 AND age >= 18 AND age <= 100");
		testTranslation("select id, name, age from Person where id in(1,2,3)", "SELECT id, name, age FROM Person WHERE id IN (1, 2, 3)");
		//testTranslation("select id from Person where bday = {ts '1900-01-01 12:00:00'} and employed = true", "SELECT id FROM Person WHERE bday = -2208927600000 AND employed = TRUE");
		testTranslation("select id from Person where custom = X'abcd'", "SELECT id FROM Person WHERE custom = 0xABCD");
		
		testTranslation("insert into Person (id, custom1) values (1, X'abcd')", "INSERT INTO Person (id, custom1) VALUES (1, 0xABCD)");
	}
	
}
