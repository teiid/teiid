package org.teiid.translator.cassandra;

import java.util.Properties;

import static org.junit.Assert.*;

import org.junit.Test;
import org.teiid.cdk.CommandBuilder;
import org.teiid.language.Command;
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

	private QueryMetadataInterface cassandraMetadata(Properties modelProperties) {

		MetadataFactory factory = new MetadataFactory("", 1, "", SystemMetadata.getInstance().getRuntimeTypeMap(),
				modelProperties, "");
		createFakeMetadata(factory);
		return new TransformationMetadata(null, new CompositeMetadataStore(factory.asMetadataStore()), null,
				RealMetadataFactory.SFM.getSystemFunctions(), null);
	}

	private void createFakeMetadata(MetadataFactory factory) {
		Table person = factory.addTable("Person");
		factory.addColumn("id", TypeFacility.RUNTIME_NAMES.INTEGER, person);
		factory.addColumn("name", TypeFacility.RUNTIME_NAMES.STRING, person);
		factory.addColumn("age", TypeFacility.RUNTIME_NAMES.INTEGER, person);
		factory.addColumn("bday", TypeFacility.RUNTIME_NAMES.TIMESTAMP, person);
		factory.addColumn("employed", TypeFacility.RUNTIME_NAMES.BOOLEAN, person);
		factory.addColumn("custom", TypeFacility.RUNTIME_NAMES.VARBINARY, person);
		factory.addColumn("custom1", TypeFacility.RUNTIME_NAMES.BLOB, person);
		factory.getModelProperties().forEach((k, v) -> {
			person.setProperty(k.toString(), v.toString());
		});
	}

	private String getTranslation(String sql, Properties modelProperties) {
		CommandBuilder builder = new CommandBuilder(cassandraMetadata(modelProperties));
		Command c = builder.getCommand(sql);
		CassandraSQLVisitor visitor = new CassandraSQLVisitor();
		visitor.translateSQL(c);
		return visitor.getTranslatedSQL();
	}

	@Test
	public void testSelect() throws Exception {
		Properties props = new Properties();
		assertEquals("SELECT id FROM Person", getTranslation("select id FROM Person", props));

		assertEquals("SELECT name, age FROM Person", getTranslation("select name,age from Person", props));

		assertEquals("SELECT id, name, age, bday, employed, custom, custom1 FROM Person",
				getTranslation("SELECT * FROM Person", props));

		assertEquals("SELECT COUNT(*) FROM Person LIMIT 10",
				getTranslation("SELECT count(*) FROM Person limit 10", props));

		assertEquals("SELECT id, name, age FROM Person WHERE id = 1 AND age >= 18 AND age <= 100",
				getTranslation("SELECT id, name, age from Person where id=1 and age>=18 and age<=100", props));

		assertEquals("SELECT id, name, age FROM Person WHERE id IN (1, 2, 3)",
				getTranslation("SELECT id, name, age from Person where id in(1,2,3)", props));

		assertEquals("SELECT id FROM Person WHERE bday = -2208966800000 AND employed = TRUE", getTranslation(
				"select id from Person where bday = {ts '1900-01-01 12:00:00'} and employed = true", props));

		//assertEquals("SELECT id FROM Person where custom = X'abcd'", getTranslation("SELECT id FROM Person WHERE custom = 0xABCD", props));

		//assertEquals("INSERT into Person (id, custom1) values (1, X'abcd')",getTranslation("INSERT INTO Person (id, custom1) VALUES (1, 0xABCD)", props));
	}

	@Test
	public void testSelectWithAllowFiltering() throws Exception {
		Properties props = new Properties();
		props.put(CassandraMetadataProcessor.ALLOWFILTERING, "TRUE");
		assertEquals("SELECT id, name, age, bday, employed, custom, custom FROM Person WHERE age = 8 ALLOW FILTERING",
				getTranslation("select id, name, age, bday, employed, custom, custom from Person WHERE age = 8",
						props));
	}

}
