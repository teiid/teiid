package org.teiid.translator.cassandra;

import java.util.Properties;

import junit.framework.Assert;

import org.junit.Test;
import org.teiid.cdk.CommandBuilder;
import org.teiid.language.Command;
import org.teiid.language.Select;
import org.teiid.metadata.Column;
import org.teiid.metadata.MetadataFactory;
import org.teiid.metadata.Table;
import org.teiid.query.metadata.CompositeMetadataStore;
import org.teiid.query.metadata.QueryMetadataInterface;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.query.metadata.TransformationMetadata;
import org.teiid.query.unittest.RealMetadataFactory;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.cassandra.execution.CassandraSQLVisitor;

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
	}


	private void testTranslation(String sql, String expectedCQL){
		Select select = (Select)getCommand(sql);
		
		CassandraSQLVisitor visitor = new CassandraSQLVisitor();
		visitor.translateSQL(select);
		Assert.assertEquals(expectedCQL, visitor.getTranslatedSQL());
	}
	
	public Command getCommand(String sql){
		CommandBuilder builder = new CommandBuilder(cassandraMetadata());
		return builder.getCommand(sql);
	}
	
	@Test
	public void testSelect() throws Exception{
		testTranslation("select id from Person", "SELECT id FROM Person");
		testTranslation("select name,age from Person", "SELECT name, age FROM Person");
		testTranslation("select * from Person", "SELECT * FROM Person");
		testTranslation("select count(*) from Person limit 10", "SELECT COUNT(*) FROM Person LIMIT 10");
		testTranslation("select * from Person where id=1 and age>=18 and age<=100", "SELECT * FROM Person WHERE id = 1 AND age >= 18 AND age <= 100");
		testTranslation("select * from Person where id in(1,2,3)", "SELECT * FROM Person WHERE id IN (1, 2, 3)");
		
	}
	
}
