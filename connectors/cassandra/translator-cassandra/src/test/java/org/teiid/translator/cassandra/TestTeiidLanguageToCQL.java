package org.teiid.translator.cassandra;

import static org.junit.Assert.*;

import java.util.Properties;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.cdk.CommandBuilder;
import org.teiid.core.util.TimestampWithTimezone;
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

    private QueryMetadataInterface cassandraMetadata(Properties modelProperties) {

        MetadataFactory factory = new MetadataFactory("", 1, "", SystemMetadata.getInstance().getRuntimeTypeMap(), modelProperties, "");
        createFakeMetadata(factory);
        return new TransformationMetadata(null, new CompositeMetadataStore(factory.asMetadataStore()), null, RealMetadataFactory.SFM.getSystemFunctions(), null);
    }

    private void createFakeMetadata(MetadataFactory factory) {
        Table person = factory.addTable("Person");
        factory.addColumn("id", TypeFacility.RUNTIME_NAMES.INTEGER, person);
        factory.addColumn("name", TypeFacility.RUNTIME_NAMES.STRING, person);
        Column c = factory.addColumn("age", TypeFacility.RUNTIME_NAMES.INTEGER, person);
        c.setNameInSource("\"Age\"");
        factory.addColumn("bday", TypeFacility.RUNTIME_NAMES.DATE, person);
        factory.addColumn("bts", TypeFacility.RUNTIME_NAMES.TIMESTAMP, person);
        factory.addColumn("employed", TypeFacility.RUNTIME_NAMES.BOOLEAN, person);
        factory.addColumn("custom", TypeFacility.RUNTIME_NAMES.VARBINARY, person);
        factory.addColumn("custom1", TypeFacility.RUNTIME_NAMES.BLOB, person);
        Properties pros = factory.getModelProperties();
        for(Object key: pros.keySet()) {
            person.setProperty(key.toString(), pros.getProperty(key.toString()));
        }
    }


    private String getTranslation(String sql, Properties modelProperties){
        CommandBuilder builder = new CommandBuilder(cassandraMetadata(modelProperties));
        Command c = builder.getCommand(sql);
        CassandraSQLVisitor visitor = new CassandraSQLVisitor();
        visitor.translateSQL(c);
        return visitor.getTranslatedSQL();
    }


    @Test
    public void testSelect() throws Exception{
        Properties props = new Properties();
        assertEquals("SELECT id FROM Person", getTranslation("select id FROM Person", props));

        assertEquals("SELECT name, \"Age\" FROM Person", getTranslation("select name,age from Person", props));

        assertEquals("SELECT id, name, \"Age\", bday, bts, employed, custom, custom1 FROM Person", getTranslation("SELECT * FROM Person", props));

        assertEquals("SELECT COUNT(*) FROM Person LIMIT 10", getTranslation("SELECT count(*) FROM Person limit 10", props));

        assertEquals("SELECT id, name, \"Age\" FROM Person WHERE id = 1 AND \"Age\" >= 18 AND \"Age\" <= 100", getTranslation("SELECT id, name, age from Person where id=1 and age>=18 and age<=100", props));

        assertEquals("SELECT id, name, \"Age\" FROM Person WHERE id IN (1, 2, 3)", getTranslation("SELECT id, name, age from Person where id in(1,2,3)", props));

        assertEquals("SELECT id FROM Person WHERE bday = '1900-01-01' AND employed = TRUE", getTranslation("select id from Person where bday = {d '1900-01-01'} and employed = true", props));

        assertEquals("SELECT id FROM Person WHERE bts = -2208949200000 AND employed = TRUE", getTranslation("select id from Person where bts = {ts '1900-01-01 12:00:00'} and employed = true", props));

        //assertEquals("SELECT id FROM Person where custom = X'abcd'", getTranslation("SELECT id FROM Person WHERE custom = 0xABCD", props));

        // assertEquals("INSERT into Person (id, custom1) values (1, X'abcd')",getTranslation("INSERT INTO Person (id, custom1) VALUES (1, 0xABCD)", props));
    }

    @Test
    public void testSelectWithAllowFiltering() throws Exception {
        Properties props = new Properties();
        props.put(CassandraMetadataProcessor.ALLOWFILTERING, "TRUE");
        assertEquals("SELECT id, name, \"Age\", bday, employed, custom, custom FROM Person WHERE \"Age\" = 8 ALLOW FILTERING",
                getTranslation("select id, name, age, bday, employed, custom, custom from Person WHERE age = 8",
                        props));
    }

    @BeforeClass
    public static void oneTimeSetup() {
        TimestampWithTimezone.resetCalendar(TimeZone.getTimeZone("GMT+1"));
    }

    @AfterClass
    public static void oneTimeTeardown() {
        TimestampWithTimezone.resetCalendar(null);
    }
}
