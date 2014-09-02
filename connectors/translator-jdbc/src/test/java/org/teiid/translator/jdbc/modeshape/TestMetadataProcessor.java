package org.teiid.translator.jdbc.modeshape;

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.metadata.MetadataFactory;
import org.teiid.query.metadata.DDLStringVisitor;
import org.teiid.query.metadata.SystemMetadata;
import org.teiid.translator.TranslatorException;

@SuppressWarnings("nls")
public class TestMetadataProcessor {
	protected static ModeShapeExecutionFactory TRANSLATOR;

	@BeforeClass
	public static void setUp() throws TranslatorException {
		TRANSLATOR = new ModeShapeExecutionFactory();
		TRANSLATOR.start();
	}

	@Test
	public void testMetadata() throws Exception {

		MetadataFactory mf = new MetadataFactory("vdb", 1, "objectvdb",
				SystemMetadata.getInstance().getRuntimeTypeMap(),
				new Properties(), null);
		
		ModeShapeJDBCMetdataProcessor mp = new ModeShapeJDBCMetdataProcessor();

		mp.addModeShapeProcedures(mf);
		
		String metadataDDL = DDLStringVisitor.getDDLString(mf.getSchema(),
				null, null);

		System.out.println("Schema: " + metadataDDL);
		String expected = "CREATE FOREIGN FUNCTION JCR_ISCHILDNODE(path1 string, path2 string) RETURNS boolean\n"
		+ "OPTIONS (UUID '1525', CATEGORY 'JCR');\n\n"

		+ "CREATE FOREIGN FUNCTION JCR_ISSAMENODE(path1 string, path2 string) RETURNS boolean\n"
		+ "OPTIONS (UUID '1526', CATEGORY 'JCR');\n\n"

		+ "CREATE FOREIGN FUNCTION JCR_ISDESCENDANTNODE(path1 string, path2 string) RETURNS boolean\n"
		+ "OPTIONS (UUID '1527', CATEGORY 'JCR');\n\n"

		+ "CREATE FOREIGN FUNCTION JCR_REFERENCE(selectOrProperty string) RETURNS boolean\n"
		+ "OPTIONS (UUID '1528', CATEGORY 'JCR');\n\n"

		+ "CREATE FOREIGN FUNCTION JCR_CONTAINS(selectOrProperty string, searchExpr string) RETURNS boolean\n"
		+ "OPTIONS (UUID '1529', CATEGORY 'JCR', DETERMINISM 'NONDETERMINISTIC');"
				;
				
		assertEquals(expected, metadataDDL);	

	}

}
