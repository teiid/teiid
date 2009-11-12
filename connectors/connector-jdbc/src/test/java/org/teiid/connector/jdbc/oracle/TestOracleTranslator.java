/*
 * JBoss, Home of Professional Open Source.
 * See the COPYRIGHT.txt file distributed with this work for information
 * regarding copyright ownership.  Some portions may be licensed
 * to Red Hat, Inc. under one or more contributor license agreements.
 * 
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */

package org.teiid.connector.jdbc.oracle;

import java.io.File;
import java.net.URL;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.jdbc.TranslationHelper;
import org.teiid.connector.jdbc.translator.TranslatedCommand;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.language.ICommand;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.unittest.FakeTranslationFactory;
import com.metamatrix.query.function.FunctionLibraryManager;
import com.metamatrix.query.function.UDFSource;

public class TestOracleTranslator {
	
    /**
     * An instance of {@link Translator} which has already been initialized.  
     */
    private static Translator TRANSLATOR; 

    @BeforeClass public static void oneTimeSetup() throws Exception {
        TRANSLATOR = new OracleSQLTranslator();        
        TRANSLATOR.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
        // Define a UDFSource to hold the reference to our function definitions
        File udfFile = new File("src/main/resources/OracleSpatialFunctions.xmi"); //$NON-NLS-1$;
        URL[] urls = new URL[0];            
        UDFSource udfSource = new UDFSource(udfFile.toURI().toURL(), urls);
        FunctionLibraryManager.deregisterSource(udfSource);
        FunctionLibraryManager.registerSource(udfSource);
    }

    /**
     * Performs cleanup tasks that should be executed after all test methods 
     * have been executed.  This method should only be executed once and does 
     * not protect from multiple executions.  It is intended to be executed by 
     * the JUnit4 test framework.
     * <p>
     * This method unloads the function definitions supported by the Oracle Spatial 
     * Connector from the global instance of <code>FunctionLibraryManager</code> 
     * so that they are no longer resolvable during query parsing. 
     * 
     * @throws Exception
     */
    @AfterClass public static void oneTimeFinished() throws Exception {
        // Define a UDFSource to hold the reference to our function definitions
        File udfFile = new File("src/main/resources/OracleSpatialFunctions.xmi"); //$NON-NLS-1$;
        URL[] urls = new URL[0];            
        UDFSource udfSource = new UDFSource(udfFile.toURI().toURL(), urls);
        FunctionLibraryManager.deregisterSource(udfSource);
    }

	private void helpTestVisitor(String input, String expectedOutput) throws ConnectorException {
        // Convert from sql to objects
        ICommand obj = FakeTranslationFactory.getInstance().getAutoIncrementTranslationUtility().parseCommand(input);
        
        TranslatedCommand tc = new TranslatedCommand(EnvironmentUtility.createSecurityContext("user"), TRANSLATOR); //$NON-NLS-1$
        tc.translateCommand(obj);
        
        // Check stuff
        org.junit.Assert.assertEquals("Did not get correct sql", expectedOutput, tc.getSql()); //$NON-NLS-1$
    }
	
	@Test public void testInsertWithSequnce() throws Exception {
		helpTestVisitor("insert into test.group (e0) values (1)", "INSERT INTO group (e0, e1) VALUES (1, MYSEQUENCE.nextVal)"); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	@Test public void testInsertWithSequnce1() throws Exception {
		helpTestVisitor("insert into test.group (e0, e1) values (1, 'x')", "INSERT INTO group (e0, e1) VALUES (1, 'x')"); //$NON-NLS-1$ //$NON-NLS-2$
	}
	
	@Test public void testJoins() throws Exception {
        String input = "select smalla.intkey from bqt1.smalla inner join bqt1.smallb on smalla.stringkey=smallb.stringkey cross join bqt1.mediuma"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA INNER JOIN SmallB ON SmallA.StringKey = SmallB.StringKey CROSS JOIN MediumA"; //$NON-NLS-1$
          
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input, 
            output, TRANSLATOR);        
    }
    
	@Test public void testJoins2() throws Exception {
        String input = "select smalla.intkey from bqt1.smalla cross join (bqt1.smallb cross join bqt1.mediuma)"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA CROSS JOIN (SmallB CROSS JOIN MediumA)"; //$NON-NLS-1$
      
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input, 
            output, TRANSLATOR);        
    }
    
	@Test public void testConversion1() throws Exception {
        String input = "SELECT char(convert(STRINGNUM, integer) + 100) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT chr((trunc(to_number(SmallA.StringNum)) + 100)) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input, output, 
            TRANSLATOR);
    }
          
    @Test public void testConversion2() throws Exception {
        String input = "SELECT convert(STRINGNUM, long) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT trunc(to_number(SmallA.StringNum)) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
          
    @Test public void testConversion3() throws Exception {
        String input = "SELECT convert(convert(STRINGNUM, long), string) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT to_char(trunc(to_number(SmallA.StringNum))) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
          
    @Test public void testConversion4() throws Exception {
        String input = "SELECT convert(convert(TIMESTAMPVALUE, date), string) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT to_char(trunc(cast(SmallA.TimestampValue AS date)), 'YYYY-MM-DD') FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testConversion6() throws Exception {
        String input = "SELECT convert(convert(TIMEVALUE, timestamp), string) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT to_char(cast(SmallA.TimeValue AS timestamp), 'YYYY-MM-DD HH24:MI:SS.FF') FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testConversion8() throws Exception {
        String input = "SELECT nvl(INTNUM, 'otherString') FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT nvl(to_char(SmallA.IntNum), 'otherString') FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testConversion7() throws Exception {
        String input = "SELECT convert(convert(STRINGNUM, integer), string) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT to_char(trunc(to_number(SmallA.StringNum))) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(INTNUM, 'chimp', 1) FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate() throws Exception {
        String input = "SELECT locate(INTNUM, 'chimp', 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT INSTR('chimp', to_char(SmallA.IntNum), 1) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(STRINGNUM, 'chimp') FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate2() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp') FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT INSTR('chimp', SmallA.StringNum) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(INTNUM, '234567890', 1) FROM BQT1.SMALLA WHERE INTKEY = 26</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate3() throws Exception {
        String input = "SELECT locate(INTNUM, '234567890', 1) FROM BQT1.SMALLA WHERE INTKEY = 26"; //$NON-NLS-1$
        String output = "SELECT INSTR('234567890', to_char(SmallA.IntNum), 1) FROM SmallA WHERE SmallA.IntKey = 26";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate('c', 'chimp', 1) FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate4() throws Exception {
        String input = "SELECT locate('c', 'chimp', 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT 1 FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(STRINGNUM, 'chimp', -5) FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate5() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp', -5) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT INSTR('chimp', SmallA.StringNum, 1) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(STRINGNUM, 'chimp', INTNUM) FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate6() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp', INTNUM) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT INSTR('chimp', SmallA.StringNum, CASE WHEN SmallA.IntNum < 1 THEN 1 ELSE SmallA.IntNum END) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test the translator's ability to rewrite the LOCATE() function in a form 
     * suitable for the data source.
     * <p>
     * <code>SELECT locate(STRINGNUM, 'chimp', LOCATE(STRINGNUM, 'chimp') + 1) FROM BQT1.SMALLA</code>
     *  
     * @throws Exception
     */
    @Test public void testLocate7() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp', LOCATE(STRINGNUM, 'chimp') + 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT INSTR('chimp', SmallA.StringNum, CASE WHEN (INSTR('chimp', SmallA.StringNum) + 1) < 1 THEN 1 ELSE (INSTR('chimp', SmallA.StringNum) + 1) END) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    @Test public void testSubstring1() throws Exception {
        String input = "SELECT substring(StringNum, 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT substr(SmallA.StringNum, 1) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testSubstring2() throws Exception {
        String input = "SELECT substring(StringNum, 1, 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT substr(SmallA.StringNum, 1, 1) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testUnionWithOrderBy() throws Exception {
        String input = "SELECT IntKey FROM BQT1.SMALLA UNION SELECT IntKey FROM BQT1.SMALLB ORDER BY IntKey"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA UNION SELECT SmallB.IntKey FROM SmallB ORDER BY IntKey NULLS FIRST";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRowLimit1() throws Exception {
        String input = "select intkey from bqt1.smalla limit 10, 0"; //$NON-NLS-1$
        String output = "SELECT * FROM (SELECT VIEW_FOR_LIMIT.*, ROWNUM ROWNUM_ FROM (SELECT SmallA.IntKey FROM SmallA) VIEW_FOR_LIMIT WHERE ROWNUM <= 10) WHERE ROWNUM_ > 10"; //$NON-NLS-1$
               
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRowLimit2() throws Exception {
        String input = "select intkey from bqt1.smalla limit 0, 10"; //$NON-NLS-1$
        String output = "SELECT * FROM (SELECT SmallA.IntKey FROM SmallA) WHERE ROWNUM <= 10"; //$NON-NLS-1$
               
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRowLimit3() throws Exception {
        String input = "select intkey from bqt1.smalla limit 1, 10"; //$NON-NLS-1$
        String output = "SELECT * FROM (SELECT VIEW_FOR_LIMIT.*, ROWNUM ROWNUM_ FROM (SELECT SmallA.IntKey FROM SmallA) VIEW_FOR_LIMIT WHERE ROWNUM <= 11) WHERE ROWNUM_ > 1"; //$NON-NLS-1$
               
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRowLimit4() throws Exception {
        String input = "select intkey from bqt1.mediuma limit 100"; //$NON-NLS-1$
        String output = "SELECT * FROM (SELECT MediumA.IntKey FROM MediumA) WHERE ROWNUM <= 100"; //$NON-NLS-1$
               
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRowLimit5() throws Exception {
        String input = "select intkey from bqt1.mediuma limit 50, 100"; //$NON-NLS-1$
        String output = "SELECT * FROM (SELECT VIEW_FOR_LIMIT.*, ROWNUM ROWNUM_ FROM (SELECT MediumA.IntKey FROM MediumA) VIEW_FOR_LIMIT WHERE ROWNUM <= 150) WHERE ROWNUM_ > 50"; //$NON-NLS-1$
               
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void testConcat2_useLiteral() throws Exception {        
        String input = "select concat2(stringnum,'_xx') from bqt1.Smalla"; //$NON-NLS-1$
        String output = "SELECT concat(nvl(SmallA.StringNum, ''), '_xx') FROM SmallA"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    @Test public void testConcat2() throws Exception {        
        String input = "select concat2(stringnum, stringkey) from bqt1.Smalla"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN (SmallA.StringNum IS NULL) AND (SmallA.StringKey IS NULL) THEN NULL ELSE concat(nvl(SmallA.StringNum, ''), nvl(SmallA.StringKey, '')) END FROM SmallA"; //$NON-NLS-1$
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    
    /**
     * Test a query which uses 
     * <code>sdo_relate(Object element, Object element, String literal) in its 
     * criteria into a source specific command.
     * 
     * @throws Exception
     */
    @Test public void test_sdo_relate() throws Exception {
        String input = "SELECT a.INTKEY FROM BQT1.SMALLA A, BQT1.SMALLB B WHERE sdo_relate(A.OBJECTVALUE, b.OBJECTVALUE, 'mask=ANYINTERACT') = true"; //$NON-NLS-1$
        String output = "SELECT /*+ ORDERED */ A.IntKey FROM SmallA A, SmallB B WHERE sdo_relate(A.ObjectValue, B.ObjectValue, 'mask=ANYINTERACT') = 'true'";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test a query which uses 
     * <code>sdo_within_distance(Object element, String literal, String literal) 
     * in its criteria into a source specific command.
     * 
     * @throws Exception
     */
    @Test public void test_sdo_within_distance() throws Exception {
        String input = "SELECT INTKEY FROM BQT1.SMALLA WHERE sdo_within_distance(OBJECTVALUE, 'SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL)', 'DISTANCE=25.0 UNIT=NAUT_MILE') = true"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE sdo_within_distance(SmallA.ObjectValue, SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL), 'DISTANCE=25.0 UNIT=NAUT_MILE') = 'true'";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test a query which uses 
     * <code>sdo_within_distance(String literal, Object element, String literal) 
     * in its criteria into a source specific command.
     * 
     * @throws Exception
     */
    @Test public void test_sdo_within_distance2() throws Exception {
        String input = "SELECT INTKEY FROM BQT1.SMALLA WHERE sdo_within_distance('SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL)', OBJECTVALUE, 'DISTANCE=25.0 UNIT=NAUT_MILE') = true"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE sdo_within_distance(SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL), SmallA.ObjectValue, 'DISTANCE=25.0 UNIT=NAUT_MILE') = 'true'";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test a query which uses 
     * <code>sdo_within_distance(String element, String literal, String literal) 
     * in its criteria into a source specific command.
     * 
     * @throws Exception
     */
    @Test public void test_sdo_within_distance3() throws Exception {
        String input = "SELECT INTKEY FROM BQT1.SMALLA WHERE sdo_within_distance(STRINGKEY, 'SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL)', 'DISTANCE=25.0 UNIT=NAUT_MILE') = true"; //$NON-NLS-1$
        // using ? for bind value as r marks the criteria as bindEligible 
        // due to literal of type Object appearing in left side of criteria.  
        // The literal Object is a result of the sdo_within_distance function 
        // signature being sdo_within_distance(string, object, string) : string 
        // as the signature was the best match for this query.
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE sdo_within_distance(SmallA.StringKey, SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL), 'DISTANCE=25.0 UNIT=NAUT_MILE') = ?";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test a query which uses 
     * <code>sdo_within_distance(String literal, String literal, String literal) 
     * in its criteria into a source specific command.
     * 
     * @throws Exception
     */
    @Test public void test_sdo_within_distance4() throws Exception {
        String input = "SELECT INTKEY FROM BQT1.SMALLA WHERE sdo_within_distance('SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL)', 'SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL)', 'DISTANCE=25.0 UNIT=NAUT_MILE') = true"; //$NON-NLS-1$
        // using ? for bind value as r marks the criteria as bindEligible 
        // due to literal of type Object appearing in left side of criteria.  
        // The literal Object is a result of the sdo_within_distance function 
        // signature being sdo_within_distance(string, object, string) : string 
        // as the signature was the best match for this query.
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE sdo_within_distance(SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL), SDO_GEOMETRY(2001, 8307, MDSYS.SDO_POINT_TYPE(90.0, -45.0, NULL), NULL, NULL), 'DISTANCE=25.0 UNIT=NAUT_MILE') = ?";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    /**
     * Test a query which uses 
     * <code>sdo_within_distance(Object element, Object element, String literal) 
     * in its criteria into a source specific command.
     * 
     * @throws Exception
     */
    @Test public void test_sdo_within_distance5() throws Exception {
        String input = "SELECT a.INTKEY FROM BQT1.SMALLA A, BQT1.SMALLB B WHERE sdo_within_distance(a.OBJECTVALUE, b.OBJECTVALUE, 'DISTANCE=25.0 UNIT=NAUT_MILE') = true"; //$NON-NLS-1$
        String output = "SELECT A.IntKey FROM SmallA A, SmallB B WHERE sdo_within_distance(A.ObjectValue, B.ObjectValue, 'DISTANCE=25.0 UNIT=NAUT_MILE') = 'true'";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void testLogFunction() throws Exception {
        String input = "SELECT log(CONVERT(stringkey, INTEGER)) FROM bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT ln(trunc(to_number(SmallA.StringKey))) FROM SmallA"; //$NON-NLS-1$
    
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void testLog10Function() throws Exception {
        String input = "SELECT log10(CONVERT(stringkey, INTEGER)) FROM bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT log(10, trunc(to_number(SmallA.StringKey))) FROM SmallA"; //$NON-NLS-1$
    
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void testAliasedFunctions() throws Exception {
        String input = "SELECT char(CONVERT(stringkey, INTEGER)), lcase(stringkey), ucase(stringkey), ifnull(stringkey, 'x') FROM bqt1.smalla"; //$NON-NLS-1$
        String output = "SELECT chr(trunc(to_number(SmallA.StringKey))), lower(SmallA.StringKey), upper(SmallA.StringKey), nvl(SmallA.StringKey, 'x') FROM SmallA"; //$NON-NLS-1$
        
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }    

}
