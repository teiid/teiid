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

import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.teiid.connector.api.ConnectorException;
import org.teiid.connector.jdbc.MetadataFactory;
import org.teiid.connector.jdbc.translator.TranslatedCommand;
import org.teiid.connector.jdbc.translator.Translator;
import org.teiid.connector.language.ICommand;

import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.cdk.unittest.FakeTranslationFactory;

public class TestOracleTranslator {
	
    /**
     * An instance of {@link Translator} which has already been initialized.  
     */
    private static Translator TRANSLATOR; 

    /**
     * Performs setup tasks that should be executed prior to an instance of this
     * class being created.  This method should only be executed once and does 
     * not protect from multiple executions.  It is intended to be executed by 
     * the JUnit4 test framework.
     * <p>
     * This method sets {@link TestOracleTranslator#TRANSLATOR} to an instance 
     * of {@link OracleSQLTranslator} and then calls its {@link OracleSQLTranslator#initialize(ConnectorEnvironment)}
     * method.
     * @throws Exception
     */
    @BeforeClass public static void oneTimeSetup() throws Exception {
        TRANSLATOR = new OracleSQLTranslator();        
        TRANSLATOR.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
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
          
        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
            input, 
            output, TRANSLATOR);        
    }
    
	@Test public void testJoins2() throws Exception {
        String input = "select smalla.intkey from bqt1.smalla cross join (bqt1.smallb cross join bqt1.mediuma)"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA CROSS JOIN (SmallB CROSS JOIN MediumA)"; //$NON-NLS-1$
      
        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
            input, 
            output, TRANSLATOR);        
    }
    
	@Test public void testRewriteConversion1() throws Exception {
        String input = "SELECT char(convert(STRINGNUM, integer) + 100) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT chr((to_number(SmallA.StringNum) + 100)) FROM SmallA";  //$NON-NLS-1$

        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
            input, output, 
            TRANSLATOR);
    }
          
    @Test public void testRewriteConversion2() throws Exception {
        String input = "SELECT convert(STRINGNUM, long) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT to_number(SmallA.StringNum) FROM SmallA";  //$NON-NLS-1$

        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
          
    @Test public void testRewriteConversion3() throws Exception {
        String input = "SELECT convert(convert(STRINGNUM, long), string) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT to_char(to_number(SmallA.StringNum)) FROM SmallA";  //$NON-NLS-1$

        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
          
    @Test public void testRewriteConversion4() throws Exception {
        String input = "SELECT convert(convert(TIMESTAMPVALUE, date), string) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT to_char(trunc(SmallA.TimestampValue), 'YYYY-MM-DD') FROM SmallA";  //$NON-NLS-1$

        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRewriteConversion5() throws Exception {
        String input = "SELECT convert(convert(TIMESTAMPVALUE, time), string) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT to_char(to_date(('1970-01-01 ' || to_char(SmallA.TimestampValue, 'HH24:MI:SS')), 'YYYY-MM-DD HH24:MI:SS'), 'HH24:MI:SS') FROM SmallA";  //$NON-NLS-1$

        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRewriteConversion6() throws Exception {
        String input = "SELECT convert(convert(TIMEVALUE, timestamp), string) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT to_char(cast(SmallA.TimeValue AS timestamp), 'YYYY-MM-DD HH24:MI:SS.FF') FROM SmallA";  //$NON-NLS-1$

        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRewriteConversion8() throws Exception {
        String input = "SELECT nvl(INTNUM, 'otherString') FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT nvl(to_char(SmallA.IntNum), 'otherString') FROM SmallA";  //$NON-NLS-1$

        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRewriteConversion7() throws Exception {
        String input = "SELECT convert(convert(STRINGNUM, integer), string) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT to_char(to_number(SmallA.StringNum)) FROM SmallA";  //$NON-NLS-1$

        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Ignore("TEIID-754: Fix Oracle translator so fromPosition of LOCATE function is used as is")
    @Test public void testRewriteLocate() throws Exception {
        // TODO TEIID-754: Fix Oracle translator so fromPosition of LOCATE function is used as is
        String input = "SELECT locate(INTNUM, 'chimp', 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT instr('chimp', to_char(SmallA.IntNum), 1) FROM SmallA";  //$NON-NLS-1$

        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRewriteLocate2() throws Exception {
        String input = "SELECT locate(STRINGNUM, 'chimp') FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT instr('chimp', SmallA.StringNum) FROM SmallA";  //$NON-NLS-1$

        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Ignore("TEIID-754: Fix Oracle translator so fromPosition of LOCATE function is used as is")
    @Test public void testRewriteLocate3() throws Exception {
        // TODO TEIID-754: Fix Oracle translator so fromPosition of LOCATE function is used as is
        String input = "SELECT locate(INTNUM, '234567890', 1) FROM BQT1.SMALLA WHERE INTKEY = 26"; //$NON-NLS-1$
        String output = "SELECT instr('234567890', to_char(SmallA.IntNum), 1) FROM SmallA WHERE SmallA.IntKey = 26";  //$NON-NLS-1$

        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRewriteLocate4() throws Exception {
        String input = "SELECT locate('c', 'chimp', 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT 1 FROM SmallA";  //$NON-NLS-1$

        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Ignore("TEIID-754: Fix Oracle translator so fromPosition of LOCATE function is 1 if a value of < 1 is given")
    @Test public void testRewriteLocate5() throws Exception {
        // TODO TEIID-754: Fix Oracle translator so fromPosition of LOCATE function is 1 if a value of < 1 is given
        String input = "SELECT locate(STRINGNUM, 'chimp', -5) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT instr('chimp', SmallA.StringNum, 1) FROM SmallA";  //$NON-NLS-1$

        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRewriteSubstring1() throws Exception {
        String input = "SELECT substring(StringNum, 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT substr(SmallA.StringNum, 1) FROM SmallA";  //$NON-NLS-1$

        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRewriteSubstring2() throws Exception {
        String input = "SELECT substring(StringNum, 1, 1) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT substr(SmallA.StringNum, 1, 1) FROM SmallA";  //$NON-NLS-1$

        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRewriteUnionWithOrderBy() throws Exception {
        String input = "SELECT IntKey FROM BQT1.SMALLA UNION SELECT IntKey FROM BQT1.SMALLB ORDER BY IntKey"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA UNION SELECT SmallB.IntKey FROM SmallB ORDER BY IntKey NULLS FIRST";  //$NON-NLS-1$

        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRowLimit1() throws Exception {
        String input = "select intkey from bqt1.smalla limit 10, 0"; //$NON-NLS-1$
        String output = "SELECT * FROM (SELECT VIEW_FOR_LIMIT.*, ROWNUM ROWNUM_ FROM (SELECT SmallA.IntKey FROM SmallA) VIEW_FOR_LIMIT WHERE ROWNUM <= 10) WHERE ROWNUM_ > 10"; //$NON-NLS-1$
               
        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRowLimit2() throws Exception {
        String input = "select intkey from bqt1.smalla limit 0, 10"; //$NON-NLS-1$
        String output = "SELECT * FROM (SELECT SmallA.IntKey FROM SmallA) WHERE ROWNUM <= 10"; //$NON-NLS-1$
               
        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRowLimit3() throws Exception {
        String input = "select intkey from bqt1.smalla limit 1, 10"; //$NON-NLS-1$
        String output = "SELECT * FROM (SELECT VIEW_FOR_LIMIT.*, ROWNUM ROWNUM_ FROM (SELECT SmallA.IntKey FROM SmallA) VIEW_FOR_LIMIT WHERE ROWNUM <= 11) WHERE ROWNUM_ > 1"; //$NON-NLS-1$
               
        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRowLimit4() throws Exception {
        String input = "select intkey from bqt1.mediuma limit 100"; //$NON-NLS-1$
        String output = "SELECT * FROM (SELECT MediumA.IntKey FROM MediumA) WHERE ROWNUM <= 100"; //$NON-NLS-1$
               
        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    @Test public void testRowLimit5() throws Exception {
        String input = "select intkey from bqt1.mediuma limit 50, 100"; //$NON-NLS-1$
        String output = "SELECT * FROM (SELECT VIEW_FOR_LIMIT.*, ROWNUM ROWNUM_ FROM (SELECT MediumA.IntKey FROM MediumA) VIEW_FOR_LIMIT WHERE ROWNUM <= 150) WHERE ROWNUM_ > 50"; //$NON-NLS-1$
               
        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void testConcat2_useLiteral() throws Exception {        
        String input = "select concat2(stringnum,'_xx') from bqt1.Smalla"; //$NON-NLS-1$
        String output = "SELECT concat(nvl(SmallA.StringNum, ''), '_xx') FROM SmallA"; //$NON-NLS-1$
        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

    @Test public void testConcat2() throws Exception {        
        String input = "select concat2(stringnum, stringkey) from bqt1.Smalla"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN (SmallA.StringNum IS NULL) AND (SmallA.StringKey IS NULL) THEN NULL ELSE concat(nvl(SmallA.StringNum, ''), nvl(SmallA.StringKey, '')) END FROM SmallA"; //$NON-NLS-1$
        MetadataFactory.helpTestVisitor(MetadataFactory.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

}
