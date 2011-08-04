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

package org.teiid.translator.jdbc.db2;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.teiid.cdk.api.TranslationUtility;
import org.teiid.cdk.unittest.FakeTranslationFactory;
import org.teiid.core.util.UnitTestUtil;
import org.teiid.language.Command;
import org.teiid.translator.ExecutionContext;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.jdbc.TranslatedCommand;
import org.teiid.translator.jdbc.TranslationHelper;


/**
 */
public class TestDB2SqlTranslator {

    private static DB2ExecutionFactory TRANSLATOR; 

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new DB2ExecutionFactory();
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.start();
    }
    
    public String getTestVDB() {
        return UnitTestUtil.getTestDataPath() + "/PartsSupplier.vdb"; //$NON-NLS-1$
    }
    
    public void helpTestVisitor(TranslationUtility util, String input, String expectedOutput) throws TranslatorException {
        // Convert from sql to objects
        Command obj = util.parseCommand(input);
        
        TranslatedCommand tc = new TranslatedCommand(Mockito.mock(ExecutionContext.class), TRANSLATOR);
        tc.translateCommand(obj);
        
        assertEquals("Did not get correct sql", expectedOutput, tc.getSql());             //$NON-NLS-1$
    }

    @Test
    public void testRowLimit() throws Exception {
        String input = "select intkey from bqt1.smalla limit 100"; //$NON-NLS-1$
        String output = "SELECT SmallA.IntKey FROM SmallA FETCH FIRST 100 ROWS ONLY";  //$NON-NLS-1$

        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
            input, 
            output);
    }
    
    @Test
    public void testCrossJoin() throws Exception{
        String input = "SELECT bqt1.smalla.stringkey FROM bqt1.smalla cross join bqt1.smallb"; //$NON-NLS-1$
        String output = "SELECT SmallA.StringKey FROM SmallA INNER JOIN SmallB ON 1 = 1";  //$NON-NLS-1$

        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
            input, 
            output);
    }
    
    @Test
    public void testConcat2_useLiteral() throws Exception {
        String input = "select concat2(stringnum,'_xx') from BQT1.Smalla"; //$NON-NLS-1$
        String output = "SELECT concat(coalesce(SmallA.StringNum, ''), '_xx') FROM SmallA";  //$NON-NLS-1$
        
        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
                input, 
                output);

    }

    @Test
    public void testConcat2() throws Exception {
        String input = "select concat2(stringnum, stringnum) from BQT1.Smalla"; //$NON-NLS-1$       
        String output = "SELECT CASE WHEN SmallA.StringNum IS NULL THEN NULL ELSE concat(coalesce(SmallA.StringNum, ''), coalesce(SmallA.StringNum, '')) END FROM SmallA";  //$NON-NLS-1$
        
        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
                input, 
                output);
    }    
    
    @Test
    public void testSelectNullLiteral() throws Exception {
        String input = "select null + 1 as x, null || 'a', char(null) from BQT1.Smalla"; //$NON-NLS-1$       
        String output = "SELECT cast(NULL AS integer) AS x, cast(NULL AS char), cast(NULL AS char(1)) FROM SmallA";  //$NON-NLS-1$
        
        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
                input, 
                output);
    }    
    
    @Test
    public void testSelectNullLiteral1() throws Exception {
        String input = "select x, intkey from (select null as x, intkey from BQT1.Smalla) y "; //$NON-NLS-1$       
        String output = "SELECT y.x, y.intkey FROM (SELECT cast(NULL AS integer) AS x, SmallA.IntKey FROM SmallA) AS y";  //$NON-NLS-1$
        
        helpTestVisitor(FakeTranslationFactory.getInstance().getBQTTranslationUtility(),
                input, 
                output);
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
        String output = "SELECT LOCATE(char(SmallA.IntNum), 'chimp', 1) FROM SmallA";  //$NON-NLS-1$

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
        String output = "SELECT LOCATE(SmallA.StringNum, 'chimp') FROM SmallA";  //$NON-NLS-1$

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
        String output = "SELECT LOCATE(char(SmallA.IntNum), '234567890', 1) FROM SmallA WHERE SmallA.IntKey = 26";  //$NON-NLS-1$

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
        String output = "SELECT LOCATE(SmallA.StringNum, 'chimp', 1) FROM SmallA";  //$NON-NLS-1$

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
        String output = "SELECT LOCATE(SmallA.StringNum, 'chimp', CASE WHEN SmallA.IntNum < 1 THEN 1 ELSE SmallA.IntNum END) FROM SmallA";  //$NON-NLS-1$

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
        String output = "SELECT LOCATE(SmallA.StringNum, 'chimp', CASE WHEN (LOCATE(SmallA.StringNum, 'chimp') + 1) < 1 THEN 1 ELSE (LOCATE(SmallA.StringNum, 'chimp') + 1) END) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void testBooleanToString() throws Exception {
    	String input = "SELECT convert(convert(INTKEY, boolean), string) FROM BQT1.SmallA"; //$NON-NLS-1$
        String output = "SELECT CASE WHEN SmallA.IntKey = 0 THEN 'false' WHEN SmallA.IntKey IS NOT NULL THEN 'true' END FROM SmallA"; //$NON-NLS-1$
          
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
            input, 
            output, TRANSLATOR);
    }
    
    @Test public void testSubstring() throws Exception {
        String input = "SELECT substring(STRINGNUM, 2, 10) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT substr(SmallA.StringNum, 2, CASE WHEN 10 > (length(SmallA.StringNum) - (2 - 1)) THEN (length(SmallA.StringNum) - (2 - 1)) ELSE 10 END) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void testSubstring1() throws Exception {
        String input = "SELECT substring(STRINGNUM, 2, intnum) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT substr(SmallA.StringNum, 2, CASE WHEN SmallA.IntNum > (length(SmallA.StringNum) - (2 - 1)) THEN (length(SmallA.StringNum) - (2 - 1)) WHEN SmallA.IntNum > 0 THEN SmallA.IntNum END) FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }
    
    @Test public void testTrim() throws Exception {
        String input = "SELECT trim(leading 'x' from stringnum) FROM BQT1.SMALLA"; //$NON-NLS-1$
        String output = "SELECT STRIP(SmallA.StringNum, leading, 'x') FROM SmallA";  //$NON-NLS-1$

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB,
                input, output, 
                TRANSLATOR);
    }

}
