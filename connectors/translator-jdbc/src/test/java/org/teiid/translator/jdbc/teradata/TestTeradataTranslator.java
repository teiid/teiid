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
package org.teiid.translator.jdbc.teradata;

import static org.junit.Assert.*;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.In;
import org.teiid.language.LanguageFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.TranslatorException;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.SQLConversionVisitor;
import org.teiid.translator.jdbc.TranslationHelper;

@SuppressWarnings("nls")
public class TestTeradataTranslator {

    private static TeradataExecutionFactory TRANSLATOR; 
    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    @BeforeClass
    public static void setUp() throws TranslatorException {
        TRANSLATOR = new TeradataExecutionFactory();
        TRANSLATOR.setUseBindVariables(false);
        TRANSLATOR.start();
    }
    
    public void helpTest(Expression srcExpression, String tgtType, String expectedExpression) throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  
            Arrays.asList( srcExpression,LANG_FACTORY.createLiteral(tgtType, String.class)),TypeFacility.getDataTypeClass(tgtType));
        
        assertEquals("Error converting from " + srcExpression.getType() + " to " + tgtType, 
            expectedExpression, helpGetString(func)); 
    }    
    
    public String helpGetString(Expression expr) throws Exception {
        SQLConversionVisitor sqlVisitor = TRANSLATOR.getSQLConversionVisitor(); 
        sqlVisitor.append(expr);  
        
        return sqlVisitor.toString();        
    }    
    
    @Test public void testSubstring1() throws Exception {
        String input = "SELECT dayofmonth(datevalue) FROM BQT1.SMALLA"; 
        String output = "SELECT extract(DAY from SmallA.DateValue) FROM SmallA"; 

        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);
    }
    
    @Test public void testTimestampToTime() throws Exception {
    	helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(111, 4, 5, 9, 16, 34, 220000000), Timestamp.class), "time", "cast(cast('2011-05-05 09:16:34.22' AS TIMESTAMP(6)) AS TIME)");
    }
    
    @Test public void testIntegerToString() throws Exception {
        String input = "SELECT lcase(bigdecimalvalue) FROM BQT1.SMALLA"; 
        String output = "SELECT LOWER(cast(SmallA.BigDecimalValue AS varchar(100))) FROM SmallA"; 
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);    	
    }    
    
    @Test public void testSubString() throws Exception {
        String input = "SELECT intkey FROM BQT1.SmallA WHERE SUBSTRING(BQT1.SmallA.IntKey, 1) = '1' ORDER BY intkey"; 
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE substr(cast(SmallA.IntKey AS varchar(100)),1) = '1' ORDER BY SmallA.IntKey"; 
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);    	
    }  
    
    @Test public void testSubString2() throws Exception {
        String input = "SELECT intkey FROM BQT1.SmallA WHERE SUBSTRING(BQT1.SmallA.IntKey, 1, 2) = '1' ORDER BY intkey"; 
        String output = "SELECT SmallA.IntKey FROM SmallA WHERE substr(cast(SmallA.IntKey AS varchar(100)),1,2) = '1' ORDER BY SmallA.IntKey"; 
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);    	
    }     
    
    @Test public void testDateToString() throws Exception {
        String input = "SELECT intkey, UPPER(timevalue) AS UPPER FROM BQT1.SmallA ORDER BY intkey"; 
        String output = "SELECT SmallA.IntKey, UPPER(cast(cast(SmallA.TimeValue AS FORMAT 'HH:MI:SS') AS VARCHAR(9))) AS UPPER FROM SmallA ORDER BY SmallA.IntKey"; 
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);    	
    }    
    
    @Test public void testLocate() throws Exception {
        String input = "SELECT INTKEY, BIGDECIMALVALUE FROM BQT1.SmallA WHERE LOCATE('-', BIGDECIMALVALUE) = 1 ORDER BY intkey"; 
        String output = "SELECT SmallA.IntKey, SmallA.BigDecimalValue FROM SmallA WHERE position('-' in cast(SmallA.BigDecimalValue AS varchar(100))) = 1 ORDER BY SmallA.IntKey"; 
        TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, input, output, TRANSLATOR);    	
    }      
    
    
    @Test public void testByteToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "string", "cast(1 AS varchar(4000))"); 
    }
    
    @Test public void testByte2ToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)-1), Byte.class), "string", "cast(-1 AS varchar(4000))"); 
    }
    
    @Test public void testDoubleToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.0), Double.class), "string", "cast(1.0 AS varchar(4000))"); 
    }    
    
    @Test public void testStringToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("1.0", String.class), "double", "cast('1.0' AS double precision)"); 
    }      
    
	@Test public void testInDecompose() throws Exception {
    	Expression left = LANG_FACTORY.createLiteral("1", String.class);
    	List<Expression> right = new ArrayList<Expression>();
    	right.add(LANG_FACTORY.createLiteral("2", String.class));
    	right.add(LANG_FACTORY.createLiteral("3", String.class));
    		
        In expr = LANG_FACTORY.createIn(left,right, false);
        
        assertEquals("'1' IN ('2', '3')", helpGetString(expr));
    }    
	
	@Test public void testSingleInDecompose() throws Exception {
    	Expression left = LANG_FACTORY.createLiteral("1", String.class);
    	List<Expression> right = new ArrayList<Expression>();
    	right.add(LANG_FACTORY.createLiteral("2", String.class));
    		
        In expr = LANG_FACTORY.createIn(left,right, false);
        
        assertEquals("'1' IN ('2')", helpGetString(expr));
    }  
	
	@Test public void testInDecomposeNonLiterals() throws Exception {
    	Expression left = LANG_FACTORY.createLiteral("1", String.class);
    	List<Expression> right = new ArrayList<Expression>();
    	right.add(LANG_FACTORY.createFunction("func", new Expression[] {}, Date.class));
    	right.add(LANG_FACTORY.createLiteral("3", String.class));
    		
        In expr = LANG_FACTORY.createIn(left,right, false);
        
        assertEquals("'1' = func() OR '1' = '3'", helpGetString(expr));
    }
	
	@Test public void testNegatedInDecomposeNonLiterals() throws Exception {
    	Expression left = LANG_FACTORY.createLiteral("1", String.class);
    	List<Expression> right = new ArrayList<Expression>();
    	right.add(LANG_FACTORY.createFunction("func", new Expression[] {}, Date.class));
    	right.add(LANG_FACTORY.createLiteral("3", String.class));
    		
        In expr = LANG_FACTORY.createIn(left,right, true);
        
        assertEquals("'1' <> func() AND '1' <> '3'", helpGetString(expr));
    }
	
	@Test public void testsingleInDecomposeNonLiterals() throws Exception {
    	Expression left = LANG_FACTORY.createLiteral("1", String.class);
    	List<Expression> right = new ArrayList<Expression>();
    	right.add(LANG_FACTORY.createFunction("func", new Expression[] {}, Date.class));
    		
        In expr = LANG_FACTORY.createIn(left,right, false);
        
        assertEquals("'1' = func()", helpGetString(expr));
    }
	
	@Test public void testNullComapreNull() throws Exception {
		String input = "SELECT INTKEY, STRINGKEY, DOUBLENUM FROM bqt1.smalla WHERE NULL <> NULL";
		String out = "SELECT SmallA.IntKey, SmallA.StringKey, SmallA.DoubleNum FROM SmallA WHERE 1 = 0";
		TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, null, input, out, TRANSLATOR);		
	}
	
	@Test public void testPushDownFunction() throws Exception {
		String input = "SELECT teradata.HASHBAKAMP(STRINGKEY) DOUBLENUM FROM bqt1.smalla";
		String out = "SELECT HASHBAKAMP(SmallA.StringKey) AS DOUBLENUM FROM SmallA";
		TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, null, input, out, TRANSLATOR);		
	}
	
	@Test public void testRightFunction() throws Exception {
		String input = "SELECT INTKEY, FLOATNUM FROM BQT1.SmallA WHERE right(FLOATNUM, 2) <> 0 ORDER BY INTKEY";
		String out = "SELECT SmallA.IntKey, SmallA.FloatNum FROM SmallA WHERE substr(cast(SmallA.FloatNum AS varchar(100)),(character_length(cast(SmallA.FloatNum AS varchar(100)))-2+1)) <> '0' ORDER BY SmallA.IntKey";
		TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, null, input, out, TRANSLATOR);		
	}
	
	@Test public void testLocateFunction() throws Exception {
		String input = "SELECT INTKEY, STRINGKEY, SHORTVALUE FROM BQT1.SmallA WHERE (LOCATE(0, STRINGKEY) = 2) OR (LOCATE(2, SHORTVALUE, 4) = 6) ORDER BY intkey";
		String out = "SELECT SmallA.IntKey, SmallA.StringKey, SmallA.ShortValue FROM SmallA WHERE position('0' in SmallA.StringKey) = 2 OR position('2' in substr(cast(SmallA.ShortValue AS varchar(100)),4)) = 6 ORDER BY SmallA.IntKey";
		TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, null, input, out, TRANSLATOR);		
	}	
}
