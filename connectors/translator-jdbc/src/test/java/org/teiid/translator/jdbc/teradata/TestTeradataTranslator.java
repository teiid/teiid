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

import static org.junit.Assert.assertEquals;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.In;
import org.teiid.language.LanguageFactory;
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
    
    @Test public void testByteToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "string", "TRIM(BOTH FROM 1 (FORMAT 'ZZZZ')(CHAR(4)))"); 
    }
    
    @Test public void testDoubleToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.0), Double.class), "string", "TRIM(BOTH FROM 1.0 (FORMAT 'ZZZZZZZZZZZZZZZZZZZZZZZZZ')(CHAR(25)))"); 
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
		TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, null, input, out, new TeradataExecutionFactory());		
	}
	
	@Test public void testPushDownFunction() throws Exception {
		String input = "SELECT teradata.HASHBAKAMP(STRINGKEY) DOUBLENUM FROM bqt1.smalla";
		String out = "SELECT HASHBAKAMP(SmallA.StringKey) AS DOUBLENUM FROM SmallA";
		TranslationHelper.helpTestVisitor(TranslationHelper.BQT_VDB, null, input, out, new TeradataExecutionFactory());		
	}
}
