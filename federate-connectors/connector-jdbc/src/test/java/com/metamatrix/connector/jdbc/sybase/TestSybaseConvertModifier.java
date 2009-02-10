/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.connector.jdbc.sybase;

import java.sql.Timestamp;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.connector.jdbc.util.FunctionReplacementVisitor;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.ILanguageFactory;
import com.metamatrix.connector.language.ILiteral;
import com.metamatrix.connector.language.ISelectSymbol;
import com.metamatrix.query.unittest.TimestampUtil;

/**
 */
public class TestSybaseConvertModifier extends TestCase {

    private static final ILanguageFactory LANG_FACTORY = CommandBuilder.getLanuageFactory();

    /**
     * Constructor for TestSybaseConvertModifier.
     * @param name
     */
    public TestSybaseConvertModifier(String name) {
        super(name);
    }

    public String helpGetString(IExpression expr) throws Exception {
        SybaseSQLTranslator trans = new SybaseSQLTranslator();
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false), null);
        
        SybaseSQLConversionVisitor sqlVisitor = new SybaseSQLConversionVisitor(); 
        sqlVisitor.setFunctionModifiers(trans.getFunctionModifiers());
        sqlVisitor.setLanguageFactory(LANG_FACTORY);  
        sqlVisitor.append(expr);  
        
        return sqlVisitor.toString();        
    }

    private void helpGetString1(IFunction func, String expectedStr) throws Exception {
        SybaseConvertModifier mod = new SybaseConvertModifier(LANG_FACTORY);
        IExpression expr = mod.modify(func);
        
        SybaseSQLTranslator trans = new SybaseSQLTranslator();
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false), null);
        
        SybaseSQLConversionVisitor sqlVisitor = new SybaseSQLConversionVisitor(); 
        sqlVisitor.setFunctionModifiers(trans.getFunctionModifiers());
        sqlVisitor.setLanguageFactory(LANG_FACTORY);  
        sqlVisitor.append(expr);
          
        assertEquals(expectedStr, sqlVisitor.toString()); 
    }
    
    // original test -- this is not a drop one anymore
    public void testModDrop() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral("5", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("integer", String.class)     //$NON-NLS-1$
            },
            Integer.class);
        
        SybaseConvertModifier mod = new SybaseConvertModifier(LANG_FACTORY);
        IExpression expr = mod.modify(func);
        assertEquals("convert(int, '5')", helpGetString(expr)); //$NON-NLS-1$
    }

    /********************Beginning of convert(INPUT, date) ******************/
    public void testStringToDate() throws Exception {
        String dateStr = "2003-12-31"; //$NON-NLS-1$
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(dateStr, String.class), 
                LANG_FACTORY.createLiteral("date", String.class)}, //$NON-NLS-1$
            java.sql.Date.class);
        
        SybaseConvertModifier mod = new SybaseConvertModifier(LANG_FACTORY);
        IExpression expr = mod.modify(func);
        
        assertEquals("convert(datetime, '2003-12-31')", helpGetString(expr)); //$NON-NLS-1$
    }
    
    public void testTimestampToDate() throws Exception {
        ILiteral c = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(89, 2, 3, 7, 8, 12, 99999), Timestamp.class); 
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                c, 
                LANG_FACTORY.createLiteral("date", String.class)}, //$NON-NLS-1$
            java.sql.Date.class);
        
        helpGetString1(func,  "convert(datetime, convert(varchar, {ts'1989-03-03 07:08:12.000099999'}, 1))");  //$NON-NLS-1$
    }
    
    public void testConvertFormat() throws Exception {
        
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);
        IFunction func1 = LANG_FACTORY.createFunction("formattimestamp",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(ts, Timestamp.class), 
                LANG_FACTORY.createLiteral("MM/dd/yyyy", String.class)}, //$NON-NLS-1$
            String.class);

        IFunction func2 = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                func1, 
                LANG_FACTORY.createLiteral("date", String.class)}, //$NON-NLS-1$
            java.sql.Date.class);
        
        SybaseSQLTranslator trans = new SybaseSQLTranslator();
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false), null);

        // Perform function replacement, per standard function modifiers
        FunctionReplacementVisitor functionVisitor = new FunctionReplacementVisitor(trans.getFunctionModifiers());
        ISelectSymbol holder = LANG_FACTORY.createSelectSymbol("holder", func2); //$NON-NLS-1$
        holder.acceptVisitor(functionVisitor);
        IExpression expr = holder.getExpression();
        
        assertEquals("convert(datetime, convert(varchar, {ts'2003-11-01 12:05:02.0'}, 1))", helpGetString(expr)); //$NON-NLS-1$
    }
    /********************END of convert(INPUT, date) ******************/
    /********************Beginning of convert(INPUT, time) ******************/
    public void testStringToTime() throws Exception {
        String timeStr = "12:08:07"; //$NON-NLS-1$
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(timeStr, String.class), 
                LANG_FACTORY.createLiteral("time", String.class)}, //$NON-NLS-1$
            java.sql.Time.class);
        
        helpGetString1(func,  "convert(datetime, '12:08:07')");  //$NON-NLS-1$
    }
    
    public void testTimestampToTime() throws Exception {
        ILiteral c = LANG_FACTORY.createLiteral(TimestampUtil.createTimestamp(89, 2, 3, 7, 8, 12, 99999), Timestamp.class); 
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                c, 
                LANG_FACTORY.createLiteral("time", String.class)}, //$NON-NLS-1$
            java.sql.Time.class);
        
        helpGetString1(func,  "convert(datetime, convert(varchar, {ts'1989-03-03 07:08:12.000099999'}, 108))");  //$NON-NLS-1$
    }
    /********************END of convert(INPUT, time) ******************/
    
    /********************Beginning of convert(INPUT, timestamp) ******************/
    public void testStringToTimestamp() throws Exception {
        String timestampStr = "1989-07-09 12:08:07"; //$NON-NLS-1$
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(timestampStr, String.class), 
                LANG_FACTORY.createLiteral("timestamp", String.class)}, //$NON-NLS-1$
            java.sql.Timestamp.class);
        
        helpGetString1(func,  "convert(datetime, '1989-07-09 12:08:07')");  //$NON-NLS-1$
    }
    
    public void testTimeToTimestamp() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(TimestampUtil.createTime(12, 2, 3), java.sql.Time.class), 
                LANG_FACTORY.createLiteral("timestamp", String.class)}, //$NON-NLS-1$
            java.sql.Timestamp.class);
        
        helpGetString1(func,  "convert(datetime, {t'12:02:03'})");  //$NON-NLS-1$
    }
        
    public void testDateToTimestamp() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(TimestampUtil.createDate(89, 2, 3), java.sql.Date.class), 
                LANG_FACTORY.createLiteral("timestamp", String.class)}, //$NON-NLS-1$
            java.sql.Timestamp.class);
        
        helpGetString1(func,  "convert(datetime, {d'1989-03-03'})");  //$NON-NLS-1$
    }
    /********************END of convert(INPUT, timestamp) ******************/

    /*****************Beginning of convert(input, string)******************/
    public void testBooleanToStringa() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class),
                LANG_FACTORY.createLiteral("string", String.class)}, //$NON-NLS-1$
            String.class);

        helpGetString1(func,  "convert(varchar, 1)");  //$NON-NLS-1$
    }
    
    public void testBooleanToStringb() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class),
                LANG_FACTORY.createLiteral("string", String.class)}, //$NON-NLS-1$
            String.class);

        helpGetString1(func,  "convert(varchar, 0)");  //$NON-NLS-1$
    }   
         
    public void testTimestampToString() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(ts, Timestamp.class),
                LANG_FACTORY.createLiteral("string", String.class)}, //$NON-NLS-1$
            String.class);

        helpGetString1(func,  "convert(varchar, {ts'2003-11-01 12:05:02.0'}, 109)");  //$NON-NLS-1$
    }
    
    public void testDateToString() throws Exception {
        java.sql.Date d = TimestampUtil.createDate(103, 10, 1);
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(d, java.sql.Date.class),
                LANG_FACTORY.createLiteral("string", String.class)}, //$NON-NLS-1$
            String.class);
        
        helpGetString1(func,  "convert(varchar, {d'2003-11-01'}, 101)");  //$NON-NLS-1$
    } 
    
    public void testTimeToString() throws Exception {
        java.sql.Time t = TimestampUtil.createTime(3, 10, 1);
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(t, java.sql.Time.class),
                LANG_FACTORY.createLiteral("string", String.class)}, //$NON-NLS-1$
            String.class);
        
        helpGetString1(func,  "convert(varchar, {t'03:10:01'}, 108)");  //$NON-NLS-1$
    }    
    
    public void testBigDecimalToString() throws Exception {
        java.math.BigDecimal m = new java.math.BigDecimal("-123124534.3"); //$NON-NLS-1$
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(m, java.math.BigDecimal.class),
                LANG_FACTORY.createLiteral("string", String.class)}, //$NON-NLS-1$
            String.class);
        
        helpGetString1(func,  "convert(varchar, -123124534.3)");  //$NON-NLS-1$
    }
    /***************** End of convert(input, string)******************/
    
    /***************** Beginning of convert(input, char) ************/
    public void testStringToChar() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral("12", String.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("char", Character.class)}, //$NON-NLS-1$
        Character.class);
        
        helpGetString1(func,  "convert(char, '12')");  //$NON-NLS-1$
    }     
    /***************** End of convert(input, char)******************/
     
    /***************** Beginning of convert(input, boolean) ************/
    public void testStringToBoolean() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral("true", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);
        
        helpGetString1(func,  "convert(bit, 'true')");  //$NON-NLS-1$
    } 
    
    public void testByteToBoolean() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class),  
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);
        
        helpGetString1(func,  "convert(bit, 1)");  //$NON-NLS-1$
    } 
    
    public void testShortToBoolean() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Short((short) 0), Short.class),  
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);
        
        helpGetString1(func,  "convert(bit, 0)");  //$NON-NLS-1$
    } 
    
    public void testIntegerToBoolean() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Integer(1), Integer.class),  
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);
        
        helpGetString1(func,  "convert(bit, 1)");  //$NON-NLS-1$
    } 
    
    public void testLongToBoolean() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Long(1), Long.class),  
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);
        
        helpGetString1(func,  "convert(bit, 1)");  //$NON-NLS-1$
    } 
    
    public void testBigIntegerToBoolean() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new java.math.BigInteger("1"), java.math.BigInteger.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);
        
        helpGetString1(func,  "convert(bit, 1)");  //$NON-NLS-1$
    } 
    
    public void testFloatToBoolean() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Float((float)1.0), Float.class),  
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);
        
        helpGetString1(func,  "convert(bit, 1.0)");  //$NON-NLS-1$
    } 
    
    public void testDoubleToBoolean() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Double(1.0), Double.class),  
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);
        
        helpGetString1(func,  "convert(bit, 1.0)");  //$NON-NLS-1$
    } 
    
    public void testBigDecimalToBoolean() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new java.math.BigDecimal("1.0"), java.math.BigDecimal.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("boolean", Boolean.class)}, //$NON-NLS-1$
            Boolean.class);
        
        helpGetString1(func,  "convert(bit, 1.0)");  //$NON-NLS-1$
    } 
    
    /***************** End of convert(input, boolean)******************/
    
    
    /***************** Beginning of convert(input, byte) ************/
    public void testStringToByte() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral("12", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("byte", Byte.class)}, //$NON-NLS-1$
            Byte.class);
        
        helpGetString1(func,  "convert(tinyint, '12')");  //$NON-NLS-1$
    } 
    
    public void testBooleanToBytea() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class),
                LANG_FACTORY.createLiteral("byte", Byte.class)}, //$NON-NLS-1$
            Byte.class);
        
        helpGetString1(func,  "convert(tinyint, 1)");  //$NON-NLS-1$
    }  
    
    public void testBooleanToByteb() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class),
                LANG_FACTORY.createLiteral("byte",  Byte.class)}, //$NON-NLS-1$
            Byte.class);
        
        helpGetString1(func,  "convert(tinyint, 0)");  //$NON-NLS-1$
    } 
    
    public void testShortToByte() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Short((short) 123), Short.class),
                LANG_FACTORY.createLiteral("byte",  Byte.class)}, //$NON-NLS-1$
            Byte.class);
        
        helpGetString1(func,  "convert(tinyint, 123)");  //$NON-NLS-1$
    } 

    public void testIntegerToByte() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Integer(1232321), Integer.class),
                LANG_FACTORY.createLiteral("byte",  Byte.class)}, //$NON-NLS-1$
            Byte.class);
        
        helpGetString1(func,  "convert(tinyint, 1232321)");  //$NON-NLS-1$
    } 
    
    public void testLongToByte() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Long(1231232341), Long.class),
                LANG_FACTORY.createLiteral("byte",  Byte.class)}, //$NON-NLS-1$
            Byte.class);
        
        helpGetString1(func,  "convert(tinyint, 1231232341)");  //$NON-NLS-1$
    } 
    
    public void testBigIntegerToByte() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new java.math.BigInteger("123"), java.math.BigInteger.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("byte",  Byte.class)}, //$NON-NLS-1$
            Byte.class);
        
        helpGetString1(func,  "convert(tinyint, 123)");  //$NON-NLS-1$
    } 
    
    public void testFloatToByte() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Float((float) 123.0), Float.class),
                LANG_FACTORY.createLiteral("byte",  Byte.class)}, //$NON-NLS-1$
            Byte.class);
        
        helpGetString1(func,  "convert(tinyint, 123.0)");  //$NON-NLS-1$
    } 
    
    public void testDoubleToByte() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Double(1.0), Double.class),
                LANG_FACTORY.createLiteral("byte",  Byte.class)}, //$NON-NLS-1$
            Byte.class);
        
        helpGetString1(func,  "convert(tinyint, 1.0)");  //$NON-NLS-1$
    } 
    
    public void testBigDecimalToByte() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new java.math.BigDecimal("12.3"), java.math.BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("byte",  Byte.class)}, //$NON-NLS-1$
            Byte.class);
        
        helpGetString1(func,  "convert(tinyint, 12.3)");  //$NON-NLS-1$
    } 

    /***************** End of convert(input, byte)******************/

    /*****************Beginning of convert(input, short)************/
    public void testStringToShort() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral("123", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("short", Short.class)}, //$NON-NLS-1$
            Short.class);
        
        helpGetString1(func,  "convert(smallint, '123')");  //$NON-NLS-1$
    }    
    
    public void testBooleanToShorta() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class),
                LANG_FACTORY.createLiteral("short", Short.class)}, //$NON-NLS-1$
            Short.class);
        
        helpGetString1(func,  "convert(smallint, 1)");  //$NON-NLS-1$
    }  
    
    public void testBooleanToShortb() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class),
                LANG_FACTORY.createLiteral("short", Short.class)}, //$NON-NLS-1$
            Short.class);
        
        helpGetString1(func,  "convert(smallint, 0)");  //$NON-NLS-1$
    } 
      
    public void testByteToShort() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Byte((byte) 12), Byte.class),
                LANG_FACTORY.createLiteral("short",  Short.class)}, //$NON-NLS-1$
            Short.class);
        
        helpGetString1(func,  "convert(smallint, 12)");  //$NON-NLS-1$
    } 

    public void testIntegerToShort() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Integer(1232321), Integer.class),
                LANG_FACTORY.createLiteral("short",  Short.class)}, //$NON-NLS-1$
            Short.class);
        
        helpGetString1(func,  "convert(smallint, 1232321)");  //$NON-NLS-1$
    } 
    
    public void testLongToShort() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Long(1231232341), Long.class),
                LANG_FACTORY.createLiteral("short",  Short.class)}, //$NON-NLS-1$
            Short.class);
        
        helpGetString1(func,  "convert(smallint, 1231232341)");  //$NON-NLS-1$
    } 
    
    public void testBigIntegerToShort() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new java.math.BigInteger("123"), java.math.BigInteger.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("short",  Short.class)}, //$NON-NLS-1$
            Short.class);
        
        helpGetString1(func,  "convert(smallint, 123)");  //$NON-NLS-1$
    } 
    
    public void testFloatToShort() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Float((float) 123.0), Float.class),
                LANG_FACTORY.createLiteral("short",  Short.class)}, //$NON-NLS-1$
            Short.class);
        
        helpGetString1(func,  "convert(smallint, 123.0)");  //$NON-NLS-1$
    } 
    
    public void testDoubleToShort() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Double(1.0), Double.class),
                LANG_FACTORY.createLiteral("short",  Short.class)}, //$NON-NLS-1$
            Short.class);
        
        helpGetString1(func,  "convert(smallint, 1.0)");  //$NON-NLS-1$
    } 
    
    public void testBigDecimalToShort() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new java.math.BigDecimal("12.3"), java.math.BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("short",  Short.class)}, //$NON-NLS-1$
            Short.class);
        
        helpGetString1(func,  "convert(smallint, 12.3)");  //$NON-NLS-1$
    }       
    /***************** End of convert(input, short)******************/
    
    /***************** Beginning of convert(input, integer) ************/
    public void testStringToInteger() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral("12332", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("integer", Integer.class)}, //$NON-NLS-1$
            Integer.class);
        
        helpGetString1(func,  "convert(int, '12332')");  //$NON-NLS-1$
    } 
    
    public void testBooleanToIntegera() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class),
                LANG_FACTORY.createLiteral("integer", Integer.class)}, //$NON-NLS-1$
            Integer.class);
        
        helpGetString1(func,  "convert(int, 1)");  //$NON-NLS-1$
    }  
    
    public void testBooleanToIntegerb() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class),
                LANG_FACTORY.createLiteral("integer", Integer.class)}, //$NON-NLS-1$
            Integer.class);
        
        helpGetString1(func,  "convert(int, 0)");  //$NON-NLS-1$
    } 
    
    public void testByteToInteger() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Byte((byte)12), Byte.class),
                LANG_FACTORY.createLiteral("integer",  Integer.class)}, //$NON-NLS-1$
            Integer.class);
        
        helpGetString1(func,  "convert(int, 12)");  //$NON-NLS-1$
    } 
    
    public void testShortToInteger() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Short((short)1243 ), Short.class),
                LANG_FACTORY.createLiteral("integer",  Integer.class)}, //$NON-NLS-1$
            Integer.class);
        
        helpGetString1(func,  "convert(int, 1243)");  //$NON-NLS-1$
    } 
    
    public void testLongToInteger() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Long(1231232341), Long.class),
                LANG_FACTORY.createLiteral("integer",  Integer.class)}, //$NON-NLS-1$
            Integer.class);
        
        helpGetString1(func,  "convert(int, 1231232341)");  //$NON-NLS-1$
    } 
    
    public void testBigIntegerToInteger() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new java.math.BigInteger("123"), java.math.BigInteger.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("integer",  Integer.class)}, //$NON-NLS-1$
            Integer.class);
        
        helpGetString1(func,  "convert(int, 123)");  //$NON-NLS-1$
    } 
    
    public void testFloatToInteger() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Float((float) 123.0), Float.class),
                LANG_FACTORY.createLiteral("integer",  Integer.class)}, //$NON-NLS-1$
            Integer.class);
        
        helpGetString1(func,  "convert(int, 123.0)");  //$NON-NLS-1$
    } 
    
    public void testDoubleToInteger() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new Double(1.0), Double.class),
                LANG_FACTORY.createLiteral("integer",  Integer.class)}, //$NON-NLS-1$
            Integer.class);
        
        helpGetString1(func,  "convert(int, 1.0)");  //$NON-NLS-1$
    } 
    
    public void testBigDecimalToInteger() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral(new java.math.BigDecimal("12.3"), java.math.BigDecimal.class), //$NON-NLS-1$
                LANG_FACTORY.createLiteral("integer",  Integer.class)}, //$NON-NLS-1$
            Integer.class);
        
        helpGetString1(func,  "convert(int, 12.3)");  //$NON-NLS-1$
    }       
   
    /***************** End of convert(input, integer)******************/
    
    /***************** Beginning of convert(input, long) ************/
    public void testStringToLong() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral("12332131413", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("long", Long.class)}, //$NON-NLS-1$
            Long.class);
        
        helpGetString1(func,  "convert(numeric, '12332131413')");  //$NON-NLS-1$
    } 
    
    public void testBooleanToLonga() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new IExpression[] { LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), LANG_FACTORY.createLiteral("long", Long.class)}, //$NON-NLS-1$
            Integer.class);

        helpGetString1(func, "convert(numeric, 1)"); //$NON-NLS-1$
    }

    public void testBooleanToLongb() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new IExpression[] { LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), LANG_FACTORY.createLiteral("long", Long.class)}, //$NON-NLS-1$
            Long.class);

        helpGetString1(func, "convert(numeric, 0)"); //$NON-NLS-1$
    }
   
    /***************** End of convert(input, long)******************/
    
    /***************** Beginning of convert(input, biginteger) ************/
    public void testStringToBigInteger() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral("12323143241414", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("biginteger", java.math.BigInteger.class)}, //$NON-NLS-1$
            java.math.BigInteger.class);
        
        helpGetString1(func,  "convert(numeric, '12323143241414')");  //$NON-NLS-1$
    } 
    
    public void testBooleanToBigIntegera() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new IExpression[] { LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), LANG_FACTORY.createLiteral("biginteger", java.math.BigInteger.class)}, //$NON-NLS-1$
            Integer.class);

        helpGetString1(func, "convert(numeric, 1)"); //$NON-NLS-1$
    }

    public void testBooleanToBigIntegerb() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new IExpression[] { LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), LANG_FACTORY.createLiteral("biginteger", java.math.BigInteger.class)}, //$NON-NLS-1$
            java.math.BigInteger.class);

        helpGetString1(func, "convert(numeric, 0)"); //$NON-NLS-1$
    }
    
    /***************** End of convert(input, biginteger)******************/
    
    /***************** Beginning of convert(input, float) ************/
    public void testStringToFloat() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral("123", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("float", Float.class)}, //$NON-NLS-1$
            Float.class);
        
        helpGetString1(func,  "convert(real, '123')");  //$NON-NLS-1$
    } 
    
    public void testBooleanToFloata() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new IExpression[] { LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), 
                LANG_FACTORY.createLiteral("float", Float.class)}, //$NON-NLS-1$
            Float.class);

        helpGetString1(func, "convert(real, 1)"); //$NON-NLS-1$
    }

    public void testBooleanToFloatb() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new IExpression[] { LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), 
                LANG_FACTORY.createLiteral("float", Float.class)}, //$NON-NLS-1$
            Float.class);

        helpGetString1(func, "convert(real, 0)"); //$NON-NLS-1$
    }
    
    /***************** End of convert(input, float)******************/
    
    /***************** Beginning of convert(input, double) ************/
    public void testStringToDouble() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral("123", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("double", Double.class)}, //$NON-NLS-1$
            Double.class);
        
        helpGetString1(func,  "convert(float, '123')");  //$NON-NLS-1$
    } 
    
    public void testBooleanToDoublea() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new IExpression[] { LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), 
                LANG_FACTORY.createLiteral("double", Double.class)}, //$NON-NLS-1$
            Double.class);

        helpGetString1(func, "convert(float, 1)"); //$NON-NLS-1$
    }

    public void testBooleanToDoubleb() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new IExpression[] { LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), 
                LANG_FACTORY.createLiteral("double", Double.class)}, //$NON-NLS-1$
            Double.class);

        helpGetString1(func, "convert(float, 0)"); //$NON-NLS-1$
    }
    
    /***************** End of convert(input, double)******************/
    
    /***************** Beginning of convert(input, bigdecimal) ************/
    public void testStringToBigDecimal() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                LANG_FACTORY.createLiteral("123", String.class),  //$NON-NLS-1$
                LANG_FACTORY.createLiteral("bigdecimal", java.math.BigDecimal.class)}, //$NON-NLS-1$
            java.math.BigDecimal.class);
        
        helpGetString1(func,  "convert(float, '123')");  //$NON-NLS-1$
    } 

    public void testBooleanToBigDecimala() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new IExpression[] { LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), 
                LANG_FACTORY.createLiteral("bigdecimal", java.math.BigDecimal.class)}, //$NON-NLS-1$
            java.math.BigDecimal.class);

        helpGetString1(func, "convert(float, 1)"); //$NON-NLS-1$
    }

    public void testBooleanToBigDecimalb() throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert", //$NON-NLS-1$
            new IExpression[] { LANG_FACTORY.createLiteral(Boolean.FALSE, Boolean.class), 
                LANG_FACTORY.createLiteral("bigdecimal", java.math.BigDecimal.class)}, //$NON-NLS-1$
            java.math.BigDecimal.class);

        helpGetString1(func, "convert(float, 0)"); //$NON-NLS-1$
    }
        
}
