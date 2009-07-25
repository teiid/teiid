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

package org.teiid.connector.jdbc.sybase;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Properties;

import org.teiid.connector.jdbc.sybase.SybaseConvertModifier;
import org.teiid.connector.jdbc.sybase.SybaseSQLTranslator;
import org.teiid.connector.jdbc.translator.SQLConversionVisitor;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;
import org.teiid.connector.language.ILiteral;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.common.types.DataTypeManager;
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
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
        
        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor(); 
        sqlVisitor.append(expr);  
        
        return sqlVisitor.toString();        
    }

    private void helpGetString1(IFunction func, String expectedStr) throws Exception {
        SybaseConvertModifier mod = new SybaseConvertModifier(LANG_FACTORY);
        IExpression expr = mod.modify(func);
        
        assertEquals(expectedStr, helpGetString(expr)); 
    }
    
    public void helpTest(IExpression srcExpression, String tgtType, String expectedExpression) throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                srcExpression,
                LANG_FACTORY.createLiteral(tgtType, String.class)},
            DataTypeManager.getDataTypeClass(tgtType));
        
        SybaseConvertModifier mod = new SybaseConvertModifier(LANG_FACTORY);
        IExpression expr = mod.modify(func);
        
        assertEquals("Error converting from " + DataTypeManager.getDataTypeName(srcExpression.getType()) + " to " + tgtType, //$NON-NLS-1$ //$NON-NLS-2$ 
            expectedExpression, helpGetString(expr)); 
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
        
        helpGetString1(func,  "convert(datetime, convert(varchar, {ts'1989-03-03 07:08:12.000099999'}, 109))");  //$NON-NLS-1$
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
        
        helpGetString1(func,  "convert(datetime, {ts'1900-01-01 12:02:03'})");  //$NON-NLS-1$
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
        
        helpGetString1(func,  "convert(varchar, {ts'1900-01-01 03:10:01'}, 108)");  //$NON-NLS-1$
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

    // Source = CHAR
    
    public void testCharToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Character('5'), Character.class), "string", "convert(varchar, '5')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BOOLEAN
    
    public void testBooleanToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "string", "convert(varchar, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "byte", "convert(tinyint, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "short", "convert(smallint, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "integer", "convert(int, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "long", "convert(numeric, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "biginteger", "convert(numeric, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "float", "convert(real, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "double", "convert(float, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "bigdecimal", "convert(float, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    // Source = BYTE
    
    public void testByteToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "string", "convert(varchar, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "long", "convert(numeric, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "biginteger", "convert(numeric, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "float", "convert(real, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "double", "convert(float, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "bigdecimal", "convert(float, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = SHORT
    
    public void testShortToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "string", "convert(varchar, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "long", "convert(numeric, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "biginteger", "convert(numeric, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "float", "convert(real, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "double", "convert(float, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "bigdecimal", "convert(float, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = INTEGER
    
    public void testIntegerToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "string", "convert(varchar, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "long", "convert(numeric, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "biginteger", "convert(numeric, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "float", "convert(real, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "double", "convert(float, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "bigdecimal", "convert(float, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = LONG
    
    public void testLongToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "string", "convert(varchar, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "biginteger", "convert(numeric, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "float", "convert(real, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "double", "convert(float, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "bigdecimal", "convert(float, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BIGINTEGER
    
    public void testBigIntegerToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "string", "convert(varchar, 1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "long", "convert(numeric, 1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "float", "convert(real, 1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "double", "convert(float, 1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "bigdecimal", "convert(float, 1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // Source = FLOAT
    
    public void testFloatToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "string", "convert(varchar, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "long", "convert(numeric, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "biginteger", "convert(numeric, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "double", "convert(float, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "bigdecimal", "convert(float, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = DOUBLE
    
    public void testDoubleToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "string", "convert(varchar, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "long", "convert(numeric, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "biginteger", "convert(numeric, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "float", "convert(real, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "bigdecimal", "convert(float, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBigDecimalToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "long", "convert(numeric, 1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "biginteger", "convert(numeric, 1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "float", "convert(real, 1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToDoublel() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "double", "convert(float, 1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

        
}
