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

package com.metamatrix.connector.jdbc.sqlserver;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Properties;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.connector.jdbc.extension.SQLConversionVisitor;
import com.metamatrix.connector.language.IExpression;
import com.metamatrix.connector.language.IFunction;
import com.metamatrix.connector.language.ILanguageFactory;
import com.metamatrix.query.unittest.TimestampUtil;

/**
 */
public class TestSqlServerConvertModifier extends TestCase {

    private static final ILanguageFactory LANG_FACTORY = CommandBuilder.getLanuageFactory();

    /**
     * Constructor for TestSybaseConvertModifier.
     * @param name
     */
    public TestSqlServerConvertModifier(String name) {
        super(name);
    }

    public String helpGetString(IExpression expr) throws Exception {
        SqlServerSQLTranslator trans = new SqlServerSQLTranslator();
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false), null);
        
        SQLConversionVisitor sqlVisitor = trans.getTranslationVisitor(); 
        sqlVisitor.setFunctionModifiers(trans.getFunctionModifiers());
        sqlVisitor.setLanguageFactory(LANG_FACTORY);  
        sqlVisitor.append(expr);  
        
        return sqlVisitor.toString();        
    }

    public void helpTest(IExpression srcExpression, String tgtType, String expectedExpression) throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            new IExpression[] { 
                srcExpression,
                LANG_FACTORY.createLiteral(tgtType, String.class)},
            DataTypeManager.getDataTypeClass(tgtType));
        
        SqlServerConvertModifier mod = new SqlServerConvertModifier(LANG_FACTORY);
        IExpression expr = mod.modify(func);
        
        assertEquals("Error converting from " + DataTypeManager.getDataTypeName(srcExpression.getType()) + " to " + tgtType, //$NON-NLS-1$ //$NON-NLS-2$ 
            expectedExpression, helpGetString(expr)); 
    }

    // Source = STRING

    public void testStringToChar() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "char", "convert(char, '5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "boolean", "convert(bit, '5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "byte", "convert(tinyint, '5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "short", "convert(smallint, '5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "integer", "convert(int, '5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "long", "convert(numeric, '5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "biginteger", "convert(numeric, '5')");  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "float", "convert(real, '5')");//$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "double", "convert(float, '5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToDate() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("2004-06-29", String.class), "date", "convert(datetime, '2004-06-29')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToTime() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("23:59:59", String.class), "time", "convert(datetime, '23:59:59')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("2004-06-29 23:59:59.987", String.class), "timestamp", "convert(datetime, '2004-06-29 23:59:59.987')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "bigdecimal", "convert(float, '5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
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

    public void testByteToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "boolean", "convert(bit, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "short", "convert(smallint, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "integer", "convert(int, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
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

    public void testShortToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "boolean", "convert(bit, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "byte", "convert(tinyint, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "integer", "convert(int, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
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

    public void testIntegerToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "boolean", "convert(bit, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "byte", "convert(tinyint, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "short", "convert(smallint, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
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

    public void testLongToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "boolean", "convert(bit, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "byte", "convert(tinyint, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "short", "convert(smallint, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "integer", "convert(int, 1)"); //$NON-NLS-1$ //$NON-NLS-2$
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

    public void testBigIntegerToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "boolean", "convert(bit, 1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "byte", "convert(tinyint, 1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "short", "convert(smallint, 1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "integer", "convert(int, 1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
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

    public void testFloatToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "boolean", "convert(bit, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "byte", "convert(tinyint, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "short", "convert(smallint, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "integer", "convert(int, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
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

    public void testDoubleToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "boolean", "convert(bit, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "byte", "convert(tinyint, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "short", "convert(smallint, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "integer", "convert(int, 1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
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

    // Source = BIGDECIMAL
    
    public void testBigDecimalToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "string", "convert(varchar, 1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "boolean", "convert(bit, 1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "byte", "convert(tinyint, 1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "short", "convert(smallint, 1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "integer", "convert(int, 1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
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

    // Source = DATE

    public void testDateToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createDate(103, 10, 1), java.sql.Date.class), "string", "convert(varchar, {d'2003-11-01'}, 101)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDateToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createDate(103, 10, 1), java.sql.Date.class), "timestamp", "convert(datetime, {d'2003-11-01'})"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = TIME

    public void testTimeToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTime(23, 59, 59), java.sql.Time.class), "string", "convert(varchar, {ts'1900-01-01 23:59:59'}, 108)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTimeToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTime(23, 59, 59), java.sql.Time.class), "timestamp", "convert(datetime, {ts'1900-01-01 23:59:59'})"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = TIMESTAMP
    
    public void testTimestampToString() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);        
        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "string", "convert(varchar, {ts'2003-11-01 12:05:02.0'}, 109)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTimestampToDate() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);        
        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "date", "convert(datetime, convert(varchar, {ts'2003-11-01 12:05:02.0'}, 109))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTimestampToTime() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);        
        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "time", "convert(datetime, convert(varchar, {ts'2003-11-01 12:05:02.0'}, 108))"); //$NON-NLS-1$ //$NON-NLS-2$
    }    

}
