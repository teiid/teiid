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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Properties;

import org.teiid.connector.api.TypeFacility;
import org.teiid.connector.jdbc.oracle.OracleConvertModifier;
import org.teiid.connector.jdbc.oracle.OracleSQLTranslator;
import org.teiid.connector.jdbc.translator.SQLConversionVisitor;
import org.teiid.connector.language.IExpression;
import org.teiid.connector.language.IFunction;
import org.teiid.connector.language.ILanguageFactory;

import junit.framework.TestCase;

import com.metamatrix.cdk.CommandBuilder;
import com.metamatrix.cdk.api.EnvironmentUtility;
import com.metamatrix.query.unittest.TimestampUtil;

/**
 */
public class TestOracleConvertModifier extends TestCase {

    private static final ILanguageFactory LANG_FACTORY = CommandBuilder.getLanuageFactory();

    /**
     * Constructor for TestSybaseConvertModifier.
     * @param name
     */
    public TestOracleConvertModifier(String name) {
        super(name);
    }

    public String helpGetString(IExpression expr) throws Exception {
        OracleSQLTranslator trans = new OracleSQLTranslator();
        trans.initialize(EnvironmentUtility.createEnvironment(new Properties(), false));
        
        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor(); 
        sqlVisitor.append(expr);  
        
        return sqlVisitor.toString();        
    }

    public void helpTest(IExpression srcExpression, String tgtType, String expectedExpression) throws Exception {
        IFunction func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            Arrays.asList( 
                srcExpression,
                LANG_FACTORY.createLiteral(tgtType, String.class)),
            TypeFacility.getDataTypeClass(tgtType));
        
        OracleConvertModifier mod = new OracleConvertModifier(LANG_FACTORY, null);
        IExpression expr = mod.modify(func);
        
        assertEquals("Error converting from " + srcExpression.getType() + " to " + tgtType, //$NON-NLS-1$ //$NON-NLS-2$ 
            expectedExpression, helpGetString(expr)); 
    }

    // Source = STRING

    public void testStringToChar() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "char", "'5'"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "boolean", "decode('5', 'true', 1, 'false', 0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "byte", "to_number('5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "short", "to_number('5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "integer", "to_number('5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "long", "to_number('5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "biginteger", "to_number('5')");  //$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "float", "to_number('5')");//$NON-NLS-1$//$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "double", "to_number('5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToDate() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("2004-06-29", String.class), "date", "to_date('2004-06-29', 'YYYY-MM-DD')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToTime() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("23:59:59", String.class), "time", "to_date(('1970-01-01 ' || to_char('23:59:59', 'HH24:MI:SS')), 'YYYY-MM-DD HH24:MI:SS')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("2004-06-29 23:59:59.987", String.class), "timestamp", "to_timestamp('2004-06-29 23:59:59.987', 'YYYY-MM-DD HH24:MI:SS.FF')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testStringToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "bigdecimal", "to_number('5')"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // Source = CHAR
    
    public void testCharToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Character('5'), Character.class), "string", "'5'"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BOOLEAN
    
    public void testBooleanToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "string", "decode(1, 0, 'false', 1, 'true')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "byte", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "short", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "integer", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "long", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "biginteger", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "float", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "double", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testBooleanToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "bigdecimal", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    // Source = BYTE
    
    public void testByteToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "string", "to_char(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "boolean", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "short", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "integer", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "long", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "biginteger", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "float", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "double", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testByteToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "bigdecimal", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = SHORT
    
    public void testShortToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "string", "to_char(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "boolean", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "byte", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "integer", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "long", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "biginteger", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "float", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "double", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testShortToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Short((short)1), Short.class), "bigdecimal", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = INTEGER
    
    public void testIntegerToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "string", "to_char(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "boolean", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "byte", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "short", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "long", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "biginteger", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "float", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "double", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testIntegerToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Integer(1), Integer.class), "bigdecimal", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = LONG
    
    public void testLongToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "string", "to_char(1)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "boolean", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "byte", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "short", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "integer", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "biginteger", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "float", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "double", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testLongToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Long(1), Long.class), "bigdecimal", "1"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BIGINTEGER
    
    public void testBigIntegerToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "string", "to_char(1)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "boolean", "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "byte", "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "short", "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "integer", "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "long", "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "float", "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "double", "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigIntegerToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "bigdecimal", "1"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // Source = FLOAT
    
    public void testFloatToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "string", "to_char(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "boolean", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "byte", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "short", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "integer", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "long", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "biginteger", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "double", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testFloatToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "bigdecimal", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = DOUBLE
    
    public void testDoubleToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "string", "to_char(1.2)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "boolean", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "byte", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "short", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "integer", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "long", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "biginteger", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "float", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDoubleToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "bigdecimal", "1.2"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BIGDECIMAL
    
    public void testBigDecimalToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "string", "to_char(1.0)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "boolean", "1.0"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "byte", "1.0"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "short", "1.0"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "integer", "1.0"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "long", "1.0"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToBigInteger() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "biginteger", "1.0"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToFloat() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "float", "1.0"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    public void testBigDecimalToDoublel() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "double", "1.0"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // Source = DATE

    public void testDateToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createDate(103, 10, 1), java.sql.Date.class), "string", "to_char({d'2003-11-01'}, 'YYYY-MM-DD')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testDateToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createDate(103, 10, 1), java.sql.Date.class), "timestamp", "cast({d'2003-11-01'} AS timestamp)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = TIME

    public void testTimeToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTime(23, 59, 59), java.sql.Time.class), "string", "to_char({ts'1970-01-01 23:59:59'}, 'HH24:MI:SS')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTimeToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTime(23, 59, 59), java.sql.Time.class), "timestamp", "cast({ts'1970-01-01 23:59:59'} AS timestamp)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = TIMESTAMP
    
    public void testTimestampToString() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);        
        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "string", "to_char({ts'2003-11-01 12:05:02.0'}, 'YYYY-MM-DD HH24:MI:SS.FF')"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTimestampToDate() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);        
        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "date", "trunc({ts'2003-11-01 12:05:02.0'})"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    public void testTimestampToTime() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);        
        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "time", "to_date(('1970-01-01 ' || to_char({ts'2003-11-01 12:05:02.0'}, 'HH24:MI:SS')), 'YYYY-MM-DD HH24:MI:SS')"); //$NON-NLS-1$ //$NON-NLS-2$
    }    

}
