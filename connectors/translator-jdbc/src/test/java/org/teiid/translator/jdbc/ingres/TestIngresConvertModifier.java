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

package org.teiid.translator.jdbc.ingres;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Arrays;

import org.junit.Test;
import org.teiid.language.Expression;
import org.teiid.language.Function;
import org.teiid.language.LanguageFactory;
import org.teiid.query.unittest.TimestampUtil;
import org.teiid.translator.TypeFacility;
import org.teiid.translator.jdbc.SQLConversionVisitor;
/**
 */
public class TestIngresConvertModifier {

    private static final LanguageFactory LANG_FACTORY = new LanguageFactory();

    public String helpGetString(Expression expr) throws Exception {
        IngresExecutionFactory trans = new IngresExecutionFactory();
        trans.start();
        SQLConversionVisitor sqlVisitor = trans.getSQLConversionVisitor(); 
        sqlVisitor.append(expr);  
        
        return sqlVisitor.toString();        
    }

    public void helpTest(Expression srcExpression, String tgtType, String expectedExpression) throws Exception {
        Function func = LANG_FACTORY.createFunction("convert",  //$NON-NLS-1$
            Arrays.asList( 
                srcExpression,
                LANG_FACTORY.createLiteral(tgtType, String.class)),
            TypeFacility.getDataTypeClass(tgtType));
        
        assertEquals("Error converting from " + srcExpression.getType() + " to " + tgtType, //$NON-NLS-1$ //$NON-NLS-2$ 
            expectedExpression, helpGetString(func)); 
    }

    // Source = STRING
    @Test public void testStringToChar() throws Exception {
        helpTest(LANG_FACTORY.createLiteral("5", String.class), "char", "cast('5' AS char(1))"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    @Test public void testBooleanToBigDecimal() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(Boolean.TRUE, Boolean.class), "bigdecimal", "cast(1 AS decimal(38,19))"); //$NON-NLS-1$ //$NON-NLS-2$
    }
    
    // Source = BYTE
    
    @Test public void testByteToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "string", "cast(1 AS varchar(4000))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testByteToBoolean() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Byte((byte)1), Byte.class), "boolean", "CASE WHEN 1 = 0 THEN 0 WHEN 1 IS NOT NULL THEN 1 END"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    @Test public void testBigIntegerToDouble() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigInteger("1"), BigInteger.class), "double", "cast(1 AS float)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // Source = FLOAT

    @Test public void testFloatToLong() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Float(1.2f), Float.class), "long", "cast(1.2 AS bigint)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = DOUBLE
    
    @Test public void testDoubleToShort() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new Double(1.2), Double.class), "short", "cast(1.2 AS smallint)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = BIGDECIMAL
    
    @Test public void testBigDecimalToByte() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(new BigDecimal("1.0"), BigDecimal.class), "byte", "cast(1.0 AS tinyint)"); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$
    }

    // Source = DATE

    @Test public void testDateToTimestamp() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createDate(103, 10, 1), java.sql.Date.class), "timestamp", "cast(DATE '2003-11-01' AS timestamp with time zone)"); //$NON-NLS-1$ //$NON-NLS-2$
    }

    // Source = TIME

    @Test public void testTimeToString() throws Exception {
        helpTest(LANG_FACTORY.createLiteral(TimestampUtil.createTime(23, 59, 59), java.sql.Time.class), "string", "cast(TIME '23:59:59' AS varchar(4000))"); //$NON-NLS-1$ //$NON-NLS-2$
    }


    // Source = TIMESTAMP
    
    @Test public void testTimestampToString() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(103, 10, 1, 12, 5, 2, 0);        
        helpTest(LANG_FACTORY.createLiteral(ts, Timestamp.class), "string", "cast(TIMESTAMP '2003-11-01 12:05:02.0' AS varchar(4000))"); //$NON-NLS-1$ //$NON-NLS-2$
    }

}
