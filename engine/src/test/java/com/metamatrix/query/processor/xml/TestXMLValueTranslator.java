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

package com.metamatrix.query.processor.xml;

import java.math.BigInteger;
import java.sql.Timestamp;

import junit.framework.TestCase;

import com.metamatrix.common.types.DataTypeManager;
import com.metamatrix.query.unittest.TimestampUtil;

public class TestXMLValueTranslator extends TestCase {

    public TestXMLValueTranslator(String name) {
        super(name);
    }
    
    public void test24HourDateTimeTranslation() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(100, 0, 2, 14, 14, 5, 6);
        
        String value = XMLValueTranslator.translateToXMLValue(ts, DataTypeManager.DefaultDataClasses.TIMESTAMP, XMLValueTranslator.DATETIME);
        assertEquals("2000-01-02T14:14:05.000000006", value); //$NON-NLS-1$
    }
    
    public void testDateTimeTranslation() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(100, 0, 2, 3, 4, 5, 6);
        
        String value = XMLValueTranslator.translateToXMLValue(ts, DataTypeManager.DefaultDataClasses.TIMESTAMP, XMLValueTranslator.DATETIME);
        assertEquals("2000-01-02T03:04:05.000000006", value); //$NON-NLS-1$
        
        ts.setNanos(6000);
        
        value = XMLValueTranslator.translateToXMLValue(ts, DataTypeManager.DefaultDataClasses.TIMESTAMP, XMLValueTranslator.DATETIME);
        assertEquals("2000-01-02T03:04:05.000006", value); //$NON-NLS-1$
        
        ts.setNanos(0);

        value = XMLValueTranslator.translateToXMLValue(ts, DataTypeManager.DefaultDataClasses.TIMESTAMP, XMLValueTranslator.DATETIME);
        assertEquals("2000-01-02T03:04:05", value); //$NON-NLS-1$
        
        ts = TimestampUtil.createTimestamp(-2000, 0, 2, 3, 4, 5, 6);

        value = XMLValueTranslator.translateToXMLValue(ts, DataTypeManager.DefaultDataClasses.TIMESTAMP, XMLValueTranslator.DATETIME);
        assertEquals("-0101-01-02T03:04:05.000000006", value); //$NON-NLS-1$
    }
    
    public void testgYearTranslation() throws Exception {
        String value = XMLValueTranslator.translateToXMLValue(new BigInteger("5"), DataTypeManager.DefaultDataClasses.BIG_INTEGER, XMLValueTranslator.GYEAR); //$NON-NLS-1$
        assertEquals("0005", value); //$NON-NLS-1$

        value = XMLValueTranslator.translateToXMLValue(new BigInteger("-10000"), DataTypeManager.DefaultDataClasses.BIG_INTEGER, XMLValueTranslator.GYEAR); //$NON-NLS-1$
        assertEquals("-10000", value); //$NON-NLS-1$
    }
    
    public void testgYearMonthTranslation() throws Exception {
        Timestamp ts = TimestampUtil.createTimestamp(100, 0, 4, 6, 8, 10, 12);
        
        String value = XMLValueTranslator.translateToXMLValue(ts, DataTypeManager.DefaultDataClasses.TIMESTAMP, XMLValueTranslator.GYEARMONTH); 
        assertEquals("2000-01", value); //$NON-NLS-1$
        
        ts = TimestampUtil.createTimestamp(-30000, 0, 4, 6, 8, 10, 12);
        
        value = XMLValueTranslator.translateToXMLValue(ts, DataTypeManager.DefaultDataClasses.TIMESTAMP, XMLValueTranslator.GYEARMONTH); 
        assertEquals("-28101-01", value); //$NON-NLS-1$
    }
    
    public void testDefaultTranslation() throws Exception {
        String value = XMLValueTranslator.translateToXMLValue("", DataTypeManager.DefaultDataClasses.STRING, XMLValueTranslator.STRING); //$NON-NLS-1$
        assertNull(value); 
    }
    
}
