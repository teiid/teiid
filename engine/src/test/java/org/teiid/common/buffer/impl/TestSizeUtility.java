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

package org.teiid.common.buffer.impl;

import static org.junit.Assert.*;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class TestSizeUtility {

    public void helpTestGetStaticSize(Object obj, long expectedSize) {
        helpTestGetSize(obj, expectedSize);
    }

    public void helpTestGetSize(Object obj, long expectedSize) {  
        long actualSize = new SizeUtility(null).getSize(obj, true, false);
        assertEquals("Got unexpected size: ", expectedSize, actualSize); //$NON-NLS-1$
    }

    @Test public void testGetSizeChar() {
        helpTestGetStaticSize(new Character('a'), 10);
    }

    @Test public void testGetSizeBoolean() {
        helpTestGetStaticSize(Boolean.TRUE, 1);
    }

    @Test public void testGetSizeByte() {
        helpTestGetStaticSize(new Byte((byte)0), 1);
    }

    @Test public void testGetSizeShort() {
        helpTestGetStaticSize(new Short((short)0), 10);
    }

    @Test public void testGetSizeInteger() {
        helpTestGetStaticSize(new Integer(0), 12);
    }

    @Test public void testGetSizeLong() {
        helpTestGetStaticSize(new Long(0l), 16);
    }
    
    @Test public void testGetSizeFloat() {
        helpTestGetStaticSize(new Float(0), 12);
    }

    @Test public void testGetSizeDouble() {
        helpTestGetStaticSize(new Double(0), 16);
    }

    @Test public void testGetSizeTimestamp() {
        helpTestGetStaticSize(new Timestamp(12301803), 28);
    }

    @Test public void testGetSizeDate() {
        helpTestGetStaticSize(new Date(12301803), 28);
    }

    @Test public void testGetSizeTime() {
        helpTestGetStaticSize(new Time(12301803), 28);
    }

    @Test public void testGetSizeEmptyString() {
        helpTestGetSize("", 40); //$NON-NLS-1$
    }

    @Test public void testGetSizeShortString() {
        helpTestGetSize("abcdefghij", 64); //$NON-NLS-1$
    }

    public void XtestGetSizeLongString() {
        // There is no clear way of figuring out the actual size of a string that is created
        // from a StringBuffer because the buffer can sometimes be twice as big as the actual length of the string
        // Since the data comin from the connector is not created this way, this test is an inaccurate setup 
        int size = 10000;
        StringBuffer str = new StringBuffer();
        for(int i=0; i<size; i++) { 
            str.append("a"); //$NON-NLS-1$
        }
        helpTestGetSize(str.toString(), size+3);
    }

    @Test public void testGetSizeRow1() {
        List<Object> row = new ArrayList<Object>(1);
        row.add(new Integer(0));
        helpTestGetStaticSize(row, 36);
    }
    
    @Test public void testGetSizeRow2() {
        List<Object> row = new ArrayList<Object>(4);
        row.add(new Integer(0));
        row.add(new Integer(101));
        row.add(Boolean.TRUE);
        row.add(new Double(1091203.00));
        helpTestGetStaticSize(row, 89);
    }
    
    @Test public void testGetSizeRows1() {
        helpTestGetStaticSize(new List[] { }, 16);
    }

    @Test public void testGetSizeRows2() {
        List<Object> row1 = new ArrayList<Object>(2);
        row1.add(new Integer(0));
        row1.add(new Integer(100));

        List<Object> row2 = new ArrayList<Object>(2);
        row2.add(new Integer(0));
        row2.add(new Integer(100));

        helpTestGetStaticSize(new List[] { row1, row2 }, 144);
    }
   
    @Test public void testGetSizeBigInteger() {
        BigInteger b = BigInteger.ONE;
                
        helpTestGetStaticSize(b, 48);
    }    
    
    @Test public void testGetSizeBigDecimal() {
        BigDecimal bd = new BigDecimal("1.0"); //$NON-NLS-1$
        
        helpTestGetStaticSize(bd, 96);
    }
    
    @Test public void testGetSizeByteArray() {
        byte[] bytes = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        helpTestGetSize(bytes, 32);
    }
    
    @Test public void testResultSet() {
        List<?>[] expected = new List[] { 
                Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),    "a",    new Integer(0) }), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),    "a",    new Integer(0) }), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),    "a",    new Integer(0) }), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),    "a",    new Integer(0) }), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),    "a",    new Integer(3) }), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList(new Object[] { "a",   new Integer(0),     Boolean.FALSE,  new Double(2.0),    "a",    new Integer(3) }), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0),    "a",    new Integer(0) }), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0),    "a",    new Integer(0) }), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList(new Object[] { "a",   new Integer(3),     Boolean.TRUE,   new Double(7.0),    "a",    new Integer(3) }), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList(new Object[] { "b",   new Integer(2),     Boolean.FALSE,  new Double(0.0),    "b",    new Integer(2) }), //$NON-NLS-1$ //$NON-NLS-2$
                Arrays.asList(new Object[] { "c",   new Integer(1),     Boolean.FALSE,  new Double(0.0),    "c",    new Integer(1) })  //$NON-NLS-1$ //$NON-NLS-2$
           };     
        
        String[] types = {"string", "integer", "boolean", "double", "string", "integer"};     //$NON-NLS-1$//$NON-NLS-2$//$NON-NLS-3$//$NON-NLS-4$ //$NON-NLS-5$//$NON-NLS-6$

        long actualSize = new SizeUtility(types).getBatchSize(Arrays.asList(expected));
        assertEquals("Got unexpected size: ", 2667, actualSize); //$NON-NLS-1$        
    }
    
}