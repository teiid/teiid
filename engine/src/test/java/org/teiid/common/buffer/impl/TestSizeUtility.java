/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.teiid.common.buffer.impl;

import static org.junit.Assert.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.DataTypeManager;

public class TestSizeUtility {

    private static class SomeObject implements Serializable {
        private String state;
        public SomeObject(String state) {
            this.state = state;
        }
    }

    public void helpTestGetStaticSize(Object obj, long expectedSize) {
        helpTestGetSize(obj, expectedSize);
    }

    public void helpTestGetSize(Object obj, long expectedSize) {
        long actualSize = SizeUtility.getSize(obj, false);
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
        helpTestGetStaticSize(new Long(0L), 16);
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

    @Test public void testGetSizeObject() {
        helpTestGetStaticSize(new SomeObject(null), 16);
        helpTestGetStaticSize(new SomeObject("Hello world"), 56);  //$NON-NLS-1$
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
        helpTestGetSize(new BinaryType(bytes), 32);
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

        Class<?>[] types = {DataTypeManager.DefaultDataClasses.STRING,
                DataTypeManager.DefaultDataClasses.INTEGER,
                DataTypeManager.DefaultDataClasses.BOOLEAN,
                DataTypeManager.DefaultDataClasses.DOUBLE,
                DataTypeManager.DefaultDataClasses.STRING,
                DataTypeManager.DefaultDataClasses.INTEGER};

        long actualSize = new SizeUtility(types).getBatchSize(false, Arrays.asList(expected));
        assertEquals("Got unexpected size: ", 2667, actualSize); //$NON-NLS-1$
    }

}