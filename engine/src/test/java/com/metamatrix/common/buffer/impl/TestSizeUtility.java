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

package com.metamatrix.common.buffer.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

/**
 */
public class TestSizeUtility extends TestCase {

    private static final int DEFAULT_BUFFER_SIZE = 4096;


    /**
     * Constructor for TestSizeUtility.
     * @param arg0
     */
    public TestSizeUtility(String arg0) {
        super(arg0);
    }

    public static int helpCalculateSerializedSize(Object obj) {
        ByteArrayOutputStream bout = null;
        ObjectOutputStream oout = null;
        try { 
            bout = new ByteArrayOutputStream(DEFAULT_BUFFER_SIZE);
            oout = new ObjectOutputStream(bout);

            // Write standard stuff to the stream - this will ensure that only the 
            // difference of adding an additional object matters
            oout.writeObject(null);
            oout.writeObject(Boolean.TRUE);
            oout.writeObject(new Character('a'));
            oout.writeObject(new Byte((byte)0));
            oout.writeObject(new Short((short)0));
            oout.writeObject(new Integer(0));
            oout.writeObject(new Long(0));
            oout.writeObject(new Float(0));
            oout.writeObject(new Double(0));
            oout.writeObject(new Date(9832498));
            oout.writeObject(new Time(9832498));
            oout.writeObject(new Timestamp(9832498));
            oout.writeObject("x"); //$NON-NLS-1$
            oout.writeObject(new BigInteger("0")); //$NON-NLS-1$
            oout.writeObject(new BigDecimal("0")); //$NON-NLS-1$
            oout.writeObject(new ArrayList());
            oout.writeObject(new List[] { });
            oout.flush();
            int firstCount = bout.size();

            oout.writeObject(obj);
            oout.flush();
            int secondCount = bout.size();

            return secondCount - firstCount;
        } catch(IOException e) {
            throw new RuntimeException(e.getMessage());
        } finally { 
            try {
                oout.close();    
            } catch(IOException e) {
            }
        }        
    }

    public void helpTestGetStaticSize(Object obj, long expectedSize) {
        helpTestGetSize(obj, expectedSize);
    }

    public void helpTestGetSize(Object obj, long expectedSize) {  
        long actualSize = SizeUtility.getSize(obj);
        assertEquals("Got unexpected size: ", expectedSize, actualSize); //$NON-NLS-1$
    }
    
    public void testBitness() {

        if ( SizeUtility.IS_64BIT ) {
            assertEquals("Got unexpected reference size: ", 8, SizeUtility.REFERENCE_SIZE); //$NON-NLS-1$
        } else {
            assertEquals("Got unexpected reference size: ", 4, SizeUtility.REFERENCE_SIZE); //$NON-NLS-1$
        }
    }

    public void testGetSizeNull() {
        helpTestGetStaticSize(null, 0);
    }

    public void testGetSizeChar() {
        helpTestGetStaticSize(new Character('a'), 20);
    }

    public void testGetSizeBoolean() {
        helpTestGetStaticSize(Boolean.TRUE, 20);
    }

    public void testGetSizeByte() {
        helpTestGetStaticSize(new Byte((byte)0), 20);
    }

    public void testGetSizeShort() {
        helpTestGetStaticSize(new Short((short)0), 20);
    }

    public void testGetSizeInteger() {
        helpTestGetStaticSize(new Integer(0), 20);
    }

    public void testGetSizeLong() {
        helpTestGetStaticSize(new Long(0l), 20);
    }
    
    public void testGetSizeFloat() {
        helpTestGetStaticSize(new Float(0), 20);
    }

    public void testGetSizeDouble() {
        helpTestGetStaticSize(new Double(0), 20);
    }

    public void testGetSizeTimestamp() {
        helpTestGetStaticSize(new Timestamp(12301803), 32);
    }

    public void testGetSizeDate() {
        helpTestGetStaticSize(new Date(12301803), 32);
    }

    public void testGetSizeTime() {
        helpTestGetStaticSize(new Time(12301803), 32);
    }

    public void testGetSizeEmptyString() {
        helpTestGetSize("", 40); //$NON-NLS-1$
    }

    public void testGetSizeShortString() {
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

    public void testGetSizeRow1() {
        List row = new ArrayList(1);
        row.add(new Integer(0));
     	 int size = (SizeUtility.IS_64BIT ? 140 : 100);
         helpTestGetStaticSize(row, size);
    }
    
    public void testGetSizeRow2() {
        List row = new ArrayList(4);
        row.add(new Integer(0));
        row.add(new Integer(101));
        row.add(Boolean.TRUE);
        row.add(new Double(1091203.00));
//        helpTestGetStaticSize(row, 160);
        int size = (SizeUtility.IS_64BIT ? 200 : 160);
        helpTestGetStaticSize(row, size);
    }
    
    public void testGetSizeRows1() {
        helpTestGetStaticSize(new List[] { }, 16);
    }

    public void testGetSizeRows2() {
        List row1 = new ArrayList(2);
        row1.add(new Integer(0));
        row1.add(new Integer(100));

        List row2 = new ArrayList(2);
        row2.add(new Integer(0));
        row2.add(new Integer(100));

        int size = (SizeUtility.IS_64BIT ? 352 : 264);
        helpTestGetStaticSize(new List[] { row1, row2 }, size);
    }
   
    static class MyClass implements Serializable{
        int intnum = 0;
        float floatnum = 2.3f;
    }
    static class MyBigClass extends MyClass{
        String data = null;
        public MyBigClass() {
            for (int i = 0; i < 1024; i++) {
                data += "One Quick Fox jumpmed over the lazy dog";  //$NON-NLS-1$
            }
        }
    }
    
    public void testGetObjectSize() {
        helpTestGetStaticSize(new MyClass(), 120);   
    }
    public void testGetBigObjectSize() {
        helpTestGetStaticSize(new MyBigClass(), 40168);   
    }
 
    public void testGetSizeBigInteger() {
        BigInteger b = BigInteger.ONE;
        BigInteger two = new BigInteger("2"); //$NON-NLS-1$
                
        int offset = b.bitLength()/8;
        helpTestGetStaticSize(b, 56+alignMemory(4+offset));
        
        offset = b.bitLength()/8;
        helpTestGetStaticSize(two, 56+alignMemory(4+offset));

         
        b = new BigInteger("43");        //$NON-NLS-1$
        offset = b.bitLength()/8;
        helpTestGetStaticSize(b, 56+alignMemory(4+offset));
        

        b = new BigInteger("-2398040231");        //$NON-NLS-1$
        offset = b.bitLength()/8;
        helpTestGetStaticSize(b, 56+alignMemory(4+offset));        

        b = new BigInteger("-298348945983429812809152098510951091509");        //$NON-NLS-1$
        offset = b.bitLength()/8;
        helpTestGetStaticSize(b, 56+alignMemory(4+offset));
    }    
    
    public void testGetSizeBigDecimal() {
        BigDecimal bd = new BigDecimal("1.0"); //$NON-NLS-1$
        
        int offset = (bd.unscaledValue().bitLength())/8;
        helpTestGetStaticSize(bd, 72+alignMemory(4+offset));
        
        bd = new BigDecimal("123.00001"); //$NON-NLS-1$
        offset = (bd.unscaledValue().bitLength())/8;
        helpTestGetStaticSize(bd, 72+alignMemory(4+offset));

        bd = new BigDecimal("123456789012345678.00001234235"); //$NON-NLS-1$
        offset = (bd.unscaledValue().bitLength())/8;
        helpTestGetStaticSize(bd, 72+alignMemory(4+offset));
        
    }
    
    public void testGetSizeByteArray() {
        byte[] bytes = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        helpTestGetSize(bytes, 56);
    }
    
    public void XtestExperimentGetSizeBigInteger() {
        BigInteger b = BigInteger.ONE;
        BigInteger two = new BigInteger("2"); //$NON-NLS-1$
        for(int i=1; i<100; i++) {             
            b = b.multiply(two);
            System.out.println("" + b + ":\t" + ", bitLength=" + b.bitLength() + ", actual=" + helpCalculateSerializedSize(b) + ", calc=" + SizeUtility.getSize(b)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
        }
 
        b = new BigInteger("43");        //$NON-NLS-1$
        System.out.println("" + b + ":\t" + ", bitLength=" + b.bitLength() + ", actual=" + helpCalculateSerializedSize(b) + ", calc=" + SizeUtility.getSize(b)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$

        b = new BigInteger("-2398040231");        //$NON-NLS-1$
        System.out.println("" + b + ":\t" + ", bitLength=" + b.bitLength() + ", actual=" + helpCalculateSerializedSize(b) + ", calc=" + SizeUtility.getSize(b)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$

        b = new BigInteger("-298348945983429812809152098510951091509");        //$NON-NLS-1$
        System.out.println("" + b + ":\t" + ", bitLength=" + b.bitLength() + ", actual=" + helpCalculateSerializedSize(b) + ", calc=" + SizeUtility.getSize(b));                 //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$
    }

    public void XtestExperimentGetSizeBigDecimal() {
        BigDecimal b = new BigDecimal("1.0"); //$NON-NLS-1$
        BigDecimal two = new BigDecimal("2"); //$NON-NLS-1$
        for(int i=1; i<100; i++) {             
            b = b.multiply(two);
            System.out.println("" + b + ":\t" + ", scale=" + b.scale() + ", intval=" + helpCalculateSerializedSize(b.unscaledValue()) +", actual=" + helpCalculateSerializedSize(b) + ", calc=" + SizeUtility.getSize(b)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$
        } 
    }

    public void XtestExperimentGetSizeString() {
        for(int size=0; size<100; size++) {
            StringBuffer str = new StringBuffer();
            for(int i=0; i<size; i++) { 
                str.append("a"); //$NON-NLS-1$
            }
            String s = str.toString();
            System.out.println("" + size + ":\t" + ", actual=" + helpCalculateSerializedSize(s) + ", computed=" + SizeUtility.getSize(s)); //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$
        }
    }
    
    private static long alignMemory(long numBytes) {
        long remainder = numBytes % 8;
        if (remainder != 0) {
            numBytes += (8 - remainder);
        }
        return numBytes;
    }
    
    public void testResultSet() {
        List[] expected = new List[] { 
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

        long actualSize = SizeUtility.getSize(expected);
        int size = (SizeUtility.IS_64BIT ? 2920 : 2616);
        assertEquals("Got unexpected size: ", size, actualSize); //$NON-NLS-1$

        size = (SizeUtility.IS_64BIT ? 3096 : 2792);
        actualSize = SizeUtility.getBatchSize(types, expected);
        assertEquals("Got unexpected size: ", size, actualSize); //$NON-NLS-1$        
    }
    
    public void testResultSet2() {
        List[] expected = new List[] { 
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
                Arrays.asList(new Object[] { "c",   null,     Boolean.FALSE,  new Double(0.0),    "c",    new Integer(1) })  //$NON-NLS-1$ //$NON-NLS-2$
           };     
        
        String[] types = {"string", "integer", "boolean", "double", "string", "integer"};     //$NON-NLS-1$//$NON-NLS-2$//$NON-NLS-3$//$NON-NLS-4$ //$NON-NLS-5$//$NON-NLS-6$

        int size = (SizeUtility.IS_64BIT ? 2900 : 2596);
        long actualSize = SizeUtility.getSize(expected);
        assertEquals("Got unexpected size: ", size, actualSize); //$NON-NLS-1$

        size = (SizeUtility.IS_64BIT ? 3096 : 2792);
        actualSize = SizeUtility.getBatchSize(types, expected);
        assertEquals("Got unexpected size: ", size, actualSize); //$NON-NLS-1$        
    }
}
