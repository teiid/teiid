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

package org.teiid.client;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.teiid.core.types.DataTypeManager;




/** 
 * @since 4.2
 */
public class TestBatchSerializer extends TestCase {

    private static void assertEqual(List[] expectedBatch, List[] batch) {
        if (expectedBatch == null) {
            assertNull(batch);
            return;
        }
        assertEquals(expectedBatch.length, batch.length);
        if (expectedBatch.length > 0) {
            int columns = expectedBatch[0].size();
            for (int row = 0; row < expectedBatch.length; row++) {
                for (int col = 0; col < columns; col++) {
                    assertEquals(expectedBatch[row].get(col), batch[row].get(col));
                }
            }
        }
    }
    
    private static void helpTestSerialization(String[] types, List<?>[] batch) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(byteStream);
        List<List<?>> batchList = Arrays.asList(batch);
        BatchSerializer.writeBatch(out, types, batchList);
        out.flush();
        
        byte[] bytes = byteStream.toByteArray();
        
        ByteArrayInputStream bytesIn = new ByteArrayInputStream(bytes);
        ObjectInputStream in = new ObjectInputStream(bytesIn);
        List<List<Object>> newBatch = BatchSerializer.readBatch(in, types);
        out.close();
        in.close();

        assertTrue(batchList.equals(newBatch));
    }
    
    private static final String[] sampleBatchTypes = {DataTypeManager.DefaultDataTypes.BIG_DECIMAL,
                                                      DataTypeManager.DefaultDataTypes.BIG_INTEGER,
                                                      DataTypeManager.DefaultDataTypes.BOOLEAN,
                                                      DataTypeManager.DefaultDataTypes.BYTE,
                                                      DataTypeManager.DefaultDataTypes.CHAR,
                                                      DataTypeManager.DefaultDataTypes.DATE,
                                                      DataTypeManager.DefaultDataTypes.DOUBLE,
                                                      DataTypeManager.DefaultDataTypes.FLOAT,
                                                      DataTypeManager.DefaultDataTypes.INTEGER,
                                                      DataTypeManager.DefaultDataTypes.LONG,
                                                      DataTypeManager.DefaultDataTypes.SHORT,
                                                      DataTypeManager.DefaultDataTypes.STRING,
                                                      DataTypeManager.DefaultDataTypes.TIME,
                                                      DataTypeManager.DefaultDataTypes.TIMESTAMP
                                                     };
    private static String sampleString(int length) {
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = (char)i;
        }
        return new String(chars);
    }
    
    private static List[] sampleBatch(int rows) {
        List[] batch = new List[rows];
        
        for (int i = 0; i < rows; i++) {
            long currentTime = System.currentTimeMillis();
            Object[] data = { new BigDecimal("" + i), //$NON-NLS-1$
                              new BigInteger(Integer.toString(i)),
                              (i%2 == 0) ? Boolean.FALSE: Boolean.TRUE,
                              new Byte((byte)i),
                              new Character((char)i),
                              new Date(currentTime),
                              new Double(i),
                              new Float(i),
                              new Integer(i),
                              new Long(i),
                              new Short((short)i),
                              sampleString(i),
                              new Time(currentTime),
                              new Timestamp(currentTime)
                            };
            batch[i] = Arrays.asList(data);
        }
        return batch;
    }
    
    private static List[] sampleBatchWithNulls(int rows) {
        List[] batch = new List[rows];
        
        for (int i = 0; i < rows; i++) {
            long currentTime = System.currentTimeMillis();
            int mod = i%14;
            Object[] data = { (mod == 0) ? null : new BigDecimal("" + i), //$NON-NLS-1$
                              (mod == 1) ? null : new BigInteger(Integer.toString(i)),
                              (mod == 2) ? null : ((i%2 == 0) ? Boolean.FALSE: Boolean.TRUE),
                              (mod == 3) ? null : new Byte((byte)i),
                              (mod == 4) ? null : new Character((char)i),
                              (mod == 5) ? null : new Date(currentTime),
                              (mod == 6) ? null : new Double(i),
                              (mod == 7) ? null : new Float(i),
                              (mod == 8) ? null : new Integer(i),
                              (mod == 9) ? null : new Long(i),
                              (mod == 10) ? null : new Short((short)i),
                              (mod == 11) ? null : sampleString(i),
                              (mod == 12) ? null : new Time(currentTime),
                              (mod == 13) ? null : new Timestamp(currentTime)
                            };
            batch[i] = Arrays.asList(data);
        }
        return batch;
    }
    
    public void testSerializeBasicTypes() throws Exception {
        // The number 8 is important here because boolean isNull information is packed into bytes,
        // so we want to make sure the boundary cases are handled correctly
        helpTestSerialization(sampleBatchTypes, sampleBatch(1)); // Less than 8 rows
        helpTestSerialization(sampleBatchTypes, sampleBatch(8)); // Exactly 8 rows
        helpTestSerialization(sampleBatchTypes, sampleBatch(17)); // More than 8 rows, but not a multiple of 8
        helpTestSerialization(sampleBatchTypes, sampleBatch(120)); // A multiple of 8 rows
        helpTestSerialization(sampleBatchTypes, sampleBatch(833)); // A bunch of rows. This should also test large strings
    }
    
    public void testSerializeBasicTypesWithNulls() throws Exception {
        helpTestSerialization(sampleBatchTypes, sampleBatchWithNulls(1));
        helpTestSerialization(sampleBatchTypes, sampleBatchWithNulls(8));
        helpTestSerialization(sampleBatchTypes, sampleBatchWithNulls(17));
        helpTestSerialization(sampleBatchTypes, sampleBatchWithNulls(120));
        helpTestSerialization(sampleBatchTypes, sampleBatchWithNulls(833));
    }
    
    public void testSerializeLargeStrings() throws Exception {
        List row = Arrays.asList(new Object[] {sampleString(66666)});
        helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.STRING}, new List[] {row});
    }
    
    public void testSerializeNoData() throws Exception {
        helpTestSerialization(sampleBatchTypes, new List[0]);
    }
    
    public void testSerializeDatatypeMismatch() throws Exception {
        try {
            helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.DOUBLE}, new List[] {Arrays.asList(new Object[] {"Hello!"})}); //$NON-NLS-1$
        } catch (RuntimeException e) {
            assertEquals("The modeled datatype double for column 0 doesn't match the runtime type \"java.lang.String\". Please ensure that the column's modeled datatype matches the expected data.", e.getMessage()); //$NON-NLS-1$
        }
    }
}
