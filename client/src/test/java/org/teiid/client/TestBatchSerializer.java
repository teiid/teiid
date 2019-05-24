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

package org.teiid.client;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.GeographyType;
import org.teiid.core.types.GeometryType;
import org.teiid.core.types.JsonType;
import org.teiid.core.util.TimestampWithTimezone;
import org.teiid.query.unittest.TimestampUtil;


/**
 * @since 4.2
 */
@SuppressWarnings("nls")
public class TestBatchSerializer {

    private static List<List<Object>> helpTestSerialization(String[] types, List<?>[] batch, byte version) throws IOException, ClassNotFoundException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ObjectOutputStream out = new ObjectOutputStream(byteStream);
        List<List<?>> batchList = Arrays.asList(batch);

        BatchSerializer.writeBatch(out, types, batchList, version);
        out.flush();

        byte[] bytes = byteStream.toByteArray();

        ByteArrayInputStream bytesIn = new ByteArrayInputStream(bytes);
        ObjectInputStream in = new ObjectInputStream(bytesIn);
        List<List<Object>> newBatch = BatchSerializer.readBatch(in, types);
        out.close();
        in.close();

        assertTrue(batchList.equals(newBatch));
        return newBatch;
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
                                                      DataTypeManager.DefaultDataTypes.TIMESTAMP,
                                                      DataTypeManager.DefaultDataTypes.OBJECT,
                                                      DataTypeManager.DefaultDataTypes.VARBINARY,
                                                     };
    private static String sampleString(int length) {
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = (char)i;
        }
        return new String(chars);
    }

    private static List<?>[] sampleBatchWithNulls(int rows) {
        List<?>[] batch = new List[rows];

        for (int i = 0; i < rows; i++) {
            java.util.Date d = new java.util.Date();
            int mod = i%16;
            Object[] data = { (mod == 0) ? null : new BigDecimal("" + i), //$NON-NLS-1$
                              (mod == 1) ? null : new BigInteger(Integer.toString(i)),
                              (mod == 2) ? null : ((i%2 == 0) ? Boolean.FALSE: Boolean.TRUE),
                              (mod == 3) ? null : new Byte((byte)i),
                              (mod == 4) ? null : new Character((char)i),
                              (mod == 5) ? null : TimestampWithTimezone.createDate(d),
                              (mod == 6) ? null : new Double(i),
                              (mod == 7) ? null : new Float(i),
                              (mod == 8) ? null : new Integer(i),
                              (mod == 9) ? null : new Long(i),
                              (mod == 10) ? null : new Short((short)i),
                              (mod == 11) ? null : sampleString(i),
                              (mod == 12) ? null : TimestampWithTimezone.createTime(d),
                              (mod == 13) ? null : TimestampWithTimezone.createTimestamp(d),
                              (mod == 14) ? null : TimestampWithTimezone.createTimestamp(d),
                              (mod == 15) ? null : new BinaryType(new byte[] {(byte)i}),
                            };
            batch[i] = Arrays.asList(data);
        }
        return batch;
    }

    @Test public void testSerializeBasicTypes() throws Exception {
        // The number 8 is important here because boolean isNull information is packed into bytes,
        // so we want to make sure the boundary cases are handled correctly
        helpTestSerialization(sampleBatchTypes, sampleBatchWithNulls(1), BatchSerializer.CURRENT_VERSION); // Less than 8 rows
        helpTestSerialization(sampleBatchTypes, sampleBatchWithNulls(8), BatchSerializer.CURRENT_VERSION); // Exactly 8 rows
        helpTestSerialization(sampleBatchTypes, sampleBatchWithNulls(17), BatchSerializer.CURRENT_VERSION); // More than 8 rows, but not a multiple of 8
        helpTestSerialization(sampleBatchTypes, sampleBatchWithNulls(120), BatchSerializer.CURRENT_VERSION); // A multiple of 8 rows
        helpTestSerialization(sampleBatchTypes, sampleBatchWithNulls(833), BatchSerializer.CURRENT_VERSION); // A bunch of rows. This should also test large strings
        helpTestSerialization(sampleBatchTypes, sampleBatchWithNulls(4096), BatchSerializer.CURRENT_VERSION); // A bunch of rows. This should also test large strings
    }

    @Test public void testSerializeLargeStrings() throws Exception {
        List<?> row = Arrays.asList(new Object[] {sampleString(66666)});
        helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.STRING}, new List[] {row}, BatchSerializer.CURRENT_VERSION);
    }

    @Test public void testSerializeNoData() throws Exception {
        helpTestSerialization(sampleBatchTypes, new List[0], BatchSerializer.CURRENT_VERSION);
    }

    @Test public void testSerializeDatatypeMismatch() throws Exception {
        try {
            helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.DOUBLE}, new List[] {Arrays.asList(new Object[] {"Hello!"})}, BatchSerializer.CURRENT_VERSION); //$NON-NLS-1$
        } catch (RuntimeException e) {
            assertEquals("TEIID20001 The modeled datatype double for column 0 doesn't match the runtime type \"java.lang.String\". Please ensure that the column's modeled datatype matches the expected data.", e.getMessage()); //$NON-NLS-1$
        }
    }

    @Test(expected=IOException.class) public void testOutOfRangeDate() throws Exception {
        helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.DATE}, new List[] {Arrays.asList(TimestampUtil.createDate(-2, 0, 1))}, (byte)1);
    }

    @Test public void testStringArray() throws IOException, ClassNotFoundException {
        helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.LONG,  "string[]"}, new List[] {Arrays.asList(1L, new ArrayImpl(new String[] {"Silly String", "Silly String"}))}, BatchSerializer.CURRENT_VERSION);
    }

    @Test public void testGeometry() throws IOException, ClassNotFoundException {
        GeometryType geometryType = new GeometryType(new byte[0]);
        geometryType.setReferenceStreamId(null);
        geometryType.setSrid(10000);
        Object val = helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.GEOMETRY}, new List[] {Arrays.asList(geometryType)}, BatchSerializer.VERSION_GEOMETRY).get(0).get(0);
        assertTrue(val instanceof GeometryType);
        assertEquals(10000, ((GeometryType)val).getSrid());
        helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.GEOMETRY}, new List[] {Arrays.asList(geometryType)}, (byte)0); //object serialization - should fail on the client side

        val = helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.GEOMETRY}, new List[] {Arrays.asList(geometryType)}, (byte)1).get(0).get(0); //blob serialization
        assertFalse(val instanceof GeometryType);

        val = helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.OBJECT}, new List[] {Arrays.asList(geometryType)}, (byte)1).get(0).get(0); //blob serialization
        assertFalse(val instanceof GeometryType);
    }

    @Test public void testGeography() throws IOException, ClassNotFoundException {
        GeometryType geometryType = new GeometryType(new byte[0]);
        geometryType.setReferenceStreamId(null);
        geometryType.setSrid(4326);
        Object val = helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.GEOGRAPHY}, new List[] {Arrays.asList(geometryType)}, BatchSerializer.VERSION_GEOGRAPHY).get(0).get(0);
        assertTrue(val instanceof GeographyType);
        assertEquals(4326, ((GeographyType)val).getSrid());
        helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.GEOGRAPHY}, new List[] {Arrays.asList(geometryType)}, (byte)0); //object serialization - should fail on the client side

        val = helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.GEOGRAPHY}, new List[] {Arrays.asList(geometryType)}, (byte)1).get(0).get(0); //blob serialization
        assertFalse(val instanceof GeographyType);

        val = helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.OBJECT}, new List[] {Arrays.asList(geometryType)}, (byte)1).get(0).get(0); //blob serialization
        assertFalse(val instanceof GeographyType);
    }

    @Test public void testJson() throws IOException, ClassNotFoundException {
        JsonType json = new JsonType(new ClobImpl("5"));
        json.setReferenceStreamId(null);
        Object val = helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.JSON}, new List[] {Arrays.asList(json)}, BatchSerializer.VERSION_GEOGRAPHY).get(0).get(0);
        assertTrue(val instanceof JsonType);
        helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.JSON}, new List[] {Arrays.asList(json)}, (byte)0); //object serialization - should fail on the client side

        val = helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.JSON}, new List[] {Arrays.asList(json)}, (byte)1).get(0).get(0); //clob serialization
        assertTrue(val instanceof ClobType);

        val = helpTestSerialization(new String[] {DataTypeManager.DefaultDataTypes.OBJECT}, new List[] {Arrays.asList(json)}, (byte)1).get(0).get(0); //clob serialization
        assertTrue(val instanceof ClobType);
    }

}
