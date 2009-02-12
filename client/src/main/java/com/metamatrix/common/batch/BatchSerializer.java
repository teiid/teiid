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

package com.metamatrix.common.batch;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.metamatrix.admin.AdminPlugin;
import com.metamatrix.common.types.DataTypeManager;


/** 
 * @since 4.2
 */
public class BatchSerializer {
    
    private BatchSerializer() {} // Uninstantiable
    
    private static final Map serializers = new HashMap(18, 1.0f);
    static {
        serializers.put(DataTypeManager.DefaultDataTypes.BIG_DECIMAL,   new BigDecimalColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.BIG_INTEGER,   new BigIntegerColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.BLOB,          new ObjectColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.BOOLEAN,       new BooleanColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.BYTE,          new ByteColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.CHAR,          new CharColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.CLOB,          new ObjectColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.XML,          new ObjectColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.DATE,          new DateColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.DOUBLE,        new DoubleColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.FLOAT,         new FloatColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.INTEGER,       new IntColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.LONG,          new LongColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.OBJECT,        new ObjectColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.SHORT,         new ShortColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.STRING,        new StringColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.TIME,          new TimeColumnSerializer());
        serializers.put(DataTypeManager.DefaultDataTypes.TIMESTAMP,     new TimestampColumnSerializer());
    }
    
    /**
     * Packs the (boolean) information about whether data values in the column are null
     * into bytes so that we send ~n/8 instead of n bytes.
     * @param out
     * @param col
     * @param batch
     * @throws IOException
     * @since 4.2
     */
    static void writeIsNullData(ObjectOutput out, int col, List[] batch) throws IOException {
        int numBytes = batch.length / 8, row = 0, currentByte = 0;
        for (int byteNum = 0; byteNum < numBytes; byteNum++, row+=8) {
            currentByte  = (batch[row].get(col) == null) ? 0x80 : 0;
            if (batch[row+1].get(col) == null) currentByte |= 0x40;
            if (batch[row+2].get(col) == null) currentByte |= 0x20;
            if (batch[row+3].get(col) == null) currentByte |= 0x10;
            if (batch[row+4].get(col) == null) currentByte |= 0x08;
            if (batch[row+5].get(col) == null) currentByte |= 0x04;
            if (batch[row+6].get(col) == null) currentByte |= 0x02;
            if (batch[row+7].get(col) == null) currentByte |= 0x01;
            out.write(currentByte);
        }
        if (batch.length % 8 > 0) {
            currentByte = 0;
            for (int mask = 0x80; row < batch.length; row++, mask >>= 1) {
                if (batch[row].get(col) == null) currentByte |= mask;
            }
            out.write(currentByte);
        }
    }
    /**
     * Reads the isNull data into a byte array 
     * @param in
     * @param isNullBytes
     * @throws IOException
     * @since 4.2
     */
    static void readIsNullData(ObjectInput in, byte[] isNullBytes) throws IOException {
        for (int i = 0; i < isNullBytes.length; i++) {
            isNullBytes[i] = in.readByte();
        }
    }
    
    /**
     * Gets whether a data value is null based on a packed byte array containing boolean data 
     * @param isNull
     * @param row
     * @return
     * @since 4.2
     */
    static boolean isNullObject(byte[] isNull, int row) {
        //              byte number           mask     bits to shift mask
        return (isNull [ row / 8 ]         & (0x01 << (7 - (row % 8))))   != 0;
    }
    
    /**
     * An interface representing a stateless serializer of a batch column 
     * @since 4.2
     */
    private static interface ColumnSerializer {
        void writeColumn(ObjectOutput out, int col, List[] results) throws IOException;
        void readColumn(ObjectInput in, int col, List[] batch, byte[] isNullNuffer) throws IOException, ClassNotFoundException;
    }
    
    /**
     * An abstract serializer for native types
     * @since 4.2
     */
    private static abstract class AbstractNativeColumnSerializer implements ColumnSerializer {
        public void writeColumn(ObjectOutput out, int col, List[] batch) throws IOException {
            writeIsNullData(out, col, batch);
            Object obj = null;
            for (int i = 0; i < batch.length; i++) {
                obj = batch[i].get(col);
                if (obj != null) {
                    writeObject(out, obj);
                }
            }
        }
        
        public void readColumn(ObjectInput in, int col, List[] batch, byte[] isNull) throws IOException, ClassNotFoundException {
            readIsNullData(in, isNull);
            for (int i = 0; i < batch.length; i++) {
                if (!isNullObject(isNull, i)) {
                    batch[i].set(col, readObject(in));
                }
            }
        }
        
        protected abstract void writeObject(ObjectOutput out, Object obj) throws IOException;
        protected abstract Object readObject(ObjectInput in) throws IOException;
    }
    
    private static class IntColumnSerializer extends AbstractNativeColumnSerializer {
        protected void writeObject(ObjectOutput out, Object obj) throws IOException {
            out.writeInt(((Integer)obj).intValue());
        }
        protected Object readObject(ObjectInput in) throws IOException {
            return Integer.valueOf(in.readInt());
        }
    }
    
    private static class LongColumnSerializer extends AbstractNativeColumnSerializer {
        protected void writeObject(ObjectOutput out, Object obj) throws IOException {
            out.writeLong(((Long)obj).longValue());
        }
        protected Object readObject(ObjectInput in) throws IOException {
            return Long.valueOf(in.readLong());
        }
    }
    
    private static class FloatColumnSerializer extends AbstractNativeColumnSerializer {
        protected void writeObject(ObjectOutput out, Object obj) throws IOException {
            out.writeFloat(((Float)obj).floatValue());
        }
        protected Object readObject(ObjectInput in) throws IOException {
            return new Float(in.readFloat());
        }
    }
    
    private static class DoubleColumnSerializer extends AbstractNativeColumnSerializer {
        protected void writeObject(ObjectOutput out, Object obj) throws IOException {
            out.writeDouble(((Double)obj).doubleValue());
        }
        protected Object readObject(ObjectInput in) throws IOException {
            return new Double(in.readDouble());
        }
    }
    
    private static class ShortColumnSerializer extends AbstractNativeColumnSerializer {
        protected void writeObject(ObjectOutput out, Object obj) throws IOException {
            out.writeShort(((Short)obj).shortValue());
        }
        protected Object readObject(ObjectInput in) throws IOException {
            return Short.valueOf(in.readShort());
        }
    }
    
    private static class BooleanColumnSerializer implements ColumnSerializer {
        /* This implementation compacts the isNull and boolean data for non-null values into a byte[]
         * by using a 8 bit mask that is bit-shifted to mask each value.
         */
        public void writeColumn(ObjectOutput out, int col, List[] batch) throws IOException {
            int currentByte = 0;
            int mask = 0x80;
            Object obj;
            for (int row = 0; row < batch.length; row++) {
                // Write the isNull value
                obj = batch[row].get(col);
                if (obj == null ) {
                    currentByte |= mask;
                }
                mask >>= 1; // Shift the mask to the next bit
                if (mask == 0) {
                    // If the current byte has been used up, write it and reset.
                    out.write(currentByte);
                    currentByte = 0;
                    mask = 0x80;
                }
                if (obj != null) {
                    // Write the boolean value if it's not null
                    if (((Boolean)obj).booleanValue()) {
                        currentByte |= mask;
                    }
                    mask >>= 1;
                    if (mask == 0) {
                        out.write(currentByte);
                        currentByte = 0;
                        mask = 0x80;
                    }
                }
            }
            // Invariant mask != 0
            // If we haven't reached the eight-row mark then the loop would not have written this byte
            // Write the final byte containing data for th extra rows, if it exists.
            if (mask != 0x80) {
                out.write(currentByte);
            }
        }
        
        public void readColumn(ObjectInput in, int col, List[] batch, byte[] isNull) throws IOException, ClassNotFoundException {
            int currentByte = 0, mask = 0; // Initialize the mask so that it is reset in the loop
            boolean isNullVal;
            for (int row = 0; row < batch.length; row++) {
                if (mask == 0) {
                    // If we used up the byte, read the next one, and reset the mask
                    currentByte = in.read();
                    mask = 0x80;
                }
                isNullVal = (currentByte & mask) != 0;
                mask >>= 1; // Shift the mask to the next bit
                if (!isNullVal) {
                    if (mask == 0) {
                        currentByte = in.read();
                        mask = 0x80;
                    }
                    batch[row].set(col, ((currentByte & mask) == 0) ? Boolean.FALSE : Boolean.TRUE);
                    mask >>= 1;
                }
            }
        }
    }
    
    private static class ByteColumnSerializer extends AbstractNativeColumnSerializer {
        protected void writeObject(ObjectOutput out, Object obj) throws IOException {
            out.writeByte(((Byte)obj).byteValue());
        }
        protected Object readObject(ObjectInput in) throws IOException {
            return Byte.valueOf(in.readByte());
        }
    }
    
    private static class CharColumnSerializer extends AbstractNativeColumnSerializer {
        protected void writeObject(ObjectOutput out, Object obj) throws IOException {
            out.writeChar(((Character)obj).charValue());
        }
        protected Object readObject(ObjectInput in) throws IOException {
            return Character.valueOf(in.readChar());
        }
    }
    
    private static class StringColumnSerializer extends AbstractNativeColumnSerializer {
        /*
         * This implementation writes single-byte chars until it reaches a non-ascii char in the string,
         * at which point it starts writing two-byte characters. This implementation never writes more
         * than two bytes per char.
         */
        protected void writeObject(ObjectOutput out, Object obj) throws IOException {
            String val = (String)obj;
            int length = val.length();
            out.writeInt(length);
            boolean writingShort = true;
            char c;
            for (int i = 0 ; i < length; i++) {
                if (writingShort) {
                    /* charAt() simply gets the char out of the underlying array. The assumption is that this would be quicker
                     * calling getChars() which makes a copy of the underlying char[].
                     */
                    c = val.charAt(i);
                    if (c < 0x80) {
                        out.write(c);
                    } else {
                        out.write(0x80);
                        writingShort = false;
                        out.writeChar(c);
                    }
                } else {
                    out.writeChar(val.charAt(i));
                }
            }
        }
        protected Object readObject(ObjectInput in) throws IOException {
            int b;
            boolean readingShort;
            int length = in.readInt();
            /* Although using a StringBuffer and doing a toString() to get the String value reuses
             * the StringBuffer's internal char[], the StringBuffer.append() calls are all synchronized,
             * and likely too costly compared to simply copying the array during derialization.
             */
            char[] chars = new char[length];
            readingShort = true;
            for (int i = 0; i < length; i++) {
                if (readingShort) {
                    b = in.read();
                    if (b == 0x80) {
                        readingShort = false;
                        chars[i] = in.readChar();
                    } else {
                        chars[i] = ((char)b);
                    }
                } else {
                    chars[i] = in.readChar();
                }
            }
            return new String(chars);
        }
    }
    
    private static class BigIntegerColumnSerializer extends AbstractNativeColumnSerializer {
        protected void writeObject(ObjectOutput out, Object obj) throws IOException {
            BigInteger val = (BigInteger)obj;
            byte[] bytes = val.toByteArray();
            out.writeInt(bytes.length);
            out.write(bytes);
        }
        protected Object readObject(ObjectInput in) throws IOException {
            int length = in.readInt();
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            return new BigInteger(bytes);
        }
    }
    
    private static class BigDecimalColumnSerializer extends AbstractNativeColumnSerializer {
        protected void writeObject(ObjectOutput out, Object obj) throws IOException {
            BigDecimal val = (BigDecimal)obj;
            out.writeInt(val.scale());
            BigInteger unscaled = val.unscaledValue();
            byte[] bytes = unscaled.toByteArray();
            out.writeInt(bytes.length);
            out.write(bytes);
        }
        protected Object readObject(ObjectInput in) throws IOException {
            int scale = in.readInt();
            int length = in.readInt();
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            return new BigDecimal(new BigInteger(bytes), scale);
        }
    }
    
    private static class DateColumnSerializer extends AbstractNativeColumnSerializer {
        protected void writeObject(ObjectOutput out, Object obj) throws IOException {
            out.writeLong(((java.sql.Date)obj).getTime());
        }
        protected Object readObject(ObjectInput in) throws IOException {
            return new java.sql.Date(in.readLong());
        }
    }
    
    private static class TimeColumnSerializer extends AbstractNativeColumnSerializer {
        protected void writeObject(ObjectOutput out, Object obj) throws IOException {
            out.writeLong(((Time)obj).getTime());
        }
        protected Object readObject(ObjectInput in) throws IOException {
            return new Time(in.readLong());
        }
    }
    
    private static class TimestampColumnSerializer extends AbstractNativeColumnSerializer {
        protected void writeObject(ObjectOutput out, Object obj) throws IOException {
            Timestamp ts =  (Timestamp)obj;
            out.writeLong(ts.getTime());
            out.writeInt(ts.getNanos());
        }
        protected Object readObject(ObjectInput in) throws IOException {
            Timestamp ts = new Timestamp(in.readLong());
            ts.setNanos(in.readInt());
            return ts;
        }
    }
    
    private static class ObjectColumnSerializer implements ColumnSerializer {
        public void writeColumn(ObjectOutput out, int col, List[] results) throws IOException {
            for (int i = 0; i < results.length; i++) {
                out.writeObject(results[i].get(col));
            }
        }
        
        public void readColumn(ObjectInput in, int col, List[] batch, byte[] isNull) throws IOException, ClassNotFoundException {
            for (int i = 0; i < batch.length; i++) {
                batch[i].set(col, in.readObject());
            }
        }
    }
    
    private static ColumnSerializer getSerializer(String type) {
        ColumnSerializer cs = (ColumnSerializer)serializers.get((type == null) ? DataTypeManager.DefaultDataTypes.OBJECT : type);
        assert cs != null;
        return cs;
    }
    
    public static void writeBatch(ObjectOutput out, String[] types, List[] batch) throws IOException {
        // If there are no type hints, simply use the default mechanism to serialize
        if (types == null || types.length == 0) {
            out.writeObject(batch);
            return;
        }
        if (batch == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(batch.length);
            if (batch.length > 0) {
	            int columns = types.length;
	            out.writeInt(columns);
	            for(int i = 0; i < columns; i++) {
	            	ColumnSerializer serializer = getSerializer(types[i]);
	                try {
	                    serializer.writeColumn(out, i, batch);
	                } catch (ClassCastException e) {
	                    Object obj = null;
	                    String objectClass = null;
	                    objectSearch: for (int row = 0; row < batch.length; row++) {
	                        obj = batch[row].get(i);
	                        if (obj != null) {
	                            objectClass = obj.getClass().getName();
	                            break objectSearch;
	                        }
	                    }
	                    throw new IOException(AdminPlugin.Util.getString("BatchSerializer.datatype_mismatch", new Object[] {types[i], new Integer(i), objectClass})); //$NON-NLS-1$
	                }
	            }
            }
        }
    }
    
    public static List[] readBatch(ObjectInput in, String[] types) throws IOException, ClassNotFoundException {
        // If there are no type hints, use the default mechanism to deserialize
        if (types == null || types.length == 0) {
            return (List[])in.readObject();
        }
        int rows = in.readInt();
        if (rows == 0) {
            return new List[0];
        } else if (rows > 0) {
            int columns = in.readInt();
            List[] batch = new List[rows];
            int numBytes = rows/8;
            int extraRows = rows % 8;
            for (int currentRow = 0; currentRow < rows; currentRow++) {
                batch[currentRow] = Arrays.asList(new Object[columns]);
            }
            byte[] isNullBuffer = new byte[(extraRows > 0) ? numBytes + 1: numBytes];
            for (int col = 0; col < columns; col++) {
                getSerializer(types[col]).readColumn(in, col, batch, isNullBuffer);
            }
            return batch;
        }
        return null;
    }
}
