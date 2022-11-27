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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ObjectStreamConstants;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Array;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.ArrayImpl;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.GeographyType;
import org.teiid.core.types.GeometryType;
import org.teiid.core.types.JsonType;
import org.teiid.core.types.XMLType;
import org.teiid.jdbc.JDBCPlugin;



/**
 * @since 4.2
 *
 * <ul>
 * <li>version 0: starts with 7.1 and uses simple serialization too broadly
 * <li>version 1: starts with 8.0 uses better string, blob, clob, xml, etc.
 *   add varbinary support.
 *   however was possibly silently truncating date/time values that were
 *   outside of jdbc allowed values
 * <li>version 2: starts with 8.2 and adds better array serialization and
 *   uses a safer date/time serialization
 * <li>version 3: starts with 8.6 and adds better repeated string performance
 * <li>version 4: starts with 8.10 and adds the geometry type
 * <li>version 5: starts with 11.2 and adds the geography and json types
 * </ul>
 */
public class BatchSerializer {

    public static final byte VERSION_GEOMETRY = (byte)4;
    public static final byte VERSION_GEOGRAPHY = (byte)5;
    static final byte CURRENT_VERSION = VERSION_GEOGRAPHY;

    private BatchSerializer() {} // Uninstantiable

    private static ColumnSerializer defaultSerializer = new ColumnSerializer();

    private static final Map<String, ColumnSerializer[]> serializers = new HashMap<String, ColumnSerializer[]>(128);
    static {
        serializers.put(DataTypeManager.DefaultDataTypes.BIG_DECIMAL,   new ColumnSerializer[] {new BigDecimalColumnSerializer()});
        serializers.put(DataTypeManager.DefaultDataTypes.BIG_INTEGER,   new ColumnSerializer[] {new BigIntegerColumnSerializer()});
        serializers.put(DataTypeManager.DefaultDataTypes.BOOLEAN,       new ColumnSerializer[] {new BooleanColumnSerializer()});
        serializers.put(DataTypeManager.DefaultDataTypes.BYTE,          new ColumnSerializer[] {new ByteColumnSerializer()});
        serializers.put(DataTypeManager.DefaultDataTypes.CHAR,          new ColumnSerializer[] {new CharColumnSerializer()});
        serializers.put(DataTypeManager.DefaultDataTypes.DATE,          new ColumnSerializer[] {new DateColumnSerializer(), new DateColumnSerializer1(), new DateColumnSerializer()});
        serializers.put(DataTypeManager.DefaultDataTypes.DOUBLE,        new ColumnSerializer[] {new DoubleColumnSerializer()});
        serializers.put(DataTypeManager.DefaultDataTypes.FLOAT,         new ColumnSerializer[] {new FloatColumnSerializer()});
        serializers.put(DataTypeManager.DefaultDataTypes.INTEGER,       new ColumnSerializer[] {new IntColumnSerializer()});
        serializers.put(DataTypeManager.DefaultDataTypes.LONG,          new ColumnSerializer[] {new LongColumnSerializer()});
        serializers.put(DataTypeManager.DefaultDataTypes.SHORT,         new ColumnSerializer[] {new ShortColumnSerializer()});
        serializers.put(DataTypeManager.DefaultDataTypes.TIME,          new ColumnSerializer[] {new TimeColumnSerializer(), new TimeColumnSerializer1(), new TimeColumnSerializer()});
        serializers.put(DataTypeManager.DefaultDataTypes.TIMESTAMP,     new ColumnSerializer[] {new TimestampColumnSerializer()});
        serializers.put(DataTypeManager.DefaultDataTypes.STRING,         new ColumnSerializer[] {defaultSerializer, new StringColumnSerializer1(), new StringColumnSerializer1(), new StringColumnSerializer3()});
        serializers.put(DataTypeManager.DefaultDataTypes.CLOB,             new ColumnSerializer[] {defaultSerializer, new ClobColumnSerializer1()});
        serializers.put(DataTypeManager.DefaultDataTypes.JSON,          new ColumnSerializer[] {defaultSerializer, new ClobColumnSerializer1(), new ClobColumnSerializer1(), new ClobColumnSerializer1(), new ClobColumnSerializer1(), new JsonColumnSerializer()});
        serializers.put(DataTypeManager.DefaultDataTypes.BLOB,             new ColumnSerializer[] {defaultSerializer, new BlobColumnSerializer1()});
        serializers.put(DataTypeManager.DefaultDataTypes.GEOMETRY,         new ColumnSerializer[] {defaultSerializer, new GeometryColumnSerializer()});
        serializers.put(DataTypeManager.DefaultDataTypes.GEOGRAPHY,      new ColumnSerializer[] {defaultSerializer, new GeographyColumnSerializer()});
        serializers.put(DataTypeManager.DefaultDataTypes.XML,             new ColumnSerializer[] {defaultSerializer, new XmlColumnSerializer1()});
        serializers.put(DataTypeManager.DefaultDataTypes.NULL,             new ColumnSerializer[] {defaultSerializer, new NullColumnSerializer1()});
        serializers.put(DataTypeManager.DefaultDataTypes.OBJECT,         new ColumnSerializer[] {defaultSerializer, new ObjectColumnSerializer((byte)1)});
        serializers.put(DataTypeManager.DefaultDataTypes.VARBINARY,        new ColumnSerializer[] {new BinaryColumnSerializer(), new BinaryColumnSerializer1()});
    }

    private static ColumnSerializer arrayColumnSerializer = new ColumnSerializer() {

        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version)
                throws IOException {
            try {
                super.writeObject(out, ((java.sql.Array)obj).getArray(), cache, version);
            } catch (SQLException e) {
                throw new IOException(e);
            }
        }

        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException,
                ClassNotFoundException {
            return new ArrayImpl((Object[]) in.readObject());
        }

    };

    private static final ColumnSerializer arrayColumnSerialier2 = new ArrayColumnSerializer2(new ObjectColumnSerializer((byte)2));

    private static final class ArrayColumnSerializer2 extends ColumnSerializer {

        ObjectColumnSerializer ser;

        public ArrayColumnSerializer2(ObjectColumnSerializer ser) {
            this.ser = ser;
        }

        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version)
                throws IOException {
            Object[] values = null;
            try {
                values = (Object[]) ((Array)obj).getArray();
            } catch (SQLException e) {
                out.writeInt(-1);
                return;
            }
            out.writeInt(values.length);
            int code = DataTypeManager.getTypeCode(values.getClass().getComponentType());
            if ((code == DataTypeManager.DefaultTypeCodes.GEOMETRY && version < VERSION_GEOMETRY)
                    || (code == DataTypeManager.DefaultTypeCodes.GEOGRAPHY && version < VERSION_GEOGRAPHY)){
                code = DataTypeManager.DefaultTypeCodes.BLOB;
            } else if ((code == DataTypeManager.DefaultTypeCodes.JSON && version < VERSION_GEOGRAPHY)){
                code = DataTypeManager.DefaultTypeCodes.CLOB;
            }
            out.writeByte((byte)code);
            for (int i = 0; i < values.length;) {
                writeIsNullData(out, i, values);
                int end = Math.min(values.length, i+8);
                for (; i < end; i++) {
                    if (values[i] != null) {
                        this.ser.writeObject(out, values[i], code, cache, version);
                    }
                }
            }
            out.writeBoolean((obj instanceof ArrayImpl && ((ArrayImpl)obj).isZeroBased()));
        }

        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException,
                ClassNotFoundException {
            int length = in.readInt();
            if (length == -1) {
                return new ArrayImpl(null);
            }
            int code = in.readByte();
            Object[] vals = (Object[])java.lang.reflect.Array.newInstance(DataTypeManager.getClass(code), length);
            for (int i = 0; i < length;) {
                byte b = in.readByte();
                int end = Math.min(length, i+8);
                for (; i < end; i++) {
                    if (!isNullObject(i, b)) {
                        vals[i] = this.ser.readObject(in, cache, code, version);
                    }
                }
            }
            ArrayImpl result = new ArrayImpl(vals);
            result.setZeroBased(in.readBoolean());
            return result;
        }

        @Override
        public boolean usesCache(byte version) {
            return version >= 3;
        }
    }

    static class BinaryColumnSerializer1 extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version)
                throws IOException {
            byte[] bytes = ((BinaryType)obj).getBytes();
            out.writeInt(bytes.length); //in theory this could be a short, but we're not strictly enforcing the length
            out.write(bytes);
        }

        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException,
                ClassNotFoundException {
            int length = in.readInt();
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            return new BinaryType(bytes);
        }
    }

    static class BinaryColumnSerializer extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version)
                throws IOException {
            //uses object serialization for compatibility with legacy clients
            super.writeObject(out, ((BinaryType)obj).getBytesDirect(), cache, version);
        }

        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException,
                ClassNotFoundException {
            //won't actually be used
            byte[] bytes = (byte[])super.readObject(in, cache, version);
            return new BinaryType(bytes);
        }
    }

    public static final class ObjectColumnSerializer extends ColumnSerializer {

        byte defaultVersion;

        public ObjectColumnSerializer(byte version) {
            this.defaultVersion = version;
        }

        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version)
                throws IOException {
            int code = DataTypeManager.getTypeCode(obj.getClass());
            if (code == DataTypeManager.DefaultTypeCodes.GEOMETRY) {
                if (version < VERSION_GEOMETRY) {
                    code = DataTypeManager.DefaultTypeCodes.BLOB;
                }
            } else if (code == DataTypeManager.DefaultTypeCodes.GEOGRAPHY) {
                if (version < VERSION_GEOGRAPHY) {
                    code = DataTypeManager.DefaultTypeCodes.BLOB;
                }
            }
            out.writeByte((byte)code);
            writeObject(out, obj, code, cache, version<VERSION_GEOMETRY?this.defaultVersion:version);
        }

        protected void writeObject(ObjectOutput out, Object obj, int code, Map<Object, Integer> cache, byte effectiveVersion)
                throws IOException {
            if (code == DataTypeManager.DefaultTypeCodes.BOOLEAN) {
                if (Boolean.TRUE.equals(obj)) {
                    out.write((byte)1);
                } else {
                    out.write((byte)0);
                }
            } else if (code == DataTypeManager.DefaultTypeCodes.OBJECT) {
                super.writeObject(out, obj, cache, effectiveVersion);
            } else {
                String name = DataTypeManager.getDataTypeName(obj.getClass());
                ColumnSerializer s = getSerializer(name, effectiveVersion);
                s.writeObject(out, obj, cache, effectiveVersion);
            }
        }

        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException,
                ClassNotFoundException {
            int code = in.readByte();
            return readObject(in, cache, code, version<VERSION_GEOMETRY?this.defaultVersion:version);
        }

        private Object readObject(ObjectInput in, List<Object> cache, int code, byte effectiveVersion) throws IOException,
                ClassNotFoundException {
            if (code == DataTypeManager.DefaultTypeCodes.BOOLEAN) {
                if (in.readByte() == (byte)0) {
                    return Boolean.FALSE;
                }
                return Boolean.TRUE;
            }
            if (code != DataTypeManager.DefaultTypeCodes.OBJECT) {
                ColumnSerializer s = getSerializer(DataTypeManager.getDataTypeName(DataTypeManager.getClass(code)), effectiveVersion);
                return s.readObject(in, cache, effectiveVersion);
            }
            return super.readObject(in, cache, effectiveVersion);
        }

        @Override
        public boolean usesCache(byte version) {
            return version >= 3;
        }

    }

    private static final int MAX_UTF = 0xFFFF/3; //this is greater than the expected max length of Teiid Strings

    private static class StringColumnSerializer1 extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            String str = (String)obj;
            if (str.length() <= MAX_UTF) {
                //skip object serialization if we have a short string
                out.writeByte(ObjectStreamConstants.TC_STRING);
                out.writeUTF(str);
            } else {
                out.writeByte(ObjectStreamConstants.TC_LONGSTRING);
                out.writeObject(obj);
            }
        }

        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException,
                ClassNotFoundException {
            if (in.readByte() == ObjectStreamConstants.TC_STRING) {
                return in.readUTF();
            }
            return super.readObject(in, cache, version);
        }

    }

    private static class StringColumnSerializer3 extends StringColumnSerializer1 {
        private static final int MAX_INLINE_STRING_LENGTH = 5;
        private static final byte REPEATED_STRING = 0;
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version)
                throws IOException, ClassNotFoundException {
            byte b = in.readByte();
            if (b == ObjectStreamConstants.TC_STRING) {
                String val = in.readUTF();
                if (val.length() > MAX_INLINE_STRING_LENGTH) {
                    cache.add(val);
                }
                return val;
            }
            if (b == REPEATED_STRING) {
                Integer val = in.readInt();
                return cache.get(val);
            }
            String val = (String) in.readObject();
            if (val.length() > MAX_INLINE_STRING_LENGTH) {
                cache.add(val);
            }
            return val;
        }

        @Override
        protected void writeObject(ObjectOutput out, Object obj,
                Map<Object, Integer> cache, byte version) throws IOException {
            String str = (String)obj;
            Integer val = cache.get(str);
            if (val != null) {
                out.writeByte(REPEATED_STRING);
                out.writeInt(val);
                return;
            }
            if (str.length() > MAX_INLINE_STRING_LENGTH) {
                cache.put(str, cache.size());
            }
            super.writeObject(out, obj, cache, version);
        }

        @Override
        public boolean usesCache(byte version) {
            return true;
        }
    }

    private static class NullColumnSerializer1 extends ColumnSerializer {
        @Override
        public void writeColumn(ObjectOutput out, int col,
                List<? extends List<?>> batch, Map<Object, Integer> cache, byte version) throws IOException {
        }

        @Override
        public void readColumn(ObjectInput in, int col,
                List<List<Object>> batch, byte[] isNull, List<Object> cache, byte version) throws IOException,
                ClassNotFoundException {
        }
    }

    private static class ClobColumnSerializer1 extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            ((Externalizable)obj).writeExternal(out);
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException, ClassNotFoundException {
            ClobType ct = new ClobType();
            ct.readExternal(in);
            return ct;
        }
    }

    private static class JsonColumnSerializer extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            ((Externalizable)obj).writeExternal(out);
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException, ClassNotFoundException {
            JsonType ct = new JsonType();
            ct.readExternal(in);
            return ct;
        }
    }

    private static class BlobColumnSerializer1 extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            ((Externalizable)obj).writeExternal(out);
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException, ClassNotFoundException {
            BlobType bt = new BlobType();
            bt.readExternal(in);
            return bt;
        }
    }

    private static class GeometryColumnSerializer extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            ((Externalizable)obj).writeExternal(out);
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException, ClassNotFoundException {
            if (version < VERSION_GEOMETRY) {
                BlobType bt = new BlobType();
                bt.readExternal(in);
                return bt;
            }
            GeometryType bt = new GeometryType();
            bt.readExternal(in);
            return bt;
        }
    }

    private static class GeographyColumnSerializer extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            ((Externalizable)obj).writeExternal(out);
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException, ClassNotFoundException {
            if (version < VERSION_GEOGRAPHY) {
                BlobType bt = new BlobType();
                bt.readExternal(in);
                return bt;
            }
            GeographyType bt = new GeographyType();
            bt.readExternal(in);
            return bt;
        }
    }

    private static class XmlColumnSerializer1 extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            ((XMLType)obj).writeExternal(out, (byte) 1);
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException, ClassNotFoundException {
            XMLType xt = new XMLType();
            xt.readExternal(in, (byte)1);
            return xt;
        }
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
    static void writeIsNullData(ObjectOutput out, int col, List<? extends List<?>> batch) throws IOException {
        int numBytes = batch.size() / 8, row = 0, currentByte = 0;
        for (int byteNum = 0; byteNum < numBytes; byteNum++, row+=8) {
            currentByte  = (batch.get(row).get(col) == null) ? 0x80 : 0;
            if (batch.get(row+1).get(col) == null) {
                currentByte |= 0x40;
            }
            if (batch.get(row+2).get(col) == null) {
                currentByte |= 0x20;
            }
            if (batch.get(row+3).get(col) == null) {
                currentByte |= 0x10;
            }
            if (batch.get(row+4).get(col) == null) {
                currentByte |= 0x08;
            }
            if (batch.get(row+5).get(col) == null) {
                currentByte |= 0x04;
            }
            if (batch.get(row+6).get(col) == null) {
                currentByte |= 0x02;
            }
            if (batch.get(row+7).get(col) == null) {
                currentByte |= 0x01;
            }
            out.write(currentByte);
        }
        if (batch.size() % 8 > 0) {
            currentByte = 0;
            for (int mask = 0x80; row < batch.size(); row++, mask >>= 1) {
                if (batch.get(row).get(col) == null) {
                    currentByte |= mask;
                }
            }
            out.write(currentByte);
        }
    }

    static void writeIsNullData(ObjectOutput out, int offset, Object[] batch) throws IOException {
        int currentByte = 0;
        for (int mask = 0x80; offset < batch.length; offset++, mask >>= 1) {
            if (batch[offset] == null) {
                currentByte |= mask;
            }
        }
        out.write(currentByte);
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
    static final boolean isNullObject(byte[] isNull, int row) {
        //              byte number           mask     bits to shift mask
        return (isNull [ row / 8 ]         & (0x01 << (7 - (row % 8))))   != 0;
    }

    private static final boolean isNullObject(int row, byte b) {
        return (b         & (0x01 << (7 - (row % 8))))   != 0;
    }

    /**
     * An abstract serializer for native types
     * @since 4.2
     */
    private static class ColumnSerializer {
        public void writeColumn(ObjectOutput out, int col, List<? extends List<?>> batch, Map<Object, Integer> cache, byte version) throws IOException {
            writeIsNullData(out, col, batch);
            Object obj = null;
            for (int i = 0; i < batch.size(); i++) {
                obj = batch.get(i).get(col);
                if (obj != null) {
                    writeObject(out, obj, cache, version);
                }
            }
        }

        public void readColumn(ObjectInput in, int col, List<List<Object>> batch, byte[] isNull, List<Object> cache, byte version) throws IOException, ClassNotFoundException {
            readIsNullData(in, isNull);
            for (int i = 0; i < batch.size(); i++) {
                if (!isNullObject(isNull, i)) {
                    batch.get(i).set(col, DataTypeManager.getCanonicalValue(readObject(in, cache, version)));
                }
            }
        }

        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            out.writeObject(obj);
        }
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException, ClassNotFoundException {
            return in.readObject();
        }

        public boolean usesCache(byte version) {
            return false;
        }
    }

    private static class IntColumnSerializer extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            out.writeInt(((Integer)obj).intValue());
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException {
            return Integer.valueOf(in.readInt());
        }
    }

    private static class LongColumnSerializer extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            out.writeLong(((Long)obj).longValue());
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException {
            return Long.valueOf(in.readLong());
        }
    }

    private static class FloatColumnSerializer extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            out.writeFloat(((Float)obj).floatValue());
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException {
            return new Float(in.readFloat());
        }
    }

    private static class DoubleColumnSerializer extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            out.writeDouble(((Double)obj).doubleValue());
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException {
            return new Double(in.readDouble());
        }
    }

    private static class ShortColumnSerializer extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            out.writeShort(((Short)obj).shortValue());
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException {
            return Short.valueOf(in.readShort());
        }
    }

    private static class BooleanColumnSerializer extends ColumnSerializer {
        /* This implementation compacts the isNull and boolean data for non-null values into a byte[]
         * by using a 8 bit mask that is bit-shifted to mask each value.
         */
        @Override
        public void writeColumn(ObjectOutput out, int col, List<? extends List<?>> batch, Map<Object, Integer> cache, byte version) throws IOException {
            int currentByte = 0;
            int mask = 0x80;
            Object obj;
            for (int row = 0; row < batch.size(); row++) {
                // Write the isNull value
                obj = batch.get(row).get(col);
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
            // Write the final byte containing data for the extra rows, if it exists.
            if (mask != 0x80) {
                out.write(currentByte);
            }
        }

        @Override
        public void readColumn(ObjectInput in, int col,
                List<List<Object>> batch, byte[] isNull, List<Object> cache, byte version) throws IOException,
                ClassNotFoundException {
            int currentByte = 0, mask = 0; // Initialize the mask so that it is reset in the loop
            boolean isNullVal;
            for (int row = 0; row < batch.size(); row++) {
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
                    batch.get(row).set(col, ((currentByte & mask) == 0) ? Boolean.FALSE : Boolean.TRUE);
                    mask >>= 1;
                }
            }
        }
    }

    private static class ByteColumnSerializer extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            out.writeByte(((Byte)obj).byteValue());
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException {
            return Byte.valueOf(in.readByte());
        }
    }

    private static class CharColumnSerializer extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            out.writeChar(((Character)obj).charValue());
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException {
            return Character.valueOf(in.readChar());
        }
    }

    private static class BigIntegerColumnSerializer extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            BigInteger val = (BigInteger)obj;
            byte[] bytes = val.toByteArray();
            out.writeInt(bytes.length);
            out.write(bytes);
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException {
            int length = in.readInt();
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            return new BigInteger(bytes);
        }
    }

    private static class BigDecimalColumnSerializer extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            BigDecimal val = (BigDecimal)obj;
            out.writeInt(val.scale());
            BigInteger unscaled = val.unscaledValue();
            byte[] bytes = unscaled.toByteArray();
            out.writeInt(bytes.length);
            out.write(bytes);
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException {
            int scale = in.readInt();
            int length = in.readInt();
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            return new BigDecimal(new BigInteger(bytes), scale);
        }
    }

    private static class DateColumnSerializer extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            out.writeLong(((java.sql.Date)obj).getTime());
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException {
            return new java.sql.Date(in.readLong());
        }
    }

    private static class TimeColumnSerializer extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            out.writeLong(((Time)obj).getTime());
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException {
            return new Time(in.readLong());
        }
    }

    static int DATE_NORMALIZER = 0;
    public final static long MIN_DATE_32;
    public final static long MAX_DATE_32;
    public final static long MIN_TIME_32;
    public final static long MAX_TIME_32;

    static {
        Calendar c = Calendar.getInstance();
        c.setTimeZone(TimeZone.getTimeZone("GMT")); //$NON-NLS-1$
        c.set(1900, 0, 1, 0, 0, 0);
        c.set(Calendar.MILLISECOND, 0);
        MIN_DATE_32 = c.getTimeInMillis();
        MAX_DATE_32 = MIN_DATE_32 + ((1L<<32)-1)*60000;
        DATE_NORMALIZER = -(int)(MIN_DATE_32/60000); //support a 32 bit range starting at this value
        MAX_TIME_32 = Integer.MAX_VALUE*1000L;
        MIN_TIME_32 = Integer.MIN_VALUE*1000L;
    }

    private static class DateColumnSerializer1 extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            long time = ((java.sql.Date)obj).getTime();
            if (time < MIN_DATE_32 || time > MAX_DATE_32) {
                throw new IOException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20029, obj.getClass().getName()));
            }
            out.writeInt((int)(time/60000) + DATE_NORMALIZER);
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException {
            return new java.sql.Date(((in.readInt()&0xffffffffL) - DATE_NORMALIZER)*60000);
        }
    }

    private static class TimeColumnSerializer1 extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            long time = ((Time)obj).getTime();
            if (time < MIN_TIME_32 || time > MAX_TIME_32) {
                throw new IOException(JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20029, obj.getClass().getName()));
            }
            out.writeInt((int)(time/1000));
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException {
            return new Time((in.readInt()&0xffffffffL)*1000);
        }
    }

    private static class TimestampColumnSerializer extends ColumnSerializer {
        @Override
        protected void writeObject(ObjectOutput out, Object obj, Map<Object, Integer> cache, byte version) throws IOException {
            Timestamp ts =  (Timestamp)obj;
            out.writeLong(ts.getTime());
            out.writeInt(ts.getNanos());
        }
        @Override
        protected Object readObject(ObjectInput in, List<Object> cache, byte version) throws IOException {
            Timestamp ts = new Timestamp(in.readLong());
            ts.setNanos(in.readInt());
            return ts;
        }
    }

    private static ColumnSerializer getSerializer(String type, byte version) {
        ColumnSerializer[] sers = serializers.get(type);
        if (sers == null) {
            if (DataTypeManager.isArrayType(type)) {
                if (version < 2) {
                    return arrayColumnSerializer;
                }
                return arrayColumnSerialier2;
            }
            return defaultSerializer;
        }
        return sers[Math.min(version, sers.length - 1)];
    }

    public static void writeBatch(ObjectOutput out, String[] types, List<? extends List<?>> batch) throws IOException {
        writeBatch(out, types, batch, CURRENT_VERSION);
    }

    public static void writeBatch(ObjectOutput out, String[] types, List<? extends List<?>> batch, byte version) throws IOException {
        if (batch == null) {
            out.writeInt(-1);
        } else {
            if (version > 0 && batch.size() > 0) {
                out.writeInt(-batch.size() -1);
                out.writeByte(version);
            } else {
                out.writeInt(batch.size());
            }
            if (batch.size() > 0) {
                int columns = types.length;
                out.writeInt(columns);
                Map<Object, Integer> cache = null;
                for(int i = 0; i < columns; i++) {
                    ColumnSerializer serializer = getSerializer(types[i], version);

                    if (cache == null && serializer.usesCache(version)) {
                        cache = new HashMap<Object, Integer>();
                    }
                    try {
                        serializer.writeColumn(out, i, batch, cache, version);
                    } catch (ClassCastException e) {
                        Object obj = null;
                        String objectClass = null;
                        objectSearch: for (int row = 0; row < batch.size(); row++) {
                            obj = batch.get(row).get(i);
                            if (obj != null) {
                                objectClass = obj.getClass().getName();
                                break objectSearch;
                            }
                        }
                         throw new TeiidRuntimeException(JDBCPlugin.Event.TEIID20001, e, JDBCPlugin.Util.gs(JDBCPlugin.Event.TEIID20001, new Object[] {types[i], new Integer(i), objectClass}));
                    }
                }
            }
        }
    }

    public static List<List<Object>> readBatch(ObjectInput in, String[] types) throws IOException, ClassNotFoundException {
        int rows = 0;
        try {
            rows = in.readInt();
        } catch (IOException e) {
            //7.4 compatibility
            if (types == null || types.length == 0) {
                List<Object>[] result = (List[])in.readObject();
                ArrayList<List<Object>> batch = new ArrayList<List<Object>>();
                batch.addAll(Arrays.asList(result));
                return batch;
            }
            throw e;
        }
        if (rows == 0) {
            return new ArrayList<List<Object>>(0);
        }
        if (rows == -1) {
            return null;
        }
        byte version = (byte)0;
        if (rows < 0) {
            rows = -(rows+1);
            version = in.readByte();
        }
        int columns = in.readInt();
        List<List<Object>> batch = new ResizingArrayList<List<Object>>(rows);
        int numBytes = rows/8;
        int extraRows = rows % 8;
        for (int currentRow = 0; currentRow < rows; currentRow++) {
            batch.add(currentRow, Arrays.asList(new Object[columns]));
        }
        byte[] isNullBuffer = new byte[(extraRows > 0) ? numBytes + 1: numBytes];
        List<Object> cache = null;
        for (int col = 0; col < columns; col++) {
            ColumnSerializer serializer = getSerializer(types[col], version);
            if (cache == null && serializer.usesCache(version)) {
                cache = new ArrayList<Object>();
            }
            serializer.readColumn(in, col, batch, isNullBuffer, cache, version);
        }
        return batch;
    }

    public static String getClientSafeType(String type,
            byte clientSerializationVersion) {
        if (clientSerializationVersion == CURRENT_VERSION) {
            return type;
        }
        if (DataTypeManager.isArrayType(type)) {
            return getClientSafeType(DataTypeManager.getComponentType(type), clientSerializationVersion) + DataTypeManager.ARRAY_SUFFIX;
        }
        if (clientSerializationVersion < BatchSerializer.VERSION_GEOMETRY && type.equals(DataTypeManager.DefaultDataTypes.GEOMETRY)) {
            return DataTypeManager.DefaultDataTypes.BLOB;
        }
        if (clientSerializationVersion < BatchSerializer.VERSION_GEOGRAPHY) {
            if (type.equals(DataTypeManager.DefaultDataTypes.GEOGRAPHY)) {
                return DataTypeManager.DefaultDataTypes.BLOB;
            }
            if (type.equals(DataTypeManager.DefaultDataTypes.JSON)) {
                return DataTypeManager.DefaultDataTypes.CLOB;
            }
        }
        return type;
    }
}
