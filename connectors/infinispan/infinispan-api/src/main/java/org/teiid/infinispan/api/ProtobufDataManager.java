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
package org.teiid.infinispan.api;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;

import org.infinispan.protostream.descriptors.Type;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.TransformationException;
import org.teiid.core.types.basic.ObjectToAnyTransform;
import org.teiid.core.util.ObjectConverterUtil;

import com.squareup.protoparser.DataType;
import com.squareup.protoparser.DataType.ScalarType;

public class ProtobufDataManager {

    private static HashMap<ScalarType, String> protoTypes = new HashMap<ScalarType, String>();
    private static HashMap<String, ScalarType> teiidTypes = new HashMap<String, ScalarType>();

    static {
        protoTypes.put(ScalarType.STRING, DataTypeManager.DefaultDataTypes.STRING);
        protoTypes.put(ScalarType.BOOL, DataTypeManager.DefaultDataTypes.BOOLEAN);

        protoTypes.put(ScalarType.FIXED32, DataTypeManager.DefaultDataTypes.INTEGER);
        protoTypes.put(ScalarType.SFIXED32, DataTypeManager.DefaultDataTypes.INTEGER);
        protoTypes.put(ScalarType.INT32, DataTypeManager.DefaultDataTypes.INTEGER);
        protoTypes.put(ScalarType.SINT32, DataTypeManager.DefaultDataTypes.INTEGER);
        protoTypes.put(ScalarType.UINT32, DataTypeManager.DefaultDataTypes.INTEGER);

        protoTypes.put(ScalarType.FIXED64, DataTypeManager.DefaultDataTypes.LONG);
        protoTypes.put(ScalarType.SFIXED64, DataTypeManager.DefaultDataTypes.LONG);
        protoTypes.put(ScalarType.INT64, DataTypeManager.DefaultDataTypes.LONG);
        protoTypes.put(ScalarType.SINT64, DataTypeManager.DefaultDataTypes.LONG);
        protoTypes.put(ScalarType.UINT64, DataTypeManager.DefaultDataTypes.LONG);

        protoTypes.put(ScalarType.FLOAT, DataTypeManager.DefaultDataTypes.FLOAT);
        protoTypes.put(ScalarType.DOUBLE, DataTypeManager.DefaultDataTypes.DOUBLE);
        protoTypes.put(ScalarType.BYTES, DataTypeManager.DefaultDataTypes.VARBINARY);

        teiidTypes.put(DataTypeManager.DefaultDataTypes.STRING, ScalarType.STRING);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BOOLEAN, ScalarType.BOOL);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.SHORT, ScalarType.INT32);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BYTE, ScalarType.INT32);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.SHORT, ScalarType.INT32);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.INTEGER, ScalarType.INT32);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.LONG, ScalarType.INT64);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.FLOAT, ScalarType.FLOAT);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.DOUBLE, ScalarType.DOUBLE);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BIG_DECIMAL, ScalarType.BYTES);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.DATE, ScalarType.STRING);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.TIME, ScalarType.STRING);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.TIMESTAMP, ScalarType.STRING);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.BLOB, ScalarType.BYTES);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.CLOB, ScalarType.BYTES);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.XML, ScalarType.BYTES);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.VARBINARY, ScalarType.BYTES);
        // will fail for most values
        teiidTypes.put(DataTypeManager.DefaultDataTypes.OBJECT, ScalarType.BYTES);
        teiidTypes.put(DataTypeManager.DefaultDataTypes.GEOMETRY, ScalarType.BYTES);
    }

    public static String teiidType(DataType protoType, boolean array, boolean isEnum) {
        // treat all enums as integers
        if (isEnum) {
            return DataTypeManager.DefaultDataTypes.INTEGER;
        }

        String type = protoTypes.get(protoType);
        if (type == null) {
            type = DataTypeManager.DefaultDataTypes.STRING; // special case for enum type
        }
        if (array) {
            type += "[]";
        }
        return type;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static Object convertToRuntime(Class expectedType, Object contents) throws IOException {

        if (contents == null || expectedType.isInstance(contents)) {
            return contents;
        }

        if (expectedType.isArray()) {
            expectedType = expectedType.getComponentType();
        }

        if (contents instanceof Long) {
            Long rawContents = (Long) contents;
            if (expectedType.isAssignableFrom(Date.class)) {
                return new Date(rawContents);
            } else if (expectedType.isAssignableFrom(Timestamp.class)) {
                return new Timestamp(rawContents);
            } else if (expectedType.isAssignableFrom(Time.class)) {
                return new Time(rawContents);
            }
        } else if (contents instanceof String) {

        } else if (contents instanceof byte[]) {
            byte[] rawContents = (byte[]) contents;
            if (expectedType.isAssignableFrom(String.class)) {
                try {
                    return new String(rawContents, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new IOException(e);
                }
            } else if (expectedType.isAssignableFrom(BigInteger.class)) {
                return new BigInteger(rawContents);
            } else if (expectedType.isAssignableFrom(BigDecimal.class)) {
                return new BigDecimal(new BigInteger(rawContents));
            } else if (expectedType.isAssignableFrom(Clob.class)) {
                return new ClobImpl(new InputStreamFactory() {
                    @Override
                    public InputStream getInputStream() throws IOException {
                        return new ByteArrayInputStream(rawContents);
                    }
                }, -1);
            } else if (expectedType.isAssignableFrom(Blob.class)) {
                return new BlobImpl(new InputStreamFactory() {
                    @Override
                    public InputStream getInputStream() throws IOException {
                        return new ByteArrayInputStream(rawContents);
                    }
                });
            } else if (expectedType.isAssignableFrom(SQLXML.class)) {
                return new SQLXMLImpl(rawContents);
            }
        } else {
            try {
                return ObjectToAnyTransform.INSTANCE.transform(contents, expectedType);
            } catch (TransformationException e) {
                throw new IOException(e);
            }
        }
        return contents;
    }

    public static <T> T convertToInfinispan(Class<T> expectedType, Object contents) throws IOException {
        if (contents == null || expectedType.isInstance(contents)) {
            return expectedType.cast(contents);
        }

        // date/time
        if (contents instanceof Date && expectedType.isAssignableFrom(Long.class)) {
            return expectedType.cast(((Date) contents).getTime());
        } else if (contents instanceof Timestamp && expectedType.isAssignableFrom(Long.class)) {
            return expectedType.cast(((Timestamp) contents).getTime());
        } else if (contents instanceof Time && expectedType.isAssignableFrom(Long.class)) {
            return expectedType.cast(((Time) contents).getTime());
        } else if (contents instanceof Date && expectedType.isAssignableFrom(byte[].class)) {
            long l = ((Date) contents).getTime();
            return expectedType.cast(java.nio.ByteBuffer.allocate(Long.SIZE / 8).putLong(l).array());
        } else if (contents instanceof Timestamp && expectedType.isAssignableFrom(byte[].class)) {
            long l = ((Timestamp) contents).getTime();
            return expectedType.cast(java.nio.ByteBuffer.allocate(Long.SIZE / 8).putLong(l).array());
        } else if (contents instanceof Time && expectedType.isAssignableFrom(byte[].class)) {
            long l = ((Time) contents).getTime();
            return expectedType.cast(java.nio.ByteBuffer.allocate(Long.SIZE / 8).putLong(l).array());
        } else if (contents instanceof String && expectedType.isAssignableFrom(byte[].class)) {
            return expectedType.cast(((String)contents).getBytes("UTF-8"));
        } else if (contents instanceof Clob && expectedType.isAssignableFrom(byte[].class)) {
            try {
                return expectedType.cast(ObjectConverterUtil.convertToByteArray(((Clob) contents).getAsciiStream()));
            } catch (SQLException e) {
                throw new IOException(e);
            }
        } else if (contents instanceof Blob && expectedType.isAssignableFrom(byte[].class)) {
            try {
                return expectedType.cast(ObjectConverterUtil.convertToByteArray(((Blob) contents).getBinaryStream(),
                        DataTypeManager.MAX_VARBINARY_BYTES, true));
            } catch (SQLException e) {
                throw new IOException(e);
            }
        } else if (contents instanceof SQLXML && expectedType.isAssignableFrom(byte[].class)) {
            try {
                return expectedType.cast(ObjectConverterUtil.convertToByteArray(((SQLXML) contents).getBinaryStream(),
                        DataTypeManager.MAX_VARBINARY_BYTES, true));
            } catch (SQLException e) {
                throw new IOException(e);
            }
        } else if (contents instanceof BigInteger && expectedType.isAssignableFrom(String.class)) {
            return expectedType.cast(((BigInteger) contents).toString());
        } else if (contents instanceof BigDecimal && expectedType.isAssignableFrom(String.class)) {
            return expectedType.cast(((BigDecimal) contents).toString());
        } else if (contents instanceof BigInteger && expectedType.isAssignableFrom(byte[].class)) {
            return expectedType.cast(((BigInteger) contents).toByteArray());
        } else if (contents instanceof BigDecimal && expectedType.isAssignableFrom(byte[].class)) {
            return expectedType.cast(((BigDecimal) contents).toBigInteger().toByteArray());
        }
        throw new IOException("unknown type to write:" + contents.getClass());
    }

    public static Type parseProtobufType(String name) {
        switch (name) {
        case "bool":
            return Type.BOOL;
        case "bytes":
            return Type.BYTES;
        case "double":
            return Type.DOUBLE;
        case "float":
            return Type.FLOAT;
        case "fixed32":
            return Type.FIXED32;
        case "fixed64":
            return Type.FIXED64;
        case "int32":
            return Type.INT32;
        case "int64":
            return Type.INT64;
        case "sfixed32":
            return Type.SFIXED32;
        case "sfixed64":
            return Type.SFIXED64;
        case "sint32":
            return Type.SINT32;
        case "sint64":
            return Type.SINT64;
        case "string":
            return Type.STRING;
        case "uint32":
            return Type.UINT32;
        case "uint64":
            return Type.UINT64;
        default:
            throw new TeiidRuntimeException("unrecognised type in metadata :" + name);
        }
    }

    public static Type getCompatibleProtobufType(Class<?> type) {
        if (type.isAssignableFrom(String.class) || type.isAssignableFrom(Character.class)) {
            return Type.STRING;
        } else if (type.isAssignableFrom(Integer.class) || type.isAssignableFrom(Short.class)
                || type.isAssignableFrom(Byte.class)) {
            return Type.INT32;
        } else if (type.isAssignableFrom(Long.class)) {
            return Type.INT64;
        } else if (type.isAssignableFrom(Boolean.class)) {
            return Type.BOOL;
        } else if (type.isAssignableFrom(Float.class)) {
            return Type.FLOAT;
        } else if (type.isAssignableFrom(Double.class)) {
            return Type.DOUBLE;
        }
        return Type.BYTES;
    }
}
