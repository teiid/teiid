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
package org.teiid.translator.accumulo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;

import org.apache.accumulo.core.client.lexicoder.BigIntegerLexicoder;
import org.apache.accumulo.core.client.lexicoder.BytesLexicoder;
import org.apache.accumulo.core.client.lexicoder.DateLexicoder;
import org.apache.accumulo.core.client.lexicoder.DoubleLexicoder;
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder;
import org.apache.accumulo.core.client.lexicoder.LongLexicoder;
import org.apache.accumulo.core.client.lexicoder.StringLexicoder;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.BinaryType;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.GeometryType;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.ObjectConverterUtil;

public class AccumuloDataTypeManager {
    public static byte[] EMPTY_BYTES = new byte[0];
    private static BytesLexicoder bytesLexicoder = new BytesLexicoder();
    private static BigIntegerLexicoder bigIntegerLexicoder = new BigIntegerLexicoder();
    private static DateLexicoder dateLexicoder = new DateLexicoder();
    private static DoubleLexicoder doubleLexicoder = new DoubleLexicoder();
    private static IntegerLexicoder integerLexicoder = new IntegerLexicoder();
    private static LongLexicoder longLexicoder = new LongLexicoder();
    private static StringLexicoder stringLexicoder = new StringLexicoder();
    private static Charset UTF_8 = Charset.forName("UTF-8");
    public static byte[] serialize(Object value) {
        if (value == null) {
            return EMPTY_BYTES;
        }

        try {
            if (value instanceof Clob) {
                // TODO:Accumulo streaming support would have been good?
                // this type materialization of the value is BAD
                Clob clob = (Clob)value;
                return ObjectConverterUtil.convertToByteArray(clob.getAsciiStream());
            } else if (value instanceof GeometryType) {
                GeometryType geometry = (GeometryType)value;
                //TODO: handle srid
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectConverterUtil.write(baos, geometry.getBinaryStream(), -1, true);
                int srid = geometry.getSrid();
                baos.write((srid >>> 24) & 0xFF);
                baos.write((srid >>> 16) & 0xFF);
                baos.write((srid >>>  8) & 0xFF);
                baos.write((srid >>>  0) & 0xFF);
                return baos.toByteArray();
            } else if (value instanceof Blob) {
                // TODO: same as CLOB
                Blob blob = (Blob)value;
                return ObjectConverterUtil.convertToByteArray(blob.getBinaryStream());
            } else if (value instanceof SQLXML) {
                // TODO: same as CLOB
                SQLXML xml = (SQLXML)value;
                return ObjectConverterUtil.convertToByteArray(xml.getBinaryStream());
            } else if (value instanceof BinaryType) {
                BinaryType binary = (BinaryType)value;
                return binary.getBytes();
            } else if (value instanceof byte[]) {
                return bytesLexicoder.encode((byte[])value);
            }
            else if (value instanceof Object[] ) {
                throw new TeiidRuntimeException(AccumuloPlugin.Event.TEIID19003,
                        AccumuloPlugin.Util.gs(AccumuloPlugin.Event.TEIID19003));
            }
            else if (value instanceof String ||
                        value instanceof Boolean ||
                        value instanceof Byte ||
                        value instanceof Short ||
                        value instanceof Character ||
                        value instanceof Integer ||
                        value instanceof Long ||
                        value instanceof BigInteger ||
                        value instanceof BigDecimal ||
                        value instanceof Float ||
                        value instanceof Double ||
                        value instanceof Date ||
                        value instanceof Time ||
                        value instanceof Timestamp) {
                return ((String)DataTypeManager.transformValue(value, String.class)).getBytes(UTF_8);
            }
            else {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos);
                oos.writeObject(value);
                oos.close();
                baos.close();
                return baos.toByteArray();
            }
        } catch (IOException e) {
            throw new TeiidRuntimeException(e);
        } catch (SQLException e) {
            throw new TeiidRuntimeException(e);
        } catch (TransformationException e) {
            throw new TeiidRuntimeException(e);
        }
    }

    public static Object deserialize(final byte[] value, final Class<?> expectedType) {
        if (value == null || Arrays.equals(value, EMPTY_BYTES)) {
            return null;
        }

        try {
            if (expectedType.isAssignableFrom(Clob.class)) {
                return new ClobImpl(new InputStreamFactory() {
                    @Override
                    public InputStream getInputStream() throws IOException {
                        return ObjectConverterUtil.convertToInputStream(value);
                    }
                }, -1);
            } else if (expectedType.isAssignableFrom(Blob.class)) {
                return new BlobType(new BlobImpl(new InputStreamFactory() {
                    @Override
                    public InputStream getInputStream() throws IOException {
                        return ObjectConverterUtil.convertToInputStream(value);
                    }
                }));
            } else if (expectedType.isAssignableFrom(SQLXML.class)) {
                return new SQLXMLImpl(new InputStreamFactory() {
                    @Override
                    public InputStream getInputStream() throws IOException {
                        return ObjectConverterUtil.convertToInputStream(value);
                    }
                });
            } else if (expectedType.isAssignableFrom(BinaryType.class)) {

                return new BinaryType(value);
            } else if (expectedType.isAssignableFrom(GeometryType.class)) {
                GeometryType result = new GeometryType(Arrays.copyOf(value, value.length -4));
                int srid = (((value[value.length - 4] & 0xff) << 24) +
                        ((value[value.length - 3] & 0xff) << 16) +
                        ((value[value.length - 2] & 0xff) << 8) +
                        ((value[value.length - 1] & 0xff) << 0));
                result.setSrid(srid);
                return result;
            } else if (expectedType.isAssignableFrom(byte[].class)) {
                return value;
            } else if (expectedType.isAssignableFrom(String.class)
                    || expectedType.isAssignableFrom(Boolean.class)
                    || expectedType.isAssignableFrom(Boolean.class)
                    || expectedType.isAssignableFrom(Byte.class)
                    || expectedType.isAssignableFrom(Short.class)
                    || expectedType.isAssignableFrom(Character.class)
                    || expectedType.isAssignableFrom(Integer.class)
                    || expectedType.isAssignableFrom(Long.class)
                    || expectedType.isAssignableFrom(BigInteger.class)
                    || expectedType.isAssignableFrom(BigDecimal.class)
                    || expectedType.isAssignableFrom(Float.class)
                    || expectedType.isAssignableFrom(Double.class)
                    || expectedType.isAssignableFrom(Date.class)
                    || expectedType.isAssignableFrom(Time.class)
                    || expectedType.isAssignableFrom(Timestamp.class)) {
                return DataTypeManager.transformValue(new String(value, UTF_8), expectedType);
            }
            else {
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(value));
                Object obj = ois.readObject();
                ois.close();
                return obj;
            }
        } catch (ClassNotFoundException e) {
            throw new TeiidRuntimeException(e);
        } catch (IOException e) {
            throw new TeiidRuntimeException(e);
        } catch (TransformationException e) {
            throw new TeiidRuntimeException(e);
        }
    }

    private static byte[] toLexiCode(Object value) {
        if (value == null) {
            return EMPTY_BYTES;
        }

        try {
            if (value instanceof java.sql.Date
                                || value instanceof java.sql.Timestamp
                                || value instanceof java.sql.Time) {
                return dateLexicoder.encode((java.util.Date)value);
            } else if (value instanceof Long) {
                return longLexicoder.encode((Long)value);
            } else if (value instanceof Double) {
                return doubleLexicoder.encode((Double)value);
            } else if (value instanceof Float) {
                return doubleLexicoder.encode(((Float)value).doubleValue());
            } else if (value instanceof Integer) {
                return integerLexicoder.encode((Integer)value);
            } else if (value instanceof BigInteger) {
                return bigIntegerLexicoder.encode((BigInteger)value);
            } else if (value instanceof BigDecimal) {
                return stringLexicoder.encode(((BigDecimal)value).toPlainString());
            } else if (value instanceof Byte) {
                return integerLexicoder.encode(((Byte)value).intValue());
            } else if (value instanceof Short) {
                return integerLexicoder.encode(((Short)value).intValue());
            } else if (value instanceof Clob) {
                // TODO:Accumulo streaming support would have been good?
                // this type materialization of the value is BAD
                Clob clob = (Clob)value;
                return bytesLexicoder.encode(ObjectConverterUtil.convertToByteArray(clob.getAsciiStream()));
            } else if (value instanceof Blob) {
                // TODO: same as CLOB
                Blob blob = (Blob)value;
                return bytesLexicoder.encode(ObjectConverterUtil.convertToByteArray(blob.getBinaryStream()));
            }  else if (value instanceof SQLXML) {
                // TODO: same as CLOB
                SQLXML xml = (SQLXML)value;
                return bytesLexicoder.encode(ObjectConverterUtil.convertToByteArray(xml.getBinaryStream()));
            } else if (value instanceof BinaryType) {
                BinaryType binary = (BinaryType)value;
                return bytesLexicoder.encode(binary.getBytes());
            } else if (value instanceof GeometryType) {
                GeometryType geometry = (GeometryType)value;
                return bytesLexicoder.encode(ObjectConverterUtil.convertToByteArray(geometry.getBinaryStream()));
            }  else if (value instanceof byte[]) {
                return bytesLexicoder.encode((byte[])value);
            }
            else if (value instanceof Object[] ) {
                throw new TeiidRuntimeException(AccumuloPlugin.Event.TEIID19003, AccumuloPlugin.Util.gs(AccumuloPlugin.Event.TEIID19003));
            }
            return stringLexicoder.encode(((String)DataTypeManager.transformValue(value, String.class)));
        } catch (TransformationException e) {
            throw new TeiidRuntimeException(e);
        } catch (SQLException e) {
            throw new TeiidRuntimeException(e);
        } catch (IOException e) {
            throw new TeiidRuntimeException(e);
        }
    }

    private static Object fromLexiCode(final byte[] value, final Class<?> expectedType) {
        if (value == null || Arrays.equals(value, EMPTY_BYTES)) {
            return null;
        }

        if (expectedType.isAssignableFrom(String.class)) {
            return stringLexicoder.decode(value);
        } else if (expectedType.isAssignableFrom(java.sql.Date.class)) {
            return new java.sql.Date(dateLexicoder.decode(value).getTime());
        } else if (expectedType.isAssignableFrom(java.sql.Timestamp.class)) {
            return new java.sql.Timestamp(dateLexicoder.decode(value).getTime());
        } else if (expectedType.isAssignableFrom(java.sql.Time.class)) {
            return new java.sql.Time(dateLexicoder.decode(value).getTime());
        } else if (expectedType.isAssignableFrom(Long.class)) {
            return longLexicoder.decode(value);
        } else if (expectedType.isAssignableFrom( Double.class)) {
            return doubleLexicoder.decode(value);
        } else if (expectedType.isAssignableFrom(Float.class)) {
            return doubleLexicoder.decode(value).floatValue();
        } else if (expectedType.isAssignableFrom(Integer.class)) {
            return integerLexicoder.decode(value);
        } else if (expectedType.isAssignableFrom(BigInteger.class)) {
            return bigIntegerLexicoder.decode(value);
        } else if (expectedType.isAssignableFrom(BigDecimal.class)) {
            return new BigDecimal(stringLexicoder.decode(value));
        } else if (expectedType.isAssignableFrom(Byte.class)) {
            return integerLexicoder.decode(value).byteValue();
        } else if (expectedType.isAssignableFrom(Short.class)) {
            return integerLexicoder.decode(value).shortValue();
        } else if (expectedType.isAssignableFrom(Clob.class)) {
            return new ClobImpl(new InputStreamFactory() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return ObjectConverterUtil.convertToInputStream(bytesLexicoder.decode(value));
                }
            }, -1);
        } else if (expectedType.isAssignableFrom(Blob.class)) {
            return new BlobType(new BlobImpl(new InputStreamFactory() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return ObjectConverterUtil.convertToInputStream(bytesLexicoder.decode(value));
                }

            }));
        } else if (expectedType.isAssignableFrom(SQLXML.class)) {
            return new SQLXMLImpl(new InputStreamFactory() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return ObjectConverterUtil.convertToInputStream(bytesLexicoder.decode(value));
                }
            });
        } else if (expectedType.isAssignableFrom(BinaryType.class)) {
            return new BinaryType(bytesLexicoder.decode(value));
        } else if (expectedType.isAssignableFrom(GeometryType.class)) {
            return new GeometryType(bytesLexicoder.decode(value));
        } else if (expectedType.isAssignableFrom(byte[].class)) {
            return bytesLexicoder.decode(value);
        }
        else {
            throw new TeiidRuntimeException(AccumuloPlugin.Event.TEIID19004,
                    AccumuloPlugin.Util.gs(AccumuloPlugin.Event.TEIID19004,
                            expectedType.getName()));
        }
    }
}
