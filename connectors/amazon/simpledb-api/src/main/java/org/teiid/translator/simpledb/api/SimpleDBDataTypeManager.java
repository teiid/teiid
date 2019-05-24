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
package org.teiid.translator.simpledb.api;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLXML;
import java.util.List;

import org.teiid.core.TeiidException;
import org.teiid.core.types.*;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.translator.TranslatorException;

public class SimpleDBDataTypeManager {
    public static Object convertToSimpleDBType (Object value, Class<?> type) throws TranslatorException {
        if (value == null) {
            return null;
        }
        if (type.isArray()) {
            int length = Array.getLength(value);
            String[] arrayValue = new String[length];
            for (int i = 0; i < length; i++) {
                arrayValue[i] = convertToSimpleDBType(Array.get(value, i));
            }
            return arrayValue;
        }

        return convertToSimpleDBType(value);
    }

    private static String convertToSimpleDBType(Object value) throws TranslatorException {
        try {
            if (value instanceof Blob) {
                return new String(ObjectConverterUtil.convertToByteArray(value), "UTF-8"); //$NON-NLS-1$
            }
            return ((String)DataTypeManager.transformValue(value, String.class));
        } catch (TransformationException e) {
            throw new TranslatorException(e);
        } catch (TeiidException e) {
            throw new TranslatorException(e);
        } catch (IOException e) {
            throw new TranslatorException(e);
        }
    }

    public static Object convertFromSimpleDBType(final List<String> origialValue, final Class<?> expectedType) throws TranslatorException {
        if (origialValue == null) {
            return null;
        }

        if (expectedType.isArray()) {
            if (expectedType.getComponentType().isAssignableFrom(String.class)) {
                return origialValue.toArray(new String[origialValue.size()]);
            }
            Object array = Array.newInstance(expectedType, origialValue.size());
            for (int i = 0; i < origialValue.size(); i++) {
                Object arrayItem = convertFromSimpleDBType(origialValue.get(i), expectedType.getComponentType());
                Array.set(array, i, arrayItem);
            }
            return array;
        }

        final String value = origialValue.get(0);
        return convertFromSimpleDBType(value, expectedType);
    }

    private static Object convertFromSimpleDBType(final String value, final Class<?> expectedType) throws TranslatorException {

        if (expectedType.isAssignableFrom(String.class)) {
            return value;
        }

        if (expectedType.isAssignableFrom(Blob.class)) {
            return new BlobType(new BlobImpl(new InputStreamFactory() {
                @Override
                public InputStream getInputStream() throws IOException {
                    return new ByteArrayInputStream(value.getBytes());
                }

            }));
        } else if (expectedType.isAssignableFrom(Clob.class)) {
            return new ClobType(new ClobImpl(value));
        } else if (expectedType.isAssignableFrom(SQLXML.class)) {
            return new XMLType(new SQLXMLImpl(value.getBytes()));
        } else if (DataTypeManager.isTransformable(String.class, expectedType)) {
            try {
                return DataTypeManager.transformValue(value, expectedType);
            } catch (TransformationException e) {
                throw new TranslatorException(e);
            }
        } else {
            throw new TranslatorException("Failed to convert "+ value +" to target type of "+ expectedType);//$NON-NLS-1$ //$NON-NLS-2$
        }
    }
}
