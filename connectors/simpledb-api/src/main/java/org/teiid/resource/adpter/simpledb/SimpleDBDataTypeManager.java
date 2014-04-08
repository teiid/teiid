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
package org.teiid.resource.adpter.simpledb;

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
