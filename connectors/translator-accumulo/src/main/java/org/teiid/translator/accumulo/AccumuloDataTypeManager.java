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
package org.teiid.translator.accumulo;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Blob;
import java.util.Arrays;

import org.teiid.core.TeiidException;
import org.teiid.core.TeiidRuntimeException;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.TransformationException;
import org.teiid.core.util.ObjectConverterUtil;

public class AccumuloDataTypeManager {
	public static byte[] EMPTY_BYTES = new byte[0];
	
	public static byte[] convertToAccumuloType(Object value, Charset encoding) {
		if (value == null) {
			return EMPTY_BYTES;
		}
		
		try {
			if (value instanceof Blob) {
				// TODO:Accumulo streaming support would have been good?
				// this type materialization of the value is BAD
				return ObjectConverterUtil.convertToByteArray(value);
			}
			else if (value instanceof Object[] ) {
				throw new TeiidRuntimeException(AccumuloPlugin.Event.TEIID19003, AccumuloPlugin.Util.gs(AccumuloPlugin.Event.TEIID19003));
			}
			return ((String)DataTypeManager.transformValue(value, String.class)).getBytes(encoding);
		} catch (TransformationException e) {
			throw new TeiidRuntimeException(e);
		} catch (TeiidException e) {
			throw new TeiidRuntimeException(e);
		} catch (IOException e) {
			throw new TeiidRuntimeException(e);
		}
	}
		
	public static Object convertFromAccumuloType(final byte[] value, final Class<?> expectedType, final Charset encoding) {
		if (value == null || Arrays.equals(value, EMPTY_BYTES)) {
			return null;
		}
		
		if (expectedType.isAssignableFrom(String.class)) {
			return new String(value, encoding);
		}
		
		try {
			if (expectedType.isAssignableFrom(Blob.class)) {
				return new BlobType(new BlobImpl(new InputStreamFactory() {
					@Override
					public InputStream getInputStream() throws IOException {
						return new ByteArrayInputStream(value);
					}
					
				}));
			}
			else if (DataTypeManager.isTransformable(String.class, expectedType)) {
				return DataTypeManager.transformValue(new String(value, encoding), expectedType);
			}
			else {
				throw new TeiidRuntimeException(AccumuloPlugin.Event.TEIID19004, AccumuloPlugin.Util.gs(AccumuloPlugin.Event.TEIID19004, expectedType.getName()));
			}
		} catch (TransformationException e) {
			throw new TeiidRuntimeException(e);
		}
	}	
}
