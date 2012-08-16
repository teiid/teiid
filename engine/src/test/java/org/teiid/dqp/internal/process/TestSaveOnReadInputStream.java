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

package org.teiid.dqp.internal.process;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.Test;
import org.teiid.common.buffer.BufferManagerFactory;
import org.teiid.common.buffer.FileStore;
import org.teiid.common.buffer.FileStoreInputStreamFactory;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.Streamable;
import org.teiid.core.types.InputStreamFactory.StorageMode;
import org.teiid.core.util.ObjectConverterUtil;

@SuppressWarnings("nls")
public class TestSaveOnReadInputStream {
	
	@Test public void testSave() throws IOException {
		SaveOnReadInputStream soris = getSaveOnReadInputStream();
		InputStreamFactory isf = soris.getInputStreamFactory();
		InputStream is = isf.getInputStream();
		assertEquals("hello world", new String(ObjectConverterUtil.convertToByteArray(is), Streamable.CHARSET));
		InputStream is2 = isf.getInputStream(); 
		assertEquals("hello world", new String(ObjectConverterUtil.convertToByteArray(is2), Streamable.CHARSET));
	}

	@Test public void testPartialReadSave() throws IOException {
		SaveOnReadInputStream soris = getSaveOnReadInputStream();
		InputStreamFactory isf = soris.getInputStreamFactory();
		InputStream is = isf.getInputStream();
		is.read();
		
		InputStream is2 = isf.getInputStream(); 
		assertEquals("ello world", new String(ObjectConverterUtil.convertToByteArray(is), Streamable.CHARSET));
		assertEquals("hello world", new String(ObjectConverterUtil.convertToByteArray(is2), Streamable.CHARSET));
		InputStream is3 = isf.getInputStream(); 
		assertEquals("hello world", new String(ObjectConverterUtil.convertToByteArray(is3), Streamable.CHARSET));
	}
	
	@Test public void testStorageMode() throws IOException {
		SaveOnReadInputStream soris = getSaveOnReadInputStream();
		InputStreamFactory isf = soris.getInputStreamFactory();
		
		assertEquals(StorageMode.MEMORY, isf.getStorageMode());
		
		InputStream is = isf.getInputStream();
		assertEquals("hello world", new String(ObjectConverterUtil.convertToByteArray(is), Streamable.CHARSET));
	}

	private SaveOnReadInputStream getSaveOnReadInputStream() {
		FileStore fs = BufferManagerFactory.getStandaloneBufferManager().createFileStore("test");
		FileStoreInputStreamFactory factory = new FileStoreInputStreamFactory(fs, Streamable.ENCODING);
		
		InputStream is = new ByteArrayInputStream("hello world".getBytes(Streamable.CHARSET));
		
		SaveOnReadInputStream soris = new SaveOnReadInputStream(is, factory);
		return soris;
	}

}
