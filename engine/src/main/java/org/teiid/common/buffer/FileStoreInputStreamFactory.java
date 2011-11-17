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
 
package org.teiid.common.buffer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.Charset;

import org.teiid.common.buffer.FileStore.FileStoreOutputStream;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.util.InputStreamReader;

public final class FileStoreInputStreamFactory extends InputStreamFactory {
	private final FileStore lobBuffer;
	private FileStoreOutputStream fsos;
	private String encoding;

	public FileStoreInputStreamFactory(FileStore lobBuffer, String encoding) {
		this.encoding = encoding;
		this.lobBuffer = lobBuffer;
		AutoCleanupUtil.setCleanupReference(this, lobBuffer);
	}

	@Override
	public InputStream getInputStream() throws IOException {
		if (fsos != null && !fsos.bytesWritten()) {
			return new ByteArrayInputStream(fsos.getBuffer(), 0, fsos.getCount());
		}
		return lobBuffer.createInputStream(0);
	}
	
	@Override
	public Reader getCharacterStream() throws IOException {
		return new InputStreamReader(getInputStream(), Charset.forName(encoding).newDecoder());
	}

	@Override
	public long getLength() {
		if (fsos != null && !fsos.bytesWritten()) {
			return fsos.getCount();
		}
		return lobBuffer.getLength();
	}

	/**
	 * Returns a new writer instance that is backed by the shared output stream.
	 * Closing a writer will prevent further writes.
	 * @return
	 */
	public Writer getWriter() {
		return new OutputStreamWriter(getOuputStream(), Charset.forName(encoding));
	}
	
	/**
	 * The returned output stream is shared among all uses.
	 * Once closed no further writing can occur
	 * @return
	 */
	public FileStoreOutputStream getOuputStream() {
		if (fsos == null) {
			fsos = lobBuffer.createOutputStream(DataTypeManager.MAX_LOB_MEMORY_BYTES);
		}
		return fsos;
	}

	@Override
	public void free() throws IOException {
		lobBuffer.remove();
	}
	
	@Override
	public StorageMode getStorageMode() {
		if (fsos == null) {
			return StorageMode.PERSISTENT;
		}
		return StorageMode.MEMORY;
	}
}