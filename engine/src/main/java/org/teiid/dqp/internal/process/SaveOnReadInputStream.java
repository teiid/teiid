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

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.teiid.common.buffer.FileStoreInputStreamFactory;
import org.teiid.common.buffer.FileStore.FileStoreOutputStream;
import org.teiid.core.types.InputStreamFactory;

/**
 * An {@link InputStream} wrapper that saves the input on read and provides a {@link InputStreamFactory}.
 */
final class SaveOnReadInputStream extends FilterInputStream {
	
	class SwitchingInputStream extends FilterInputStream {

		protected SwitchingInputStream() {
			super(SaveOnReadInputStream.this);
		}
		
		public void setIn(InputStream is) {
			this.in = is;
		}
		
	}
	
	private SwitchingInputStream sis = new SwitchingInputStream();
	private final FileStoreInputStreamFactory fsisf;
	private FileStoreOutputStream fsos;
	private boolean saved;
	private boolean read;
	private boolean returned;

	InputStreamFactory inputStreamFactory = new InputStreamFactory() {
		
		@Override
		public InputStream getInputStream() throws IOException {
			if (!saved) {
				if (!returned) {
					returned = true;
					return sis;
				}
				//save the rest of the stream
				SaveOnReadInputStream.this.fsos.flush();
				long start = SaveOnReadInputStream.this.fsisf.getLength();
				SaveOnReadInputStream.this.close(); //force the pending read
				InputStream is = SaveOnReadInputStream.this.fsisf.getInputStream(start);
				sis.setIn(is);
			}
			return fsisf.getInputStream();
		}
		
		@Override
		public StorageMode getStorageMode() {
			if (!saved) {
				try {
					getInputStream().close();
				} catch (IOException e) {
					return StorageMode.OTHER;
				}
			}
			return fsisf.getStorageMode();
		}
	};

	SaveOnReadInputStream(InputStream in,
			FileStoreInputStreamFactory fsisf) {
		super(in);
		this.fsisf = fsisf;
		fsos = fsisf.getOuputStream();
	}

	@Override
	public int read() throws IOException {
		read = true;
		int i = super.read();
		read = false;
		if (i > 0) {
			fsos.write(i);
		} else {
			saved = true;
		}
		return i;
	}

	@Override
	public int read(byte[] b, int off, int len)
			throws IOException {
		read = true;
		int bytes = super.read(b, off, len);
		read = false;
		if (bytes > 0) {
			fsos.write(b, off, bytes);
		} else if (bytes == -1) {
			saved = true;
		}
		return bytes;
	}

	@Override
	public void close() throws IOException {
		try {
			if (!saved && !read) {
				byte[] bytes = new byte[1<<13];
				while (!saved) {
					read(bytes, 0, bytes.length);
				}
			}
			fsos.close();
		} finally {
			if (!saved) {
				fsisf.free();
				saved = true;
			}
			super.close();
		}
	}
	
	InputStreamFactory getInputStreamFactory() {
		return inputStreamFactory;
	}
}