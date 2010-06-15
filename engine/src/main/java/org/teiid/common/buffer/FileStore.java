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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import org.teiid.core.TeiidComponentException;

public abstract class FileStore {
	
	private static ReferenceQueue<Object> QUEUE = new ReferenceQueue<Object>();
	private static final Set<PhantomReference<Object>> REFERENCES = Collections.newSetFromMap(new IdentityHashMap<PhantomReference<Object>, Boolean>());
	
	/**
	 * A customized buffered stream with an exposed buffer
	 */
	public final class FileStoreOutputStream extends OutputStream {
		
		private byte[] buffer;
		private int count;
		private boolean bytesWritten;
		
		public FileStoreOutputStream(int size) {
			this.buffer = new byte[size];
		}
		
		@Override
		public void write(int b) throws IOException {
			write(new byte[b], 0, 1);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			if (len > buffer.length) {
				flushBuffer();
				writeDirect(b, off, len);
				return;
			}
			int bufferedLength = Math.min(len, buffer.length - count);
			if (count < buffer.length) {
				System.arraycopy(b, off, buffer, count, bufferedLength);
				count += bufferedLength;
				if (bufferedLength == len) {
					return;
				}
			}
			flushBuffer();
			System.arraycopy(b, off + bufferedLength, buffer, count, len - bufferedLength);
			count += len - bufferedLength;
		}

		private void writeDirect(byte[] b, int off, int len) throws IOException {
			try {
				FileStore.this.write(b, off, len);
				bytesWritten = true;
			} catch (TeiidComponentException e) {
				throw new IOException(e);
			}
		}

		public void flushBuffer() throws IOException {
			if (count > 0) {
				writeDirect(buffer, 0, count);
				count = 0;
			}
		}
		
		public byte[] getBuffer() {
			return buffer;
		}
		
		public int getCount() {
			return count;
		}
		
		public boolean bytesWritten() {
			return bytesWritten;
		}
		
		@Override
		public void close() throws IOException {
			if (bytesWritten) {
				flushBuffer();
			}
		}
		
	}

	static class CleanupReference extends PhantomReference<Object> {
		
		private FileStore store;
		
		public CleanupReference(Object referent, FileStore store) {
			super(referent, QUEUE);
			this.store = store;
		}
		
		public void cleanup() {
			try {
				this.store.remove();
			} finally {
				this.clear();
			}
		}
	}
	
	private boolean removed;
	private long len;
	
	public void setCleanupReference(Object o) {
		REFERENCES.add(new CleanupReference(o, this));
		for (int i = 0; i < 10; i++) {
			CleanupReference ref = (CleanupReference)QUEUE.poll();
			if (ref == null) {
				break;
			}
			ref.cleanup();
			REFERENCES.remove(ref);
		}
	}
	
	public synchronized long getLength() {
		return len;
	}
		
	public int read(long fileOffset, byte[] b, int offSet, int length)
			throws TeiidComponentException {
		if (removed) {
			throw new TeiidComponentException("already removed"); //$NON-NLS-1$
		}
		return readDirect(fileOffset, b, offSet, length);
	}
	
	protected abstract int readDirect(long fileOffset, byte[] b, int offSet, int length)
			throws TeiidComponentException;

	public void readFully(long fileOffset, byte[] b, int offSet, int length) throws TeiidComponentException {
        int n = 0;
    	do {
    	    int count = this.read(fileOffset + n, b, offSet + n, length - n);
    	    if (count < 0) {
    	    	throw new TeiidComponentException("not enough bytes available"); //$NON-NLS-1$
    	    }
    	    n += count;
    	} while (n < length);
	}
	
	public void write(byte[] bytes) throws TeiidComponentException {
		write(bytes, 0, bytes.length);
	}

	public synchronized long write(byte[] bytes, int offset, int length) throws TeiidComponentException {
		if (removed) {
			throw new TeiidComponentException("already removed"); //$NON-NLS-1$
		}
		writeDirect(bytes, offset, length);
		long result = len;
		len += length;		
		return result;
	}

	protected abstract void writeDirect(byte[] bytes, int offset, int length) throws TeiidComponentException;

	public void remove() {
		if (!this.removed) {
			this.removed = true;
			this.removeDirect();
		}
	}
	
	protected abstract void removeDirect();
	
	public InputStream createInputStream(final long start) {
		return new InputStream() {
			private long offset = start;
			
			@Override
			public int read() throws IOException {
				throw new UnsupportedOperationException("buffered reading must be used"); //$NON-NLS-1$
			}
			
			@Override
			public int read(byte[] b, int off, int len) throws IOException {
				try {
					int bytes = FileStore.this.read(offset, b, off, len);
					if (bytes != -1) {
						this.offset += bytes;
					}
					return bytes;
				} catch (TeiidComponentException e) {
					throw new IOException(e);
				}
			}
		};
	}
	
	public OutputStream createOutputStream() {
		return new OutputStream() {
			
			@Override
			public void write(int b) throws IOException {
				throw new UnsupportedOperationException("buffered reading must be used"); //$NON-NLS-1$
			}
			
			@Override
			public void write(byte[] b, int off, int len) throws IOException {
				try {
					FileStore.this.write(b, off, len);
				} catch (TeiidComponentException e) {
					throw new IOException(e);
				}
			}
		};
	}
	
	public  FileStoreOutputStream createOutputStream(int maxMemorySize) {
		return new FileStoreOutputStream(maxMemorySize);
	}
	
}