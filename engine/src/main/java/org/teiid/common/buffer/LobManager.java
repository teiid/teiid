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
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.rowset.serial.SerialBlob;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.BlobImpl;
import org.teiid.core.types.BlobType;
import org.teiid.core.types.ClobImpl;
import org.teiid.core.types.ClobType;
import org.teiid.core.types.DataTypeManager;
import org.teiid.core.types.InputStreamFactory;
import org.teiid.core.types.SQLXMLImpl;
import org.teiid.core.types.Streamable;
import org.teiid.core.types.XMLType;
import org.teiid.core.types.InputStreamFactory.BlobInputStreamFactory;
import org.teiid.core.types.InputStreamFactory.ClobInputStreamFactory;
import org.teiid.core.types.InputStreamFactory.SQLXMLInputStreamFactory;
import org.teiid.core.types.InputStreamFactory.StorageMode;
import org.teiid.core.util.ObjectConverterUtil;
import org.teiid.query.QueryPlugin;
import org.teiid.query.sql.symbol.Expression;

/**
 * Tracks lob references so they are not lost during serialization.
 * TODO: for temp tables we may need to have a copy by value management strategy
 */
public class LobManager {
	
	public enum ReferenceMode {
		ATTACH,
		CREATE,
		REMOVE
	}
	
	private static class LobHolder {
		Streamable<?> lob;
		int referenceCount = 1;

		public LobHolder(Streamable<?> lob) {
			this.lob = lob;
		}
	}
	
	private Map<String, LobHolder> lobReferences = Collections.synchronizedMap(new HashMap<String, LobHolder>());
	private boolean inlineLobs = true;
	private int maxMemoryBytes = DataTypeManager.MAX_LOB_MEMORY_BYTES;
	private int[] lobIndexes;
	private FileStore lobStore;
	
	public LobManager(int[] lobIndexes, FileStore lobStore) {
		this.lobIndexes = lobIndexes;
		this.lobStore = lobStore;
	}
	
	public LobManager clone() {
		LobManager clone = new LobManager(lobIndexes, null);
		clone.inlineLobs = inlineLobs;
		clone.maxMemoryBytes = maxMemoryBytes;
		synchronized (lobReferences) {
			for (Map.Entry<String, LobHolder> entry : lobReferences.entrySet()) {
				LobHolder lobHolder = new LobHolder(entry.getValue().lob);
				lobHolder.referenceCount = entry.getValue().referenceCount;
				clone.lobReferences.put(entry.getKey(), lobHolder);
			}
		}
		return clone;
	}
	
	public void setInlineLobs(boolean trackMemoryLobs) {
		this.inlineLobs = trackMemoryLobs;
	}
	
	public void setMaxMemoryBytes(int maxMemoryBytes) {
		this.maxMemoryBytes = maxMemoryBytes;
	}
	
	@SuppressWarnings("unchecked")
	public void updateReferences(List<?> tuple, ReferenceMode mode)
			throws TeiidComponentException {
		for (int i = 0; i < lobIndexes.length; i++) {
			Object anObj = tuple.get(lobIndexes[i]);
			if (!(anObj instanceof Streamable<?>)) {
				continue;
			}
			Streamable lob = (Streamable) anObj;
			try {
				if (lob.getReferenceStreamId() == null || (inlineLobs 
						&& (InputStreamFactory.getStorageMode(lob) == StorageMode.MEMORY
						|| lob.length()*(lob instanceof ClobType?2:1) <= maxMemoryBytes))) {
					lob.setReferenceStreamId(null);
					continue;
				}
			} catch (SQLException e) {
				//presumably the lob is bad, but let it slide for now
			}
			String id = lob.getReferenceStreamId();
			LobHolder lobHolder = this.lobReferences.get(id);
			switch (mode) {
			case REMOVE:
				if (lobHolder != null) {
					lobHolder.referenceCount--;
					if (lobHolder.referenceCount < 1) {
						this.lobReferences.remove(id);
					}
				}
				break;
			case ATTACH:
				if (lob.getReference() == null) {
					if (lobHolder == null) {
						throw new TeiidComponentException(QueryPlugin.Util.getString("ProcessWorker.wrongdata")); //$NON-NLS-1$
					}
					lob.setReference(lobHolder.lob.getReference());
				}
				break;
			case CREATE:
				if (lob.getReference() == null) {
					throw new TeiidComponentException(QueryPlugin.Util.getString("ProcessWorker.wrongdata")); //$NON-NLS-1$					
				}
				if (lobHolder == null) {
					this.lobReferences.put(id, new LobHolder(lob));					
				} else {
					lobHolder.referenceCount++;
				}
			}
		}
	}
	
    public Streamable<?> getLobReference(String id) throws TeiidComponentException {
    	LobHolder lob = this.lobReferences.get(id);
    	if (lob == null) {
    		throw new TeiidComponentException(QueryPlugin.Util.getString("ProcessWorker.wrongdata")); //$NON-NLS-1$
    	}
    	return lob.lob;
    }
        
    public static int[] getLobIndexes(List<? extends Expression> expressions) {
    	if (expressions == null) {
    		return null;
    	}
		int[] result = new int[expressions.size()];
		int resultIndex = 0;
	    for (int i = 0; i < expressions.size(); i++) {
	    	Expression expr = expressions.get(i);
	        if (DataTypeManager.isLOB(expr.getType()) || expr.getType() == DataTypeManager.DefaultDataClasses.OBJECT) {
	        	result[resultIndex++] = i;
	        }
	    }
	    if (resultIndex == 0) {
	    	return null;
	    }
	    return Arrays.copyOf(result, resultIndex);
    }

	public void persist() throws TeiidComponentException {
		// stream the contents of lob into file store.
		byte[] bytes = new byte[1 << 14]; 

		for (Map.Entry<String, LobHolder> entry : this.lobReferences.entrySet()) {
			entry.getValue().lob = detachLob(entry.getValue().lob, lobStore, bytes);
		}
	}    
    
	public Streamable<?> detachLob(final Streamable<?> lob, final FileStore store, byte[] bytes) throws TeiidComponentException {
		// if this is not attached, just return
		if (InputStreamFactory.getStorageMode(lob) != StorageMode.OTHER) {
			return lob;
		}
		
		return persistLob(lob, store, bytes, inlineLobs, maxMemoryBytes);
	}

	public static Streamable<?> persistLob(final Streamable<?> lob,
			final FileStore store, byte[] bytes, boolean inlineLobs, int maxMemoryBytes) throws TeiidComponentException {
		try {
			//inline
			long byteLength = lob.length()*(lob instanceof ClobType?2:1);
			if (lob.getReferenceStreamId() == null || (inlineLobs 
					&& (byteLength <= maxMemoryBytes))) {
				lob.setReferenceStreamId(null);
				if (InputStreamFactory.getStorageMode(lob) == StorageMode.MEMORY) {
					return lob;
				}
				
				if (lob instanceof BlobType) {
					BlobType b = (BlobType)lob;
					byte[] blobBytes = b.getBytes(1, (int)byteLength);
					b.setReference(new SerialBlob(blobBytes));
					return b;
				} else if (lob instanceof ClobType) {
					ClobType c = (ClobType)lob;
					String s = c.getSubString(1, (int)(byteLength>>>1));
					c.setReference(ClobImpl.createClob(s.toCharArray()));
					return c;
				} else {
					XMLType x = (XMLType)lob;
					String s = x.getString();
					x.setReference(new SQLXMLImpl(s));
					return x;
				}
			}
			
			InputStream is = null;
	    	if (lob instanceof BlobType) {
	    		is = new BlobInputStreamFactory((Blob)lob).getInputStream();
	    	} else if (lob instanceof ClobType) {
	    		is = new ClobInputStreamFactory((Clob)lob).getInputStream();
	    	} else {
	    		is = new SQLXMLInputStreamFactory((SQLXML)lob).getInputStream();
	    	}

			long offset = store.getLength();
			Streamable<?> persistedLob;
						
			OutputStream fsos = store.createOutputStream();
			byteLength = ObjectConverterUtil.write(fsos, is, bytes, -1);
			
			// re-construct the new lobs based on the file store
			final long lobOffset = offset;
			final long lobLength = byteLength;
			InputStreamFactory isf = new InputStreamFactory() {
				@Override
				public InputStream getInputStream() throws IOException {
					return store.createInputStream(lobOffset, lobLength);
				}
				
				@Override
				public StorageMode getStorageMode() {
					return StorageMode.PERSISTENT;
				}
			};			
		
			if (lob instanceof BlobType) {
				persistedLob = new BlobType(new BlobImpl(isf));
			}
			else if (lob instanceof ClobType) {
				persistedLob = new ClobType(new ClobImpl(isf, ((ClobType)lob).length()));
			}
			else {
				persistedLob = new XMLType(new SQLXMLImpl(isf));
				((XMLType)persistedLob).setEncoding(((XMLType)lob).getEncoding());
				((XMLType)persistedLob).setType(((XMLType)lob).getType());
			}
			return persistedLob;		
		} catch (SQLException e) {
			throw new TeiidComponentException(e);
		} catch (IOException e) {
			throw new TeiidComponentException(e);
		}
	}
	
	public int getLobCount() {
		return this.lobReferences.size();
	}

	public void remove() {
		this.lobReferences.clear();
	}
}
