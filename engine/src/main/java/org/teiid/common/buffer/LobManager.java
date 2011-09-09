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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.teiid.core.TeiidComponentException;
import org.teiid.core.types.BaseLob;
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
	private Map<String, Streamable<?>> lobReferences = new ConcurrentHashMap<String, Streamable<?>>();
	private boolean inlineLobs = true;
	private int maxMemoryBytes = DataTypeManager.MAX_LOB_MEMORY_BYTES;
	
	public void setInlineLobs(boolean trackMemoryLobs) {
		this.inlineLobs = trackMemoryLobs;
	}
	
	public void setMaxMemoryBytes(int maxMemoryBytes) {
		this.maxMemoryBytes = maxMemoryBytes;
	}

	public void updateReferences(int[] lobIndexes, List<?> tuple)
			throws TeiidComponentException {
		for (int i = 0; i < lobIndexes.length; i++) {
			Object anObj = tuple.get(lobIndexes[i]);
			if (!(anObj instanceof Streamable<?>)) {
				continue;
			}
			Streamable lob = (Streamable) anObj;
			try {
				if (inlineLobs 
						&& (InputStreamFactory.getStorageMode(lob) == StorageMode.MEMORY
						|| lob.length()*(lob instanceof ClobType?2:1) <= maxMemoryBytes)) {
					lob.setReferenceStreamId(null);
					continue;
				}
			} catch (SQLException e) {
				//presumably the lob is bad, but let it slide for now
			}
			if (lob.getReference() == null) {
				lob.setReference(getLobReference(lob.getReferenceStreamId()).getReference());
			} else {
				String id = lob.getReferenceStreamId();
				this.lobReferences.put(id, lob);
			}
		}
	}
	
    public Streamable<?> getLobReference(String id) throws TeiidComponentException {
    	Streamable<?> lob = this.lobReferences.get(id);
    	if (lob == null) {
    		throw new TeiidComponentException(QueryPlugin.Util.getString("ProcessWorker.wrongdata")); //$NON-NLS-1$
    	}
    	return lob;
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

	public void persist(FileStore lobStore) throws TeiidComponentException {
		// stream the contents of lob into file store.
		byte[] bytes = new byte[102400]; // 100k

		for (Map.Entry<String, Streamable<?>> entry : this.lobReferences.entrySet()) {
			entry.setValue(persistLob(entry.getValue(), lobStore, bytes));
		}
	}    
    
	private Streamable<?> persistLob(final Streamable<?> lob, final FileStore store, byte[] bytes) throws TeiidComponentException {
		
		// if this is already saved to disk just return
		if (lob.getReference() instanceof BaseLob) {
			try {
				BaseLob baseLob = (BaseLob)lob.getReference();
				InputStreamFactory isf = baseLob.getStreamFactory();
				if (isf.getStorageMode() == StorageMode.PERSISTENT) {
					return lob;
				}
			} catch (SQLException e) {
				// go through regular persistence.
			}
		}
		long offset = store.getLength();
		int length = 0;
		Streamable<?> persistedLob;
					
		try {
			InputStream is = null;
	    	if (lob instanceof BlobType) {
	    		is = new BlobInputStreamFactory((Blob)lob).getInputStream();
	    	}
	    	else if (lob instanceof ClobType) {
	    		is = new ClobInputStreamFactory((Clob)lob).getInputStream();
	    	} else {
	    		is = new SQLXMLInputStreamFactory((SQLXML)lob).getInputStream();
	    	}
			OutputStream fsos = store.createOutputStream();
			length = ObjectConverterUtil.write(fsos, is, bytes, -1);
		} catch (IOException e) {
			throw new TeiidComponentException(e);
		}
		
		// re-construct the new lobs based on the file store
		final long lobOffset = offset;
		final int lobLength = length;
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
		
		try {
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
		} catch (SQLException e) {
			throw new TeiidComponentException(e);
		}		
		return persistedLob;		
	}
	
	public int getLobCount() {
		return this.lobReferences.size();
	}
}
