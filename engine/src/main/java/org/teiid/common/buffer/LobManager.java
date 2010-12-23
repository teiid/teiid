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

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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
import org.teiid.query.QueryPlugin;
import org.teiid.query.processor.xml.XMLUtil.FileStoreInputStreamFactory;
import org.teiid.query.sql.symbol.Expression;

/**
 * Tracks lob references so they are not lost during serialization.
 * TODO: for temp tables we may need to have a copy by value management strategy
 */
public class LobManager {
	private static final int IO_BUFFER_SIZE = 1 << 14;
	private Map<String, Streamable<?>> lobReferences = new ConcurrentHashMap<String, Streamable<?>>();
	private Map<String, Streamable<?>> lobFilestores = new ConcurrentHashMap<String, Streamable<?>>();

	public void updateReferences(int[] lobIndexes, List<?> tuple)
			throws TeiidComponentException {
		for (int i = 0; i < lobIndexes.length; i++) {
			Object anObj = tuple.get(lobIndexes[i]);
			if (!(anObj instanceof Streamable<?>)) {
				continue;
			}
			Streamable lob = (Streamable) anObj;
			if (lob.getReference() == null) {
				lob.setReference(getLobReference(lob.getReferenceStreamId()).getReference());
			} else {
				String id = lob.getReferenceStreamId();
				this.lobReferences.put(id, lob);
			}
		}
	}
	
    public Streamable<?> getLobReference(String id) throws TeiidComponentException {
    	Streamable<?> lob = null;
    	if (this.lobReferences != null) {
    		lob = this.lobReferences.get(id);
    	}
    	
    	if (lob == null) {
			lob = this.lobFilestores.get(id);
    	}
    	
    	if (lob == null) {
    		throw new TeiidComponentException(QueryPlugin.Util.getString("ProcessWorker.wrongdata")); //$NON-NLS-1$
    	}
    	return lob;
    }
        
    public void clear() {
    	this.lobReferences.clear();
    	this.lobFilestores.clear();
    }
    
    public static int[] getLobIndexes(List expressions) {
    	if (expressions == null) {
    		return null;
    	}
		int[] result = new int[expressions.size()];
		int resultIndex = 0;
	    for (int i = 0; i < expressions.size(); i++) {
	    	Expression expr = (Expression) expressions.get(i);
	        if (DataTypeManager.isLOB(expr.getType()) || expr.getType() == DataTypeManager.DefaultDataClasses.OBJECT) {
	        	result[resultIndex++] = i;
	        }
	    }
	    if (resultIndex == 0) {
	    	return null;
	    }
	    return Arrays.copyOf(result, resultIndex);
    }

    public Collection<Streamable<?>> getLobReferences(){
    	return lobReferences.values();
    }
    
	public void persist(FileStore lobStore) throws TeiidComponentException {
		ArrayList<Streamable<?>> lobs = new ArrayList<Streamable<?>>(this.lobReferences.values());
		for (Streamable<?> lob:lobs) {
			persist(lob.getReferenceStreamId(), lobStore);
		}
	}    
    
	public Streamable<?> persist(String id, FileStore fs) throws TeiidComponentException {
		Streamable<?> persistedLob = this.lobFilestores.get(id);
		if (persistedLob == null) {
			Streamable<?> lobReference =  this.lobReferences.get(id);
			if (lobReference == null) {
	    		throw new TeiidComponentException(QueryPlugin.Util.getString("ProcessWorker.wrongdata")); //$NON-NLS-1$
	    	}
	    	
	    	persistedLob = persistLob(lobReference, fs);
			synchronized (this) {
				this.lobFilestores.put(id, persistedLob);
				this.lobReferences.remove(id);		
			}
		}
		return persistedLob;
	}
	
	private Streamable<?> persistLob(final Streamable<?> lob, final FileStore store) throws TeiidComponentException {
		long offset = store.getLength();
		int length = 0;
		Streamable<?> persistedLob;
		
		// if this is XML and already saved to disk just return
		if (lob.getReference() instanceof SQLXMLImpl) {
			try {
				SQLXMLImpl xml = (SQLXMLImpl)lob.getReference();
				InputStreamFactory isf = xml.getStreamFactory();
				if (isf instanceof FileStoreInputStreamFactory) {
					return lob;
				}
			} catch (SQLException e) {
				// go through regular persistence.
			}
		}
					
		// stream the contents of lob into file store.
		byte[] bytes = new byte[102400]; // 100k
		try {
			InputStreamFactory isf = new InputStreamFactory() {
				@Override
				public InputStream getInputStream() throws IOException {
			    	if (lob instanceof BlobType) {
			    		return new BlobInputStreamFactory((Blob)lob).getInputStream();
			    	}
			    	else if (lob instanceof ClobType) {
			    		return new ClobInputStreamFactory((Clob)lob).getInputStream();
			    	}
			    	return new SQLXMLInputStreamFactory((SQLXML)lob).getInputStream();
				}					
			};
			InputStream is = isf.getInputStream();
			OutputStream fsos = new BufferedOutputStream(store.createOutputStream(), IO_BUFFER_SIZE);
			while(true) {
				int read = is.read(bytes, 0, 102400);
				if (read == -1) {
					break;
				}
				length += read;
				fsos.write(bytes, 0, read);
			}
			fsos.close();
			is.close();
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
}
