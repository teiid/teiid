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

package com.metamatrix.common.types;

import java.io.Serializable;

import com.metamatrix.core.CorePlugin;


/**
 * A large value object which can be streamable in chunks of data each time
 * 
 * <p>A reference stream id is tuple source id for a Streamble object where the 
 * object is in buffer manager, but the contents will never be written to disk;
 * this is the ID that client needs to reference to get the chunk of data.
 * 
 * <p>A Persistent stream id is Tuple source id under which the server *may* 
 * have saved the data to disk in buffer manager. In case of XML it is saved
 * however in case of Clobs and Blobs it is not saved yet. This id is used by
 * the process worker to in case the reference object has lost its state and we 
 * need to reinsate the object from the disk.
 */
public abstract class Streamable<T> implements Serializable {
    public static final String FORCE_STREAMING = "FORCE_STREAMING"; //$NON-NLS-1$
    public static final int STREAMING_BATCH_SIZE_IN_BYTES = 102400; // 100K

    private String referenceStreamId;
    private String persistenceStreamId;
    protected transient T reference;
    
    public Streamable() {
    	
	}
    
    public Streamable(T reference) {
        if (reference == null) {
            throw new IllegalArgumentException(CorePlugin.Util.getString("Streamable.isNUll")); //$NON-NLS-1$
        }

    	this.reference = reference;
    }
    
    public T getReference() {
		return reference;
	}
    
    public void setReference(T reference) {
		this.reference = reference;
	}
    
    public String getReferenceStreamId() {
        return this.referenceStreamId;
    }
    
    public void setReferenceStreamId(String id) {
        this.referenceStreamId = id;
    }
    
    public String getPersistenceStreamId() {
        return persistenceStreamId;
    }

    public void setPersistenceStreamId(String id) {
        this.persistenceStreamId = id;
    } 
    
    @Override
    public String toString() {
        return reference.toString();
    }
    
    @Override
    public boolean equals(Object obj) {
    	if (this == obj) {
    		return true;
    	}
    	if (!(obj instanceof Streamable<?>)) {
    		return false;
    	}
    	Streamable<?> other = (Streamable<?>)obj;
    	
    	if (this.reference != null) {
    		return this.reference.equals(other.reference);
    	}
    	
    	return this.persistenceStreamId == other.persistenceStreamId
		&& this.referenceStreamId == other.referenceStreamId;
    }

}
