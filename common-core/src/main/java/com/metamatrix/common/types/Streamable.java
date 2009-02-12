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
public interface Streamable extends Serializable {
    static final String FORCE_STREAMING = "FORCE_STREAMING"; //$NON-NLS-1$
    public static final int STREAMING_BATCH_SIZE_IN_BYTES = 102400; // 100K
    
    /**
     * Reference Stream ID in the server 
     * @return string - this is buffer managers tuple source id.
     */
    String getReferenceStreamId();
    
    /**
     * Reference Stream ID in the server
     * @param id this is buffer managers tuple source id.
     */
    void setReferenceStreamId(String id);    
    
    /**
     * Persitence Stream ID in the server 
     * @return string - this is buffer managers tuple source id.
     */
    String getPersistenceStreamId();
    
    /**
     * Persitence Stream ID in the server
     * @param id this is buffer managers tuple source id.
     */
    void setPersistenceStreamId(String id);       
}
