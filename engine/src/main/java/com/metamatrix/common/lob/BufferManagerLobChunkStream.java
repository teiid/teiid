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

package com.metamatrix.common.lob;


import java.io.IOException;

import com.metamatrix.api.exception.MetaMatrixComponentException;
import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.buffer.TupleSourceID;
import com.metamatrix.common.buffer.TupleSourceNotFoundException;
import com.metamatrix.common.log.LogManager;
import com.metamatrix.dqp.util.LogConstants;

public class BufferManagerLobChunkStream  implements LobChunkProducer {
    TupleSourceID sourceId;
    BufferManager bufferMgr;
    int position = 0;
    
    public BufferManagerLobChunkStream(String persitentId, BufferManager bufferMgr) {
        this.sourceId = new TupleSourceID(persitentId);
        this.bufferMgr = bufferMgr;
    }
    
    public LobChunk getNextChunk() throws IOException {
        try {
            this.position++;
            return bufferMgr.getStreamablePart(sourceId, position);
        } catch (TupleSourceNotFoundException e) {
            String msg = CommonPlugin.Util.getString("BufferManagerLobChunkStream.no_tuple_source", new Object[] {sourceId}); //$NON-NLS-1$
            LogManager.logWarning(LogConstants.CTX_BUFFER_MGR, e, msg); 
            throw new IOException(msg);
        } catch (MetaMatrixComponentException e) {
            String msg = CommonPlugin.Util.getString("BufferManagerLobChunkStream.error_processing", new Object[] {sourceId}); //$NON-NLS-1$
            LogManager.logWarning(LogConstants.CTX_BUFFER_MGR, e, msg); 
            throw new IOException(msg);
        }                
    }

    /** 
     * @see com.metamatrix.common.lob.LobChunkProducer#close()
     */
    public void close() throws IOException {
        // we could remove the buffer tuple here but, this is just a stream, so we need to delete 
        // that when we close th eplan.
    }
}