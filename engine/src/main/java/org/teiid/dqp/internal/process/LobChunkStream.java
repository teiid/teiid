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

import java.io.IOException;
import java.nio.charset.Charset;
import java.sql.SQLException;

import com.metamatrix.common.buffer.BufferManager;
import com.metamatrix.common.lob.BufferManagerLobChunkStream;
import com.metamatrix.common.lob.ByteLobChunkStream;
import com.metamatrix.common.lob.LobChunk;
import com.metamatrix.common.lob.LobChunkProducer;
import com.metamatrix.common.lob.ReaderInputStream;
import com.metamatrix.common.types.BlobType;
import com.metamatrix.common.types.ClobType;
import com.metamatrix.common.types.InvalidReferenceException;
import com.metamatrix.common.types.Streamable;
import com.metamatrix.common.types.XMLType;
import com.metamatrix.dqp.DQPPlugin;

/** 
 * A Lob Stream builder class. Given the Lob object this object can build 
 * an object which can produce a stream lob chunks, which can be sent to the
 * client one by one.  
 */
class LobChunkStream implements LobChunkProducer {

    LobChunkProducer internalStream = null;
    
    public LobChunkStream(Streamable streamable, int chunkSize, BufferManager bufferMgr) 
        throws IOException {
        
        try {
            if (streamable instanceof XMLType) {
                XMLType xml = (XMLType)streamable;
                this.internalStream = new ByteLobChunkStream(new ReaderInputStream(xml.getCharacterStream(), Charset.forName("UTF-16")), chunkSize); //$NON-NLS-1$
            }
            else if (streamable instanceof ClobType) {
                ClobType clob = (ClobType)streamable;
                this.internalStream = new ByteLobChunkStream(new ReaderInputStream(clob.getCharacterStream(), Charset.forName("UTF-16")), chunkSize); //$NON-NLS-1$            
            } 
            else if (streamable instanceof BlobType) {
                BlobType blob = (BlobType)streamable;
                this.internalStream = new ByteLobChunkStream(blob.getBinaryStream(), chunkSize);                        
            }
        } catch (InvalidReferenceException e) {
            // if the lob did not have a persistent id, there is no way for us to re-create the
            // object. so throw an error.
            if (streamable.getPersistenceStreamId() == null) {
                throw new IOException(DQPPlugin.Util.getString("LobStream.noreference")); //$NON-NLS-1$
            }            
            // otherwise read directly from the buffer manager. 
            this.internalStream = new BufferManagerLobChunkStream(streamable.getPersistenceStreamId(), bufferMgr);            
        } catch(SQLException e) {
            IOException ex = new IOException();
            ex.initCause(e);
            throw ex;
        }
    }
    
    /** 
     * @see com.metamatrix.common.lob.LobChunkProducer#getNextChunk(int)
     */
    public LobChunk getNextChunk() throws IOException {
        return internalStream.getNextChunk();
    }

    /** 
     * @see com.metamatrix.common.lob.LobChunkProducer#close()
     */
    public void close() throws IOException {
        internalStream.close();
    }    
}
