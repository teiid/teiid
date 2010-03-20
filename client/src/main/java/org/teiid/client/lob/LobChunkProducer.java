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

package org.teiid.client.lob;

import java.io.IOException;



/** 
 * An interface for generating or producing the LobChunks from a remote or local
 * location. A LobChunk is part or whole of a LOB (clob, blob, xml) object.
 * 
 * @see LobChunk
 */
public interface LobChunkProducer {
    /**
     * Gets the next LobChunk from the source, based on the chunk size configured
     * @param position position of the chunk starts from 1 to .. increments by one
     * until the last chunk returned.  Must start with 1 and increment.
     * @return LobChunk at position in the streamble object.
     * @throws IOException
     */
    LobChunk getNextChunk() throws IOException;
    
    /**
     * Close the underlaying stream of producing the chunks
     */
    void close() throws IOException;
}
