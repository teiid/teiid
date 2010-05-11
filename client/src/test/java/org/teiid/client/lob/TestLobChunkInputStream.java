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
import java.util.Arrays;
import java.util.Iterator;

import org.teiid.client.lob.LobChunk;
import org.teiid.client.lob.LobChunkInputStream;
import org.teiid.client.lob.LobChunkProducer;
import org.teiid.core.util.ObjectConverterUtil;

import junit.framework.TestCase;


public class TestLobChunkInputStream extends TestCase {

    public void testReadByteArray() throws Exception {
    	LobChunkProducer chunkProducer = new LobChunkProducer() {
			
    		Iterator<LobChunk> chuncks = Arrays.asList(new LobChunk("hello ".getBytes(), false), new LobChunk("world".getBytes(), true)).iterator(); //$NON-NLS-1$ //$NON-NLS-2$ 
    		
			@Override
			public LobChunk getNextChunk() throws IOException {
				return chuncks.next();
			}
			
			@Override
			public void close() throws IOException {
				
			}
		};
        LobChunkInputStream stream = new LobChunkInputStream(chunkProducer);
        
        assertEquals("hello world", ObjectConverterUtil.convertToString(stream)); //$NON-NLS-1$
    }
    
}
