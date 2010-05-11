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

import java.io.ByteArrayInputStream;
import java.util.Arrays;

import org.teiid.client.lob.LobChunkInputStream;
import org.teiid.core.util.ObjectConverterUtil;

import junit.framework.TestCase;


public class TestByteLobChunkStream extends TestCase {

    public void testGetChunk() throws Exception {
    	byte[] bytes = "hello world".getBytes(); //$NON-NLS-1$
        ByteLobChunkStream stream = new ByteLobChunkStream(new ByteArrayInputStream(bytes), 5);

        assertTrue(Arrays.equals(bytes, ObjectConverterUtil.convertToByteArray(new LobChunkInputStream(stream))));            
    }
    
}
