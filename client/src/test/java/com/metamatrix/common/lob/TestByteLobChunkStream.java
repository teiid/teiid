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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;

import junit.framework.TestCase;

import com.metamatrix.core.util.UnitTestUtil;


public class TestByteLobChunkStream extends TestCase {


    public void testGetChunk() throws Exception {
        File f = UnitTestUtil.getTestDataFile("legal_notice.xml"); //$NON-NLS-1$
        ByteLobChunkStream stream = new ByteLobChunkStream(new FileInputStream(f), 10);
        
        StringBuffer sb = new StringBuffer();
        LobChunk chunk = null;
        do{
            chunk = stream.getNextChunk();
            sb.append(new String(chunk.getBytes()));
        }while(!chunk.isLast());
        stream.close();
        
        assertEquals(readFile(f), sb.toString());            
    }
    
    private String readFile(File f) throws IOException{
        StringBuffer sb = new StringBuffer();
        
        FileReader reader = new FileReader(f);
        int chr = reader.read();
        while(chr != -1) {
            sb.append((char)chr);
            chr = reader.read();
        }
        reader.close();
        return sb.toString();
    }

}
