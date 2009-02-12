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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;

import com.metamatrix.core.CorePlugin;


/** 
 * A InputStream wrapper class for a Lob Chunks. Given a stream of Lob Chunks
 * this class will convert those chunks into InputStream, which can be used to
 * stream the lob data. 
 */
public class LobChunkInputStream extends InputStream {
    LobChunkProducer reader;

    byte[] byteData = null;
    int currentCounter = 0;
    boolean lastChunk = false;
    int availableCounter = 0;
    boolean closed = false;
    
    byte[] contents = null;
    
    public LobChunkInputStream(LobChunkProducer reader) {
        this.reader = reader;
    }
    
    public int read() throws IOException {               
        if (this.closed) {
            throw new IllegalStateException(CorePlugin.Util.getString("stream_closed")); //$NON-NLS-1$
        }        
        if (this.availableCounter == 0) {
        	if (this.lastChunk) {
	            // we are done
	            return -1;
        	}
            fetchNextChunk();
        }

        // so we have data
        int ret = -1;
        if (this.availableCounter > 0) {
            ret = (byteData[currentCounter++] & 0xFF);
            this.availableCounter--;
        }
        return ret;
    }

    void fetchNextChunk() throws IOException {
    	LobChunk value = this.reader.getNextChunk();
        if (value != null) {
            this.lastChunk = value.isLast();
            this.byteData = value.getBytes();
            this.currentCounter = 0;
            this.availableCounter = this.byteData.length;
        } else {
            throw new IOException(CorePlugin.Util.getString("lob.invaliddata")); //$NON-NLS-1$
        }
    }

    public int read(byte[] b) throws IOException {
        return read(b, 0, b.length);
    }

    /**  
     * @see java.io.InputStream#close()
     */
    public void close() throws IOException {
        this.closed = true;
        this.reader.close();
    }

    /** 
     * Get the byte contents of the input stream. use caution as this may use up VM memory as
     * the contents are loaded into memory.
     */
    public byte[] getByteContents() throws IOException {
        if (this.contents == null) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(100*1024);
            byte[] buf = new byte[100*1024];
            int read = read(buf);                    
            while(read != -1) {
                bos.write(buf, 0, read);
                read = read(buf);
            }
            close();
            this.contents = bos.toByteArray();
            bos.close();
        }
        return this.contents;
    }
    
    /**
     * @return a valid UTF16 based reader
     */
    public Reader getUTF16Reader() {
    	return new InputStreamReader(this, Charset.forName("UTF-16")); //$NON-NLS-1$
    }
}
