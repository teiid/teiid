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

package org.teiid.script.io;

import java.io.IOException;
import java.io.Reader;


/** 
 * Base class for any type of Reader, where data can be read as line by line.
 * The derived classes just need to extend this class and implement the "nextLine()"
 * method to get the full "Reader" functionality.
 * 
 */
public abstract class StringLineReader extends Reader {

    // Current line which is being fed to the reader
    String currentLine = null;
    // Current index of where the reading stopped last time
    int currentLineIndex = 0;

    boolean closed = false;
    
    /** 
     * @see java.io.Reader#close()
     * @since 4.3
     */
    public void close() throws IOException {
        closed = true;
    }

    /** 
     * @see java.io.Reader#read(char[], int, int)
     * @since 4.3
     */
    public int read(char[] cbuf, int off, int len) throws IOException {
        if (closed) {
            throw new IllegalStateException("Reader already closed"); //$NON-NLS-1$
        }

        int srcoff = currentLineIndex;        
        if (currentLine == null || (currentLine != null && (currentLine.length()-currentLineIndex) <= 0)) {            
            currentLine = nextLine();
            currentLineIndex = 0; 
            srcoff = currentLineIndex;
        }
         
        // If we have data available then send it.
        if (currentLine != null) {
            // If requested more than one line limit length to one line
            if (len > (currentLine.length()-currentLineIndex)) {
                len = (currentLine.length()-currentLineIndex);
            }
                                
            // Copy the contents to destination.
            System.arraycopy(currentLine.toCharArray(), srcoff, cbuf, off, len);
            
            // Now move the current index further 
            currentLineIndex = currentLineIndex+len;
            return len;
        }
        return -1;
    }

    /**
     * Get the next line of data from the data source. 
     * @return
     * @throws IOException
     * @since 4.3
     */
    abstract protected String nextLine() throws IOException; 
}
