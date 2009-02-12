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

package com.metamatrix.script.io;

import java.io.IOException;


/** 
 * Converts a String Array object into a Reader object.
 * @author <a href="mailto:rreddy@metamatrix.com">Ramesh Reddy</a>
 */
public class StringArrayReader extends StringLineReader {
    String[] source = null;
    int index = 0;
    
    public StringArrayReader(String[] src) {
        this.source = src;
    }

    /** 
     * @see test.metamatrix.script.io.StringLineReader#nextLine()
     * @since 4.3
     */
    protected String nextLine() throws IOException {
        if (index < this.source.length) {
            return this.source[index++]+"\n"; //$NON-NLS-1$
        }
        return null;
    }
}
