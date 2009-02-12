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

package com.metamatrix.common.util;

import java.io.ByteArrayInputStream;

/**
 * This test input stream overrides the <code>available</code>
 * to return "1" until <i>after</i> the {@link #read} method returns
 * "-1" to indicate EOF, which is how java.util.zip.ZipInputStream
 * works, which is what is used by ExtensionSourceManager to retrieve
 * Class files from JAR files.  The <code>available</code> method
 * therefore can't be relied on by ByteArrayHelper to either
 * indicate how many bytes can be read, or if more are
 * available or not.  In the latter case, ByteArrayHelper
 * relies on the <code>read</code> method returning "-1".
 */
public class FakeInputStream extends ByteArrayInputStream {

    private int available = 1;

    public FakeInputStream(byte[] buf) {
        super(buf);
    }

    /**
     * Overriden to return "1" <i>until</i> the <code>read</code> method
     * has returned "-1" to indicate EOF.
     */
    public int available(){
        super.available();
        return available;
    }

    /**
     * Overriden - basically calls to super method, but checks returned
     * number of bytes read; if "-1", then the next call to
     * {@link #available} will return "0".
     */
    public int read(byte b[], int off, int len) {
        int result = super.read(b, off, len);
        if (result<0){
            available = 0;
        }
        return result;
    }

}
