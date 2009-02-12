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

package com.metamatrix.core.log;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.metamatrix.core.CorePlugin;
import com.metamatrix.internal.core.log.PlatformLogWriter;

/**
 * The FileLogWriter is a {@link LogListener} that writes (appends) to a supplied file.
 */
public class FileLogWriter implements LogListener {
    
    private PlatformLogWriter writer;
    
    public FileLogWriter( final File file ) {
        if (file == null) {
            final String msg = CorePlugin.Util.getString("FileLogWriter.The_File_reference_may_not_be_null"); //$NON-NLS-1$
            throw new IllegalArgumentException(msg);
        }

        try {
            OutputStream destination = new FileOutputStream(file.getAbsolutePath(), true);
            this.writer = new PlatformLogWriter(destination, destination);
        } catch (IOException e) {
            this.writer = new PlatformLogWriter(System.err, System.err);
        }
    }

    public void logMessage(LogMessage msg) {
        this.writer.logMessage(msg);
    }
    
    /**
     * Close the stream to the file.
     */
    public void shutdown() {
        this.writer.shutdown();
    }

}
