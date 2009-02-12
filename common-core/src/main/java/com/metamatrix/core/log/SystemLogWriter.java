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

import com.metamatrix.internal.core.log.PlatformLogWriter;

/**
 * The SystemLogWriter is a {@link LogListener} that writes error log messages to System.err and other log
 * messages to System.out.  
 */
public class SystemLogWriter implements LogListener {
    
    private PlatformLogWriter writer;
    
    public SystemLogWriter() {
        this.writer = new PlatformLogWriter(System.out,System.err);
    }

    /* (non-Javadoc)
     * @see com.metamatrix.core.log.LogListener#logMessage(org.eclipse.core.runtime.IStatus, long, java.lang.String, java.lang.String)
     */
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
