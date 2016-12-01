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
package org.teiid.file;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.resource.ResourceException;
import org.teiid.translator.FileConnection;

/**
 * A interface used to operate a specific path of a FTP Server.
 *
 */
public interface FtpFileConnection extends FileConnection {
    
    /**
     * Return a list of files by a give file pattern 
     * @param pattern the syntax and pattern
     * @return
     * @throws IOException
     */
    File[] getFiles(String pattern) throws ResourceException;
    
    boolean remove(String name) throws ResourceException;
    
    /**
     * Read file 'name' from remote FTP server
     * @param name file name to read
     * @param out
     * @throws IOException
     */
    void read(String name, OutputStream out) throws ResourceException;

    /**
     * Create file 'name' under remote FTP server
     * @param in content need to add to remote server
     * @param name file name to created
     * @throws IOException
     */
    void write(InputStream in, String name) throws ResourceException;
    
    void append(InputStream in, String name) throws ResourceException;
    
    void rename(String oldName, String newName) throws ResourceException;
    
    /**
     * Check if the remote file exists
     * @param name the file name under remote FTP server
     * @return {@code true} or {@code false} if remote file exists or not.
     * @throws IOException
     */
    boolean exists(String name) throws ResourceException;

    /**
     * List all file names under remote FTP server
     * @return
     * @throws IOException
     */
    String[] listNames() throws ResourceException;
    
}
