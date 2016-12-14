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

import java.io.InputStream;

import javax.resource.ResourceException;
import javax.resource.cci.Connection;

import org.jboss.vfs.VirtualFile;

/**
 * Common abstraction for a VirtualFileConnection with a JBoss Virtual File System(JBoss VFS).
 * @author kylin
 *
 */
public interface VirtualFileConnection extends Connection {

    /**
     * Return a list of files by a given file pattern
     * @param namePattern - the syntax and pattern
     * @return
     * @throws ResourceException
     */
    VirtualFile[] getFiles(String namePattern) throws ResourceException;
    
    /**
     * Return a file by a given file name 
     * @param name
     * @return
     * @throws ResourceException
     */
    VirtualFile getFile(String path) throws ResourceException;
    
    /**
     * Add a file to JBoss VFS
     * @param file
     * @throws ResourceException
     */
    boolean add(InputStream in, VirtualFile file) throws ResourceException;
    
    /**
     * Remove a file from JBoss VFS by given path
     * @param path
     * @return
     * @throws ResourceException
     */
    boolean remove(String path) throws ResourceException;
}
