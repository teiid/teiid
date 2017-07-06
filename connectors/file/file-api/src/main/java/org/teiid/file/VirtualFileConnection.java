/*
 * Copyright Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags and
 * the COPYRIGHT.txt file distributed with this work.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
