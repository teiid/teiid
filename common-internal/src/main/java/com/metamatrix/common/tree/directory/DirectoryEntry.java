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

package com.metamatrix.common.tree.directory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;

import com.metamatrix.common.tree.TreeNode;

/**
 * This interface represents a single resource on a hierarchical system, such as
 * a file system.
 */
public interface DirectoryEntry extends TreeNode {

    public static final FileDefinition TYPE_FILE = new FileDefinitionImpl("File"); //$NON-NLS-1$
    public static final FolderDefinition TYPE_FOLDER = new FolderDefinitionImpl("Folder"); //$NON-NLS-1$

    /**
     * Return whether it is possible to write to this DirectoryEntry,
     * and whether an output stream can be obtained for this entry.
     * This method may return true even if this entry does not exist (i.e., <code>exists()<code>
     * returns true).
     * @return true if writing is possible, as well as whether an OutputStream instance
     * can be obtained by calling the <code>getOutputStream()</code> method.
     */
    boolean canWrite();

    /**
     * Return whether it is possible to read from this DirectoryEntry,
     * and whether an input stream can be obtained for this entry.
     * This method always returns false if this entry does not exist (i.e., <code>exists()<code>
     * returns false).
     * @return true if reading is possible, as well as whether an InputStream instance
     * can be obtained by calling the <code>getInputStream()</code> method.
     */
    boolean canRead();

    /**
     * Load property values associated with this DirectoryEntry and return whether
     * the preview properties are now available.
     * @return if the properties have been loaded.
     */
    boolean loadPreview();

    /**
     * If this DirectoryEntry is readable, then return an InputStream instance to
     * the resource represented by this entry.
     * @return the InputStream for this entry.
     * @throws AssertionError if this method is called on a DirectoryEntry
     * for which <code>canRead()</code> returns false.
     * @throws IOException if there was an error creating the stream
     */
    InputStream getInputStream() throws IOException;

    /**
     * If this DirectoryEntry is writable, then return an OutputStream instance to
     * the resource represented by this entry.
     * @return the OutputStream for this entry.
     * @throws AssertionError if this method is called on a DirectoryEntry
     * for which <code>canWrite()</code> returns false.
     * @throws IOException if there was an error creating the stream
     */
    OutputStream getOutputStream() throws IOException;

    /**
     * Determine whether this DirectoryEntry is of type FolderDefinition, meaning
     * it represents a container that may have children.
     * @return true if <code>getType()<code> returns FolderDefinition, or false otherwise.
     */
//    boolean isFolder();

    /**
     * Converts this abstract pathname into a URL.  The exact form of the URL
     * is dependent upon the implementation. If it can be determined that the
     * file denoted by this abstract pathname is a directory, then the resulting
     * URL will end with a slash.
     * @return the URL for this DirectoryEntry.
     * @throws MalformedURLException if the URL is malformed.
     */
    URL toURL() throws MalformedURLException;
}

