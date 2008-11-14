/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;

import com.metamatrix.common.CommonPlugin;
import com.metamatrix.common.object.ObjectDefinition;
import com.metamatrix.common.util.ErrorMessageKeys;
import com.metamatrix.core.util.Assertion;

/**
 * This class represents a wrapper for a DirectoryEntry that
 * holds off on actually writing to the DirectoryEntry so
 * that a preview of the result can be obtained prior to writing.
 * Once the preview is determined to be acceptable, the
 * {@link #getPreviewStream() getPreviewStream()}
 * method can be used to actually write the previewable contents
 * to the DirectoryEntry.
 * <p>An instance of this class can only be used to wrap a
 * DirectoryEntry that can write (i.e.,
 * {@link DirectoryEntry#canWrite() DirectoryEntry.canWrite()}
 * returns true).</p>
 * @version 	1.0
 * @author
 */
public class PreviewableDirectoryEntry implements DirectoryEntry {

	protected static final int BUFFER_SIZE = 1024;
	private static final String TEMP_FILE_PREFIX = "prevDirEntTemp"; //$NON-NLS-1$
	private static final String TEMP_FILE_SUFFIX = "tmp"; //$NON-NLS-1$

    private DirectoryEntry entry;
    private File tempFile;

    /**
     * Construct a new instance that wraps another DirectoryEntry
     * @param actualEntry the DirectoryEntry that this instance is a preview for;
     * may not be null
     */
    public PreviewableDirectoryEntry( DirectoryEntry actualEntry ) {
    	Assertion.isNotNull(actualEntry, "The DirectoryEntry reference may not be null."); //$NON-NLS-1$
    	this.entry = actualEntry;
    }

    /*
     * @see DirectoryEntry#getOutputStream()
     */
    public OutputStream getOutputStream() throws IOException {
        if ( !this.canWrite() ) {
        	throw new IOException(CommonPlugin.Util.getString(ErrorMessageKeys.TREE_ERR_0057));
        }
        // Create an output stream that write to the buffer
        this.tempFile = File.createTempFile(TEMP_FILE_PREFIX, TEMP_FILE_SUFFIX);
        return new FileOutputStream(this.tempFile);
    }

	/**
	 * Writes the preview to the entry.
	 * @return true if the preview was saved, or false if there was no preview to save
	 * @throws IOException if there is an error writing
	 * the contents to the DirectoryEntry
	 */
	public boolean savePreview() throws IOException {
		boolean success = false;
		if ( this.tempFile != null ) {
		    OutputStream fileOstream = this.entry.getOutputStream();	// may throw exception ...
		    Writer entryWriter = new OutputStreamWriter(fileOstream);
			success = writePreview(entryWriter,true);
		}
		return success;
	}

	/**
	 * Writes the preview to the specified writer.
	 * @param writer the Writer to which the preview contents should be written
	 * @param closeWriterUponCompletion true if this method should attempt to
	 * close <code>writer</code> when this method completes writing the contents
	 * @return true if the preview was written, or false if there was no preview to write
	 * @throws IOException if there is an error reading the contents or writing
	 * the contents to <code>writer</code>
	 */
	public boolean writePreview( Writer writer, boolean closeWriterUponCompletion ) throws IOException {
		boolean success = false;
		if ( this.tempFile != null ) {

		    // Create a reader to the temp file ...
		    FileReader contentsReader = new FileReader(this.tempFile);
		    char[] buffer = new char[BUFFER_SIZE];		// File reader uses 'char[]' not 'byte[]'
		    int n=0;

	        // Write the contents of the temp file to the writer ...
	        while ((n = contentsReader.read(buffer)) > -1) {
	            writer.write(buffer, 0, n);
	        }

	        // Close the reader and writer ...
	        contentsReader.close();
	        if ( closeWriterUponCompletion ) {
	        	writer.close();
	        }
	        success = true;
		}
		return success;
	}

	public void clearPreview() throws IOException {
		if( this.tempFile != null ) {
		    try {
				this.tempFile.delete();
		    } finally {
		    	this.tempFile = null;
		    }
		}
	}

	public boolean hasPreview() {
		return this.tempFile != null;
	}

	public DirectoryEntry getTargetDirectoryEntry() {
		return this.entry;
	}


    /**
     * Returns true if there was anything written to the buffer.
     * @see DirectoryEntry#canRead()
     */
    public boolean canRead() {
        return ( this.tempFile != null );
    }

    /*
     * @see DirectoryEntry#getInputStream()
     */
    public InputStream getInputStream() throws IOException {
		if ( this.tempFile != null ) {
			return new FileInputStream(this.tempFile);
		}
		return this.entry.getInputStream();
    }

	// #################### DELEGATE BACK TO entry #########################
	//
    /*
     * @see DirectoryEntry#canWrite()
     */
    public boolean canWrite() {
        return this.entry.canWrite();
    }

    /*
     * @see DirectoryEntry#loadPreview()
     */
    public boolean loadPreview() {
        return this.entry.loadPreview();
    }

    /*
     * @see DirectoryEntry#toURL()
     */
    public URL toURL() throws MalformedURLException {
        return this.entry.toURL();
    }

    /*
     * @see TreeNode#exists()
     */
    public boolean exists() {
        return this.entry.exists();
    }

    /*
     * @see TreeNode#getName()
     */
    public String getName() {
        return this.entry.getName();
    }

    /*
     * @see TreeNode#getFullName()
     */
    public String getFullName() {
        return this.entry.getFullName();
    }

    /*
     * @see TreeNode#getNamespace()
     */
    public String getNamespace() {
        return this.entry.getNamespace();
    }

    /*
     * @see TreeNode#getType()
     */
    public ObjectDefinition getType() {
        return this.entry.getType();
    }

    /*
     * @see TreeNode#getSeparatorChar()
     */
    public char getSeparatorChar() {
        return this.entry.getSeparatorChar();
    }

    /*
     * @see TreeNode#getSeparator()
     */
    public String getSeparator() {
        return this.entry.getSeparator();
    }

    /*
     * @see TreeNode#isModified()
     */
    public boolean isModified() {
        return this.entry.isModified();
    }

    /*
     * @see Comparable#compareTo(Object)
     */
    public int compareTo(Object o) {
        if ( o == null ) {
            return 1;	// this is greater than null
        }
        PreviewableDirectoryEntry that = (PreviewableDirectoryEntry) o;      // will throw class cast exception
        if ( that == this ) {
        	return 0;
        }
        return this.hashCode() - that.hashCode();
    }

    public boolean equals( Object o ) {
    	if ( o instanceof PreviewableDirectoryEntry ) {
    	    return ( o == this || o.hashCode() == this.hashCode() );
    	}
        return false;
    }

}
