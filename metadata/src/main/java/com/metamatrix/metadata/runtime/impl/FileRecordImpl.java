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

package com.metamatrix.metadata.runtime.impl;

import java.io.File;
import java.io.InputStream;

import com.metamatrix.core.util.EquivalenceUtil;
import com.metamatrix.core.util.FileUtils;
import com.metamatrix.core.util.HashCodeUtil;
import com.metamatrix.modeler.core.index.IndexConstants;
import com.metamatrix.modeler.core.index.IndexSelector;
import com.metamatrix.modeler.core.metadata.runtime.FileRecord;
import com.metamatrix.modeler.internal.core.workspace.ModelFileUtil;

/** 
 * @since 4.2
 */
public class FileRecordImpl extends AbstractMetadataRecord implements FileRecord {
    
    private String pathInVdb;
    private String[] tokens;
    private String[] tokenReplacements;
    private IndexSelector selector;

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.FileRecord#getPathInVdb()
     * @since 4.2
     */
    public String getPathInVdb() {
        return this.pathInVdb;
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.FileRecord#isBinary()
     * @since 4.2
     */
    public boolean getBinary() {
        if(this.pathInVdb != null && this.pathInVdb.endsWith(IndexConstants.INDEX_EXT)) {
            return true;
        }
        return false;
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.FileRecord#getContent()
     * @since 4.2
     */
    public InputStream getContent() {
        if(this.tokens != null) {
            return this.selector.getFileContent(getPathInVdb(), this.tokens, this.tokenReplacements);
        }
        return this.selector.getFileContent(getPathInVdb());        
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.FileRecord#getFileLength()
     * @since 4.2
     */
    public long getFileLength() {
        return this.selector.getFileSize(getPathInVdb());
    }

    /** 
     * @param pathInVdb The pathInVdb to set.
     * @since 4.2
     */
    public void setPathInVdb(final String pathInVdb) {
        this.pathInVdb = pathInVdb;
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.FileRecord#getTokenReplacementString1()
     * @since 4.2
     */
    public String[] getTokens() {
        return this.tokens;
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.FileRecord#getTokenReplacements()
     * @since 4.2
     */
    public String[] getTokenReplacements() {
        return this.tokenReplacements;
    }
    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.FileRecord#setTokens(java.lang.String[])
     * @since 4.2
     */
    public void setTokens(final String[] tokens) {
        this.tokens = tokens;
    }
    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.FileRecord#setTokenReplacementString2(java.lang.String[])
     * @since 4.2
     */
    public void setTokenReplacements(final String[] tokenReplacements) {
        this.tokenReplacements = tokenReplacements;
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.FileRecord#setIndexSelector(com.metamatrix.modeler.core.index.IndexSelector)
     * @since 4.2
     */
    public void setIndexSelector(final IndexSelector selector) {
        this.selector = selector;
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.FileRecord#getFileRecord()
     * @since 4.2
     */
    public FileRecord getFileRecord() {
        return this;
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.MetadataRecord#getModelName()
     * @since 4.2
     */
    public String getModelName() {
        if(isModelFile()) {
        	return FileUtils.getBaseFileNameWithoutExtension(this.pathInVdb);
        }
        return null;
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.FileRecord#isIndexFile()
     * @since 4.2
     */
    public boolean isIndexFile() {
        if(this.pathInVdb != null && this.pathInVdb.endsWith(IndexConstants.INDEX_EXT)) {
            return true;
        }
        return false;
    }

    /** 
     * @see com.metamatrix.modeler.core.metadata.runtime.FileRecord#getModelName()
     * @since 4.2
     */
    public boolean isModelFile() {
        if(this.pathInVdb != null) {
            File fileInVdb = this.selector.getFile(this.pathInVdb);
            return ModelFileUtil.isModelFile(fileInVdb);
        }
        return false;
    }

    /**
     * Compare two records for equality.
     */
    public boolean equals(Object obj) {

        if(obj == this) {
            return true;
        }

        if(obj == null || obj.getClass() != this.getClass()) {
            return false;
        }

        FileRecord other = (FileRecord)obj;

        if(!EquivalenceUtil.areEqual(this.getPathInVdb(), other.getPathInVdb())) { return false; }
        if(!EquivalenceUtil.areEqual(this.getTokens(), other.getTokens())) { return false; }
        if(!EquivalenceUtil.areEqual(this.getTokenReplacements(), other.getTokenReplacements())) { return false; }

        return true;
    }

    /**
     * Get hashcode for From.  WARNING: The hash code relies on the variables
     * in the record, so changing the variables will change the hash code, causing
     * a select to be lost in a hash structure.  Do not hash a record if you plan
     * to change it.
     */
    public int hashCode() {
        int myHash = 0;
        if (this.pathInVdb != null) {
            myHash = HashCodeUtil.hashCode(myHash, this.pathInVdb);
        }
        if (this.tokens != null) {
            myHash = HashCodeUtil.hashCode(myHash, this.tokens);
        }
        if (this.tokenReplacements != null) {
            myHash = HashCodeUtil.hashCode(myHash, this.tokenReplacements);
        }

        return myHash;
    }
    
    public String toString() {
        return this.getPathInVdb();
    }
}