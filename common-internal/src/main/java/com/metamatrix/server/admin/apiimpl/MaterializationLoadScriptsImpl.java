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

package com.metamatrix.server.admin.apiimpl;

import java.io.InputStream;
import java.io.Serializable;

import com.metamatrix.common.util.ByteArrayHelper;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.platform.security.api.SecurityPlugin;
import com.metamatrix.server.admin.api.MaterializationLoadScripts;

/** 
 * @since 4.2
 */
public class MaterializationLoadScriptsImpl implements MaterializationLoadScripts, Serializable {
    private String truncateFileName;
    private byte[] truncateFileContents;
    private String loadFileName;
    private byte[] loadFileContents;
    private String swapFileName;
    private byte[] swapFileContents;
    private String conPropsFileName;
    private byte[] conPropsFileContents;
    
    private String createFileName;
    private byte[] createFileContents;
    
    /** 
     * @param defaultFileNamePrefix
     * @param fileContents
     * @since 4.2
     */
    MaterializationLoadScriptsImpl() {
        // empty
     }
    
    /**
     * Add the named file contents. 
     * @param fileName The file name this file should have when written.
     * @param fileContents The contents of the file that will be written.
     * @since 4.2
     */
    public void setLoadScript(final String fileName, final byte[] fileContents) {
        if ( fileName == null ) {
            throw new MetaMatrixRuntimeException(SecurityPlugin.Util.getString("MaterializationLoadScriptsImpl.null_load_script_file_name")); //$NON-NLS-1$
        }
        if ( fileContents == null || fileContents.length == 0 ) {
            throw new MetaMatrixRuntimeException(SecurityPlugin.Util.getString("MaterializationLoadScriptsImpl.null_load_script_file_contents")); //$NON-NLS-1$
        }
        this.loadFileName = fileName;
        this.loadFileContents = fileContents;
    }

    /** 
     * @see com.metamatrix.server.admin.api.MaterializationLoadScripts#getLoadScriptFile()
     * @since 4.2
     */
    public InputStream getLoadScriptFile() {
        try {
            return ByteArrayHelper.toInputStream(loadFileContents);
        } catch (Exception err) {
            // Shouldn't ever happen according to ByteArrayHelper.toInputStream()
            return null;
        }
    }
    
    /** 
     * @see com.metamatrix.server.admin.api.MaterializationLoadScripts#getLoadScriptFileName()
     * @since 4.2
     */
    public String getLoadScriptFileName() {
        return this.loadFileName;
    }
    
    /**
     * Add the named file contents. 
     * @param fileName The file name this file should have when written.
     * @param fileContents The contents of the file that will be written.
     * @since 4.2
     */
    public void setSwapScript(final String fileName, final byte[] fileContents) {
        this.swapFileName = fileName;
        this.swapFileContents = fileContents;
    }

    /** 
     * @see com.metamatrix.server.admin.api.MaterializationLoadScripts#getSwapScriptFile()
     * @since 4.2
     */
    public InputStream getSwapScriptFile() {
        try {
            return ByteArrayHelper.toInputStream(swapFileContents);
        } catch (Exception err) {
            // Shouldn't ever happen according to ByteArrayHelper.toInputStream()
            return null;
        }
    }
    
    /** 
     * @see com.metamatrix.server.admin.api.MaterializationLoadScripts#getSwapScriptFileName()
     * @since 4.2
     */
    public String getSwapScriptFileName() {
        return this.swapFileName;
    }
    
    /**
     * Add the named file contents. 
     * @param fileName The file name this file should have when written.
     * @param fileContents The contents of the file that will be written.
     * @since 4.2
     */
    public void setTruncateScript(final String fileName, final byte[] fileContents) {
        this.truncateFileName = fileName;
        this.truncateFileContents = fileContents;
    }
    
    /** 
     * @see com.metamatrix.server.admin.api.MaterializationLoadScripts#getTruncateScriptFile()
     * @since 4.2
     */
    public InputStream getTruncateScriptFile() {
        try {
            return ByteArrayHelper.toInputStream(truncateFileContents);
        } catch (Exception err) {
            // Shouldn't ever happen according to ByteArrayHelper.toInputStream()
            return null;
        }
    }
    
    /** 
     * @see com.metamatrix.server.admin.api.MaterializationLoadScripts#getTruncateScriptFileName()
     * @since 4.2
     */
    public String getTruncateScriptFileName() {
        return this.truncateFileName;
    }

    /**
     * Add the named file contents. 
     * @param fileName The file name this file should have when written.
     * @param fileContents The contents of the file that will be written.
     * @since 4.2
     */
    public void setCreateScript(final String fileName, final byte[] fileContents) {
        this.createFileName = fileName;
        this.createFileContents = fileContents;
    }
    
    /** 
     * @see com.metamatrix.server.admin.api.MaterializationDDLScript#getCreateScriptFile()
     * @since 4.2
     */
    public InputStream getCreateScriptFile() {
        try {
            return ByteArrayHelper.toInputStream(createFileContents);
        } catch (Exception err) {
            // Shouldn't ever happen according to ByteArrayHelper.toInputStream()
            return null;
        }
     }
    
    /** 
     * @see com.metamatrix.server.admin.api.MaterializationDDLScript#getCreateScriptFileName()
     * @since 4.2
     */
    public String getCreateScriptFileName() {
        return this.createFileName;
    }
    
    /** 
     * @return Returns the conPropsFileContents.
     * @since 4.2
     */
    public InputStream getConnectionPropsFileContents() {
        try {
            return ByteArrayHelper.toInputStream(conPropsFileContents);
        } catch (Exception err) {
            // Shouldn't ever happen according to ByteArrayHelper.toInputStream()
            return null;
        }
    }
    
    /** 
     * @return Returns the conPropsFileName.
     * @since 4.2
     */
    public String getConnectionPropsFileName() {
        return this.conPropsFileName;
    }

    /** 
     * @param conPropsFileName The conPropsFileName to set.
     * @since 4.2
     */
    public void setConnectionPropsFile(final String conPropsFileName, final byte[] conPropsFileContents) {
        this.conPropsFileName = conPropsFileName;
        this.conPropsFileContents = conPropsFileContents;
    }
    
    

    /** 
     * @see java.lang.Object#toString()
     * @since 4.2
     */
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append(this.createFileName);
        buf.append('\n');
        buf.append(this.createFileContents);
        buf.append("\n\n"); //$NON-NLS-1$
        
        buf.append(this.loadFileName);
        buf.append('\n');
        buf.append(this.loadFileContents);
        buf.append("\n\n"); //$NON-NLS-1$
        
        buf.append(this.truncateFileName);
        buf.append('\n');
        buf.append(this.truncateFileContents);
        buf.append("\n\n"); //$NON-NLS-1$
        
        buf.append(this.swapFileName);
        buf.append('\n');
        buf.append(this.swapFileContents);
        buf.append("\n\n"); //$NON-NLS-1$
        
        buf.append(this.conPropsFileName);
        buf.append('\n');
        buf.append(this.conPropsFileContents);
        buf.append("\n\n"); //$NON-NLS-1$
        
        return buf.toString();
    }

    
    /** 
     * @return Returns the conPropsFileContents.
     * @since 4.3
     */
    public byte[] getConPropsFileContents() {
        return this.conPropsFileContents;
    }

    
    /** 
     * @return Returns the createFileContents.
     * @since 4.3
     */
    public byte[] getCreateFileContents() {
        return this.createFileContents;
    }

    
    /** 
     * @return Returns the loadFileContents.
     * @since 4.3
     */
    public byte[] getLoadFileContents() {
        return this.loadFileContents;
    }

    
    /** 
     * @return Returns the swapFileContents.
     * @since 4.3
     */
    public byte[] getSwapFileContents() {
        return this.swapFileContents;
    }

    
    /** 
     * @return Returns the truncateFileContents.
     * @since 4.3
     */
    public byte[] getTruncateFileContents() {
        return this.truncateFileContents;
    }
}
