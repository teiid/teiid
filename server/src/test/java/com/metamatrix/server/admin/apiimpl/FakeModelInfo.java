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
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.common.util.ByteArrayHelper;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.core.vdb.ModelType;


/** 
 * @since 4.2
 */
public class FakeModelInfo implements ModelInfo {
    
    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#enableMutliSourceBindings(boolean)
     * @since 4.2
     */
    public void enableMutliSourceBindings(boolean isEnabled) {
        
    }
    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#isMultiSourceBindingEnabled()
     * @since 4.2
     */
    public boolean isMultiSourceBindingEnabled() {
        return false;
    }
    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#getDescription()
     * @since 4.2
     */
    public String getDescription() {
        return null;
    }
    private int modelType;

    private Map ddlFileNamesToFiles;

    /** 
     * @param modelType
     * @since 4.2
     */
    public FakeModelInfo(int modelType) {
        super();
        this.modelType = modelType;
    }
    
    
    
    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#getConnectorBindingNames()
     * @since 4.3
     */
    public List getConnectorBindingNames() {
        return null;
    }
    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#setDDLFiles(java.util.Map)
     * @since 4.2
     */
    public void setDDLFiles(Map ddlFileNamesToFiles) {
        this.ddlFileNamesToFiles = ddlFileNamesToFiles;
    }

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#supportsMultiSourceBindings()
     * @since 4.2
     */
    public boolean supportsMultiSourceBindings() {
        return true;
    }
    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#getDDLFileNames()
     * @since 4.2
     */
    public String[] getDDLFileNames() {
        if ( this.modelType == ModelType.MATERIALIZATION ) {
            Set keys = this.ddlFileNamesToFiles.keySet();
            String[] ddlFileNames = new String[keys.size()];
            Iterator fileNameItr = keys.iterator();
            for ( int i=0; fileNameItr.hasNext(); i++ ) {
                String afileName = (String) fileNameItr.next();
                ddlFileNames[i] = afileName;
            }
            return ddlFileNames;
        }
        return new String[] {};
    }

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#getDDLFileContentsAsStream(java.lang.String)
     * @since 4.2
     */
    public InputStream getDDLFileContentsAsStream(String ddlFileName) {
        if ( this.modelType == ModelType.MATERIALIZATION ) {
            byte[] ddlFile = (byte[]) this.ddlFileNamesToFiles.get(ddlFileName);
            if (ddlFile == null) {
                return null;
            }
            InputStream fileStream;
            try {
                fileStream = ByteArrayHelper.toInputStream(ddlFile);
            } catch (Exception err) {
                return null;
            }
            return fileStream;
        }
        
        return null;
    }

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#getDDLFileContentsGetBytes(java.lang.String)
     * @since 4.2
     */
    public byte[] getDDLFileContentsGetBytes(String ddlFileName) {
        byte[] ddlFile = null;
        if ( this.modelType == ModelType.MATERIALIZATION ) {
            ddlFile = (byte[]) this.ddlFileNamesToFiles.get(ddlFileName);
        }
        return ddlFile;
    }
    
    // ===================================================================
    // NOT IMPLEMENTED
    // ===================================================================
    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#getUUID()
     * @since 4.2
     */
    public String getUUID() {
        return null;
    }

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#getName()
     * @since 4.2
     */
    public String getName() {
        return null;
    }

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#getVersion()
     * @since 4.2
     */
    public String getVersion() {
        return null;
    }

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#getDateVersioned()
     * @since 4.2
     */
    public Date getDateVersioned() {
        return null;
    }

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#getVersionedBy()
     * @since 4.2
     */
    public String getVersionedBy() {
        return null;
    }

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#isPhysical()
     * @since 4.2
     */
    public boolean isPhysical() {
        return false;
    }

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#requiresConnectorBinding()
     * @since 4.2
     */
    public boolean requiresConnectorBinding() {
        return false;
    }

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#getConnectorBindingUUID()
     * @since 4.2
     */
//    public List getConnectorBindingUUIDS() {
//        return Collections.EMPTY_LIST;
//    }

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#isVisible()
     * @since 4.2
     */
    public boolean isVisible() {
        return false;
    }

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#getVisibility()
     * @since 4.2
     */
    public short getVisibility() {
        return 0;
    }

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#isMaterialization()
     * @since 4.2
     */
    public boolean isMaterialization() {
        return false;
    }

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#getModelType()
     * @since 4.2
     */
    public int getModelType() {
        return 0;
    }

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#getModelURI()
     * @since 4.2
     */
    public String getModelURI() {
        return null;
    }

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#getModelTypeName()
     * @since 4.2
     */
    public String getModelTypeName() {
        return null;
    }
	public String getPath() {
		return null;
	}
	
	@Override
	public Properties getProperties() {
		return new Properties();
	}
}
