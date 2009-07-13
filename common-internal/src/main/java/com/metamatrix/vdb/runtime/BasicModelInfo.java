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

/*
 */
package com.metamatrix.vdb.runtime;

import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.metamatrix.common.util.ByteArrayHelper;
import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.core.vdb.ModelType;

/**
 */
public class BasicModelInfo implements ModelInfo, Serializable { 
    
    
    private static final URIResource uriresource = new URIResource();
    
    private String name;
    private String uuid;
    private String version;
    private Date versionDate;
    private String versionedBy;
    private String description;
    private boolean isPhysical;
    private boolean requireConnBinding;
    private String pathInVdb;
    
    // contains the binding name    
    private Set bindingNames = Collections.synchronizedSet(new HashSet());
    private boolean multiSourceBindingEnabled;

    
    private int modelType = ModelType.UNKNOWN;
    private String uri=null;
    
    private boolean isVisible;

    private Map ddlFileNamesToFiles = Collections.EMPTY_MAP;
    
    private Properties properties;

    protected BasicModelInfo() {
        
    }
    
    /**
     * CTOR used when the vdb is being sent to the 
     * runtime for creation.
     * @param model
     */
    public BasicModelInfo(ModelInfo model) {
         this(model.getName());
         this.setModelType(model.getModelType());
         this.setModelURI(model.getModelURI());
         this.setVersion(model.getVersion());
         this.setUuid(model.getUUID());
         this.setConnectorBindingNames(model.getConnectorBindingNames());
         this.setIsVisible(model.isVisible());
         this.enableMutliSourceBindings(model.isMultiSourceBindingEnabled());
         this.setDescription(model.getDescription());         
         this.setVersionDate(model.getDateVersioned());
         this.setVersionedBy(model.getVersionedBy());
         this.setPath(model.getPath());
    }
    
        

    public BasicModelInfo(String modelName){
        this.name = modelName;
    }
    
    public String getUUID() {
        return uuid!=null?uuid:"NoUUID";//$NON-NLS-1$
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public String getVersion() {
        return version!=null?version:"0"; //$NON-NLS-1$
    }
    
    public int getModelType() {
        return this.modelType;
    }
    
    public String getModelTypeName() {
        return ModelType.MODEL_NAMES[this.modelType];
    }
    
    public String getModelURI() {
        return this.uri;
    }    


    public Date getDateVersioned() {
        return versionDate;
    }


    public String getVersionedBy() {
        return versionedBy;
    }


    public boolean isPhysical() {
        return isPhysical;
    }
    
    public boolean requiresConnectorBinding() {
        return requireConnBinding;
    }
    
    public List getConnectorBindingNames() {
        if (bindingNames.isEmpty()) {
            return Collections.EMPTY_LIST;            
        }
        
        List bindings = new ArrayList(bindingNames.size());
        bindings.addAll(this.bindingNames);
        return bindings;
    }
    

    /** 
     * Returns true if the model, based on its model type,
     * supports mutliple connector bindings.  
     * If true, {@see #isMultiSourceBindingEnabled()} to determine
     * if the model has been flagged so that the user can
     * actually assign multi connector bindngs.
     * 
     * @see org.teiid.adminapi.Model#supportsMultiSourceBindings()
     * @since 4.2
     */
    public boolean supportsMultiSourceBindings() {
		switch (modelType) {
		case ModelType.PHYSICAL: {
			return true;
		}
		default: {
			return false;
		}
		}

	}    
    
    
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @param uuid
     */
    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    /**
     * @param version
     */
    public void setVersion(String version) {
        this.version = version;
    }

    /**
     * @param date
     */
    public void setVersionDate(Date date) {
        versionDate = date;
    }

    /**
     * @param versionedBy
     */
    public void setVersionedBy(String versionedBy) {
        this.versionedBy = versionedBy;
    }
    
    public boolean isVisible() {
        if (this.uri == null) {
            return false;
        }        
        return isVisible;
    }

    /** 
     * Check whether this model is a materialization
     * of a virtual group.
     * @return Returns the isMaterialization.
     * @since 4.2
     */
    public boolean isMaterialization() {
        return (this.modelType ==  ModelType.MATERIALIZATION);            
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
     * @see com.metamatrix.common.vdb.api.ModelInfo#setDDLFiles(Map)
     * @param ddlFileNamesToFiles
     * @since 4.2
     */
    public void setDDLFiles(Map ddlFileNamesToFiles) {
        this.ddlFileNamesToFiles = ddlFileNamesToFiles;
    }
            
    public void setIsVisible(boolean visibility) {
        this.isVisible = visibility;
    }
    
    public void setVisibility(short visibility) {
        if (visibility == PUBLIC) {
            setIsVisible(true);
        } else {
            setIsVisible(false);
        }
    }
    
    public short getVisibility()  {
        if (isVisible) {
            return PUBLIC;
        } 
        return PRIVATE;
    }
    
    boolean isConnectorBindingUsed(String bindingName) {
        return this.bindingNames.contains(bindingName);
    }
    
    public void addConnectorBindingByName(String connectrBindingName) {
        if (this.bindingNames == null) {
            bindingNames =  new HashSet();            
        }
        
        bindingNames.add(connectrBindingName);
    }      
    
    
    public void setConnectorBindingNames(List newbindingNames) {
        this.bindingNames.clear();
        this.bindingNames.addAll(newbindingNames);
    }
        
    
    public void removeConnectorBindingName(String bindingName) {
        this.bindingNames.remove(bindingName);
    }       
      
    
    public void renameConnectorBinding(String exitingbindingName, String newbindingname) {
        
        boolean removed = this.bindingNames.remove(exitingbindingName);
        if (removed) {
            addConnectorBindingByName(newbindingname);
        }
    }      
    
    public void setModelType(int type) {
        this.modelType = type;

        setTypeLogic();        
    }
    
    public void setModelURI(String uri) {
        this.uri = uri;
        setTypeLogic();
    }
    
    public void setDescription(String desc) {
        this.description = desc;
    }
    
    private void setTypeLogic() {
        if (this.modelType == ModelType.PHYSICAL ||
            this.modelType == ModelType.MATERIALIZATION ) {            
            setIsPhysical(true);     
             
            if (this.uri != null && this.uri.trim().length()  > 0) {
                if (uriresource.isPhysicalBindingAllowed(uri)) {
                    setRequireConnectorBinding(true);
                } else {
                    setRequireConnectorBinding(false);  
                }
            } else {
                setRequireConnectorBinding(false); 
             
            }
            
            
        } else {
            
            setIsPhysical(false); 
            setRequireConnectorBinding(false);                    

        }
        
    }
    

    private void setRequireConnectorBinding(boolean requireConnBinding){
        this.requireConnBinding = requireConnBinding;   
    }
    
       
    private void setIsPhysical(boolean isPhysical) {
        this.isPhysical = isPhysical;
    }  
    
    

    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#enableMutliSourceBindings(boolean)
     * @since 4.2
     */
    public void enableMutliSourceBindings(boolean isEnabled) {
        this.multiSourceBindingEnabled = isEnabled;
    }
    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#isMultiSourceBindingEnabled()
     * @since 4.2
     */
    
    
    public boolean isMultiSourceBindingEnabled() {
        return this.multiSourceBindingEnabled;
    }
    
    public String getPath() {
    	return this.pathInVdb!=null?pathInVdb:"NoPath";//$NON-NLS-1$
    }
    
    public void setPath(String path) {
    	this.pathInVdb = path;
    }
    
    @Override
    public Properties getProperties() {
    	return this.properties;
    }
    
    public void setProperties(Properties properties) {
		this.properties = properties;
	}
    
    public String toString() {
        StringBuffer sw = new StringBuffer();
        
        sw.append("ModelInfo: " + this.getName());//$NON-NLS-1$
        sw.append("\n\tVersion: " + this.getVersion());//$NON-NLS-1$    
        sw.append("\n\tTypeCode: " + this.getModelType());//$NON-NLS-1$

        sw.append("\n\tType: " + this.getModelTypeName());//$NON-NLS-1$
        sw.append("\n\thasBindings: " + (this.getConnectorBindingNames().size() > 0));//$NON-NLS-1$
        
        sw.append("\n\tIsVisible: " + this.isVisible());//$NON-NLS-1$
        sw.append("\n\tIsPhysical: " + this.isPhysical());//$NON-NLS-1$
        sw.append("\n\tIsMaterialization: " + this.isMaterialization());//$NON-NLS-1$
        sw.append("\n\tURI: " + this.getModelURI());//$NON-NLS-1$
        sw.append("\n\tRequiresBinding: " + this.requiresConnectorBinding());//$NON-NLS-1$

        
        return sw.toString();
        
    }

}
