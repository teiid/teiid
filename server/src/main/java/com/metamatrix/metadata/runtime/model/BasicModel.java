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

package com.metamatrix.metadata.runtime.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.core.vdb.ModelType;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.ModelID;
import com.metamatrix.vdb.runtime.URIResource;

public class BasicModel extends BasicMetadataObject implements Model {
    
    private static final URIResource uriresource = new URIResource();
    
    private String description;
    private boolean isPhysical;
    private boolean isVisible=true;
    private boolean multiSourceBindingEnabled;
    private Set bindingNames = null;
    private Date versionDate;
    private String versionedBy;
    
    private String GUID;
    private boolean requireConnBinding;
    private int modelType = ModelType.UNKNOWN;
    private String uri;
    
	/**
	 * Call constructor to instantiate a runtime object by passing the RuntimeID that identifies the entity and the VIrtualDatabaseID that identifes the Virtual Database the object will be contained.
	 */
    public BasicModel(BasicModelID modelID, BasicVirtualDatabaseID virtualDBID) {
	      super(modelID, virtualDBID);

    }
    
    public BasicModel(BasicModelID modelID, BasicVirtualDatabaseID virtualDBID, ModelInfo mInfo) {
          super(modelID, virtualDBID);
          
          
         setIsVisible(mInfo.isVisible());
         setModelURI(mInfo.getModelURI());
         setConnectorBindingNames(mInfo.getConnectorBindingNames());
         setVersionedBy(mInfo.getVersionedBy());
         setVersionDate(mInfo.getDateVersioned());
         setModelType(mInfo.getModelType());
         setDescription(mInfo.getDescription());
         enableMutliSourceBindings(mInfo.isMultiSourceBindingEnabled());
         
         if (mInfo.getUUID() != null) {
             this.setGUID(mInfo.getUUID());
         }         

    }    

    public String getDescription(){
        return description;
    }
    
    public boolean isPhysical() {
        return isPhysical;
    }
    
    public String getVersion() {
        return ((ModelID) this.getID()).getVersion();
    }   
    
    public boolean isVisible() {
        if (this.uri == null) {
            return false;
        }
        
        return this.isVisible;
    }
    
    
    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#isMultiSourceBindingEnabled()
     * @since 4.2
     */
    public boolean isMultiSourceBindingEnabled() {
        return this.multiSourceBindingEnabled;
    }
    
    public boolean requireConnectorBinding() {
        return this.requireConnBinding;
    }
    
    public String getGUID() {
    	if(GUID == null){
    		return ((BasicModelID)getID()).getUuid();
    	}
	    return GUID;
    }
    
   
    /** 
     * @see com.metamatrix.metadata.runtime.api.Model#getConnectorBindingNames()
     * @since 4.2
     */
    public List getConnectorBindingNames() {
        if (bindingNames == null) {
            return Collections.EMPTY_LIST;            
        }
        List bindings = null;
        
        bindings = new ArrayList(bindingNames.size());
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
     * @see com.metamatrix.metadata.runtime.api.Model#supportsMultiSourceBindings()
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
    
    
    
    public int getModelType() {
        return this.modelType;
    }
    
    public String getModelTypeName() {
        return ModelType.getString(this.modelType);
    }    
    
    public String getModelURI() {
        return this.uri;
    }
    
    /* (non-Javadoc)
     * @see com.metamatrix.metadata.runtime.api.ModelInfo#getDateVersioned()
     */
    public Date getDateVersioned() {
        return versionDate;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.metadata.runtime.api.ModelInfo#getVersionedBy()
     */
    public String getVersionedBy() {
        return versionedBy;
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
        
    public short getVisibility(){
        if (isVisible()) {
            return PUBLIC;
        } 
        return PRIVATE;
    }
    
    public  void setDescription(String desc) {
        this.description = desc;
    }
    
    private  void setIsPhysical(boolean physical) {
        this.isPhysical = physical;
    }

    
    public void setGUID(String guid){
	      this.GUID = guid;
    }
       
    /** 
     * @see com.metamatrix.common.vdb.api.ModelInfo#enableMutliSourceBindings(boolean)
     * @since 4.2
     */
    public void enableMutliSourceBindings(boolean isEnabled) {
        this.multiSourceBindingEnabled = isEnabled;
    }    

    public void addConnectorBindingName(String connectrBindingName) {
        if (this.bindingNames == null) {
            bindingNames =  new HashSet();            
        }
        
        bindingNames.add(connectrBindingName);
    }
    
    public void setConnectorBindingNames(Collection bindings) {
        bindingNames = new HashSet();
        bindingNames.addAll(bindings);
    }
    
    public void setModelType(int type) {
        this.modelType = type;

        setTypeLogic();
        
    }
    
    private void setTypeLogic() {
        if (this.modelType == ModelType.PHYSICAL ||
            this.modelType == ModelType.MATERIALIZATION ) {            
            setIsPhysical(true);     
             
            if (this.uri != null) {
                if (uriresource.isPhysicalBindingAllowed(uri)) {
                    setRequireConnectorBinding(true);                     
                } else {
                    setRequireConnectorBinding(false);  
                }
            } else {
                setRequireConnectorBinding(false); 
             
            }
            
            
        } else {
            // if no uri then the model should not be seen            
            setIsPhysical(false); 
            setRequireConnectorBinding(false);                    

        }
        
    }    
    
//    public void setVersion(String version) {
//        
//        this.version = version;
//    }
    
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
    
    public void setModelURI(String uri) {
        this.uri = uri;
        setTypeLogic();        
    }
    
//    public void setName(String modelName){
//        ((BasicModelID)getID()).setName(modelName, false);
//    }
    
    public void setIsVisible(boolean isVisible){
        this.isVisible = isVisible;
    }
    
    public void setVisibility(short visibility) {
        if (visibility == PUBLIC) {
            setIsVisible(true);
        } else {
            setIsVisible(false);
        }
    }    
    
    private void setRequireConnectorBinding(boolean requireConnBinding){
    	this.requireConnBinding = requireConnBinding;	
    }
    

    public String toString() {
        StringBuffer sw = new StringBuffer();
        
        sw.append("Model: " + this.getName());//$NON-NLS-1$
        sw.append("\n\tVersion: " + this.getVersion());//$NON-NLS-1$        
        sw.append("\n\tType: " + this.getModelTypeName());//$NON-NLS-1$
        sw.append("\n\thasBinding: " + (this.getConnectorBindingNames().size() > 0));//$NON-NLS-1$
        sw.append("\n\tIsVisible: " + this.isVisible());//$NON-NLS-1$
        sw.append("\n\tIsPhysical: " + this.isPhysical());//$NON-NLS-1$
        sw.append("\n\tURI: " + this.getModelURI());//$NON-NLS-1$
        sw.append("\n\tRequiresBinding: " + this.requireConnectorBinding());//$NON-NLS-1$

        
        return sw.toString();
        
    }
}

