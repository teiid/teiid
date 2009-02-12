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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.metamatrix.common.vdb.api.ModelInfo;
import com.metamatrix.common.vdb.api.VDBInfo;

/**
 */
public class BasicVDBInfo implements VDBInfo, Serializable {
    private String fileName;
    private String name;
    private String uuid;
    private String desc;
    private Date created;
    private String createdBy;
    protected Map modelInfos=Collections.synchronizedMap(new HashMap());
    private boolean WSDLDefined=false;
    private String version;
    
    public BasicVDBInfo(String vdbName){
        this.name = vdbName;
    }
       
    /* (non-Javadoc)
     * @see com.metamatrix.metadata.runtime.api.VDBInfo#getName()
     */
    public String getName() {
        return name;
    }
    
    public void setName(String name) {
        this.name = name;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.metadata.runtime.api.VDBInfo#getUUID()
     */
    public String getUUID() {
        return uuid;
    }

    /**
     * Returns the vdbVersion.
     * @return String
     */
    public String getVersion() {
        return version;
    }
    
    /* (non-Javadoc)
     * @see com.metamatrix.metadata.runtime.api.VDBInfo#getDescription()
     */
    public String getDescription() {
        return desc;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.metadata.runtime.api.VDBInfo#getDateCreated()
     */
    public Date getDateCreated() {
        return created;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.metadata.runtime.api.VDBInfo#getCreatedBy()
     */
    public String getCreatedBy() {
        return createdBy;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.metadata.runtime.api.VDBInfo#getModels()
     */
    public Collection getModels() {
        if (modelInfos == null || modelInfos.size() == 0) {
            return Collections.EMPTY_LIST;
        }

        Collection models = new ArrayList(modelInfos.size());
        models.addAll(modelInfos.values());
        return models;
    }
    
    /**
     * Returns the {@link ModelInfo ModelInfo} for the name specified
     * @return ModelInfo
     * 
     */
    public ModelInfo getModel(String name) {
        return (ModelInfo) modelInfos.get(name);
    }
    
    /**
     * Returns true if a WSDL is defined for this VDB
     * @return true if a WSDL is defined for this VDB
     */
    public boolean hasWSDLDefined() {
        return WSDLDefined;
    }

    /**
     * @param date
     */
    public void setDateCreated(Date date) {
        created = date;
    }

    /**
     * @param createdBy
     */
    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    /**
     * @param description
     */
    public void setDescription(String description) {
        desc = description;
    }

    /**
     * @param modelInfos
     */
    public void setModelInfos(final Collection models) {
        modelInfos.clear();
        for (Iterator it=models.iterator(); it.hasNext();) {
            ModelInfo mi = (ModelInfo) it.next();
            modelInfos.put(mi.getName(), mi);
        }        
    }

    public void addModelInfo(ModelInfo model) {
        if (model == null) {
            return;
        }
        // first remove an existing model,
        // model names are assumed unique
        removeModelInfo(model.getName());
        this.modelInfos.put(model.getName(), model);        
    }
    
    public ModelInfo removeModelInfo(String modelName) {
        if (modelName == null || modelName.length() == 0) {
            return null;
        }
        if (this.modelInfos != null) {
            return  (ModelInfo) modelInfos.remove(modelName);
        }
        return null;
    }
    
  
    /**
     * @param objectID
     */
    public void setUUID(String objectID) {
        uuid = objectID;
    }
    
    public void setHasWSDLDefined(boolean defined ) {
        WSDLDefined = defined;
    }

    /* (non-Javadoc)
     * @see com.metamatrix.metadata.runtime.api.VDBInfo#getFileName()
     */
    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
    
    public void setVersion(String version) {
        this.version = version;
    }
    

    public String toString() {
        StringBuffer sb = new StringBuffer("\nDefn for VDB " + getName() + " and # models " + (modelInfos==null?0:modelInfos.size())); //$NON-NLS-1$ //$NON-NLS-2$
        return sb.toString();

    }    
}
