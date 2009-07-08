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

import org.teiid.metadata.RuntimeMetadataPlugin;

import com.metamatrix.common.log.LogManager;
import com.metamatrix.core.MetaMatrixRuntimeException;
import com.metamatrix.metadata.runtime.api.GroupID;
import com.metamatrix.metadata.runtime.api.ModelID;
import com.metamatrix.metadata.runtime.util.LogRuntimeMetadataConstants;
import com.metamatrix.metadata.util.ErrorMessageKeys;

public class BasicGroupID extends BasicMetadataID implements GroupID {
    private ModelID modelID = null;
    private String alias = null;

/**
 * Call constructor to instantiate a BasicGroupID object for the fully qualified Group name and an internal unique identifier.
 */
    public BasicGroupID(String fullName, long internalUniqueID) {
        super(fullName, internalUniqueID);
        if(this.getNameComponents().size() < 2){
            LogManager.logDetail(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA,new Object[]{"Invalid GroupID \"",fullName,"\". Number of name components must be > 1."});
            throw new MetaMatrixRuntimeException (ErrorMessageKeys.BGID_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.BGID_0001) );
        }
    }

/**
 * Call constructor to instantiate a BasicGroupID object for the fully qualified Group name and uid.
 */
    public BasicGroupID(String fullName){
        super(fullName);
        if(this.getNameComponents().size() < 2){
            LogManager.logDetail(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA,new Object[]{"Invalid GroupID \"",fullName,"\". Number of name components must be > 1."});
            throw new MetaMatrixRuntimeException (ErrorMessageKeys.BGID_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.BGID_0001) );
        }
    }
/*
 * @return ModelID is the model the key is contained in
 */
    public ModelID getModelID() {
        if (modelID != null) {
                return modelID;
        }
        modelID = new BasicModelID(this.getNameComponent(0));
        return modelID;
    }

    public String getModelName(){
        return this.getNameComponent(0);
    }

    public void setModelID(ModelID id){
        this.modelID = id;
    }

    public String getAlias(){
        return this.alias;
    }
    void setAlias(String alias){
        this.alias = alias;
    }

}

