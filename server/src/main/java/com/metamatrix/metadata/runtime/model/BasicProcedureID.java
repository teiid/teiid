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
import com.metamatrix.metadata.runtime.api.ModelID;
import com.metamatrix.metadata.runtime.api.ProcedureID;
import com.metamatrix.metadata.runtime.util.LogRuntimeMetadataConstants;
import com.metamatrix.metadata.util.ErrorMessageKeys;
public class BasicProcedureID extends BasicMetadataID implements ProcedureID {
    private ModelID modelID=null;


/**
 * Call constructor to instantiate a BasicProcedureID object for the fully qualified name and an internal unique identifier.
 */
    public BasicProcedureID(String fullName, long internalUniqueID) {
        super(fullName, internalUniqueID);
        if(this.getNameComponents().size() < 2){
            LogManager.logDetail(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA,new Object[]{"Invalid ProcedureID \"",fullName,"\". Number of name components must be > 1."});
            throw new MetaMatrixRuntimeException(ErrorMessageKeys.BPID_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.BPID_0001) );
        }
    }

/**
 * Call constructor to instantiate a BasicProcedureID object for the fully qualified name.
 */
    public BasicProcedureID(String fullName){
        super(fullName);
        if(this.getNameComponents().size() < 2){
            LogManager.logDetail(LogRuntimeMetadataConstants.CTX_RUNTIME_METADATA,new Object[]{"Invalid ProcedureID \"",fullName,"\". Number of name components must be > 1."});
            throw new MetaMatrixRuntimeException(ErrorMessageKeys.BPID_0001, RuntimeMetadataPlugin.Util.getString(ErrorMessageKeys.BPID_0001) );
        }
    }


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

    public void setModelID(ModelID modelID){
        this.modelID = modelID;
    }
}

