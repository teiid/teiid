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

package com.metamatrix.metadata.runtime.vdb.defn;

import java.util.Collections;
import java.util.List;

import com.metamatrix.common.vdb.api.VDBArchive;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;


public class VDBDefnImport {
    
    public static VirtualDatabase importVDBDefn(VDBArchive vdbArchive, String principal) throws Exception {
        return importVDBDefn(vdbArchive, principal, false);
    }
    
    public static VirtualDatabase importVDBDefn(VDBArchive vdbArchive, String principal, boolean updateExistingBinding) throws Exception {
        return importVDBDefn(vdbArchive, principal, updateExistingBinding, Collections.EMPTY_LIST);        
    }  
    
    public static VirtualDatabase importVDBDefn(VDBArchive vdb, String principal, boolean updateExistingBinding, List vms) throws Exception {
        VDBCreation vdbc = new VDBCreation();
        vdbc.setUpdateBindingProperties(updateExistingBinding);
        vdbc.setVMsToDeployBindings(vms);
        return vdbc.loadVDBDefn(vdb,principal);
    } 

}
