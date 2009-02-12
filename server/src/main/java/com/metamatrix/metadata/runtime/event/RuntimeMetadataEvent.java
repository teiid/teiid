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

package com.metamatrix.metadata.runtime.event;

import java.util.*;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;

public class RuntimeMetadataEvent extends EventObject{
	public static final int REFRESH_VDB = 0;
	public static final int REFRESH_MODELS = 1;
	public static final int DELETE_VDB = 2;
	public static final int REFRESH_VDB_AND_MODELS = 3;
    public static final int CLEAR_CACHE_FOR_VDB = 4;    
    public static final int ADD_VDB = 5;
    
    //private boolean refreshVDB;
    //private boolean refreshModels;
    private VirtualDatabaseID vdbID;
    private int action;

    //public RuntimeMetadataEvent(Object source, VirtualDatabaseID vdbID, boolean refreshVDB, boolean refreshModels) {
    //    super(source);
    //    this.refreshVDB = refreshVDB;
    //    this.refreshModels = refreshModels;
    //    this.vdbID = vdbID;
    //}
    
    public RuntimeMetadataEvent(Object source, VirtualDatabaseID vdbID, int action) {
        super(source);
        this.action = action;
        this.vdbID = vdbID;
    }

    public boolean refreshVDB(){
        return action == REFRESH_VDB || action == REFRESH_VDB_AND_MODELS;
    }

    public boolean refreshModels(){
        return action == REFRESH_MODELS || action == REFRESH_VDB_AND_MODELS;
    }

	public boolean deleteVDB(){
		return action == DELETE_VDB;	
	}
    
    public boolean clearCacheForVDB(){
        return action == CLEAR_CACHE_FOR_VDB;    
    }    
    
    public boolean createVDB() {
        return action == ADD_VDB; 
    }
	
    public VirtualDatabaseID getVirtualDatabaseID(){
        return vdbID;
    }
} 
