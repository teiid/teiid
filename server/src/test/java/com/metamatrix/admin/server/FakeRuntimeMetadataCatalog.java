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

package com.metamatrix.admin.server;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.model.BasicModel;
import com.metamatrix.metadata.runtime.model.BasicModelID;
import com.metamatrix.metadata.runtime.model.BasicVirtualDatabase;
import com.metamatrix.metadata.runtime.model.BasicVirtualDatabaseID;


/**
 * Fake RuntimeMetadataCatalog for testing purposes
 */
public class FakeRuntimeMetadataCatalog  {

    private static String VDB_NAME1 = "myVdb1"; //$NON-NLS-1$
    private static String VDB_NAME2 = "myVdb2"; //$NON-NLS-1$
    private static String VERSION1 = "1"; //$NON-NLS-1$
    private static String PHYSICAL_MODEL_NAME1 = "PhysicalModel1"; //$NON-NLS-1$
    private static String PHYSICAL_MODEL_NAME2 = "PhysicalModel2"; //$NON-NLS-1$

    private static Map vdbMap = new HashMap();
    private static Map modelsMap = new HashMap();
    
    static {
    	VirtualDatabase vdb1 = helpGetVirtualDatabaseWithPhysicalModel();
    	vdbMap.put(vdb1.getVirtualDatabaseID(),vdb1);
    	VirtualDatabase vdb2 = helpGetVirtualDatabaseWithMultiEnabledPhysicalModel();
    	vdbMap.put(vdb2.getVirtualDatabaseID(),vdb2);
    }
    
    public static VirtualDatabase helpGetVirtualDatabaseWithPhysicalModel() {
    	BasicVirtualDatabaseID vdbid = new BasicVirtualDatabaseID(VDB_NAME1, VERSION1); 
    	BasicVirtualDatabase vdb = new BasicVirtualDatabase(vdbid);
    	
    	BasicModelID mid = new BasicModelID(PHYSICAL_MODEL_NAME1, VERSION1); 
    	BasicModel model = new BasicModel(mid, vdbid);
    	vdb.addModelID(mid);
    	
    	Collection models = new HashSet();
    	models.add(model);

    	modelsMap.put(vdbid,models);
    	
        return vdb;        
    }
    
    public static VirtualDatabase helpGetVirtualDatabaseWithMultiEnabledPhysicalModel() {
    	BasicVirtualDatabaseID vdbid = new BasicVirtualDatabaseID(VDB_NAME2, VERSION1); 
    	BasicVirtualDatabase vdb = new BasicVirtualDatabase(vdbid);
    	
    	BasicModelID mid = new BasicModelID(PHYSICAL_MODEL_NAME2, VERSION1); 
    	BasicModel model = new BasicModel(mid, vdbid);
    	model.enableMutliSourceBindings(true);
    	vdb.addModelID(mid);
    	
    	Collection models = new HashSet();
    	models.add(model);

    	modelsMap.put(vdbid,models);
    	
        return vdb;        
    }

    public static Collection getVirtualDatabases()  {
    	return vdbMap.values();
    }

    public static Collection getModels(VirtualDatabaseID vdbID) {
    	return (Collection)modelsMap.get(vdbID);
    }
    
    public static void setConnectorBindingNames(VirtualDatabaseID vdbID, Map modelAndCBNames, String userName)throws VirtualDatabaseException{
    	// Get models for the VDB
    	Collection models = getModels(vdbID);
    	
    	Iterator mapKeyIter = modelAndCBNames.keySet().iterator();
    	while(mapKeyIter.hasNext()) {
    		String keyName = (String)mapKeyIter.next();
    		// Find matching model
        	Iterator iter = models.iterator();
        	while(iter.hasNext()) {
        		BasicModel model = (BasicModel)iter.next();
        		if(model.getName().equals(keyName)) {
        			Collection cbNames = (Collection)modelAndCBNames.get(keyName);
        			model.setConnectorBindingNames(cbNames);
        		}
        		
        	}
    	}
    	
    }
    
    public static void setVDBStatus(VirtualDatabaseID virtualDBID, short status, String userName) {
    }

}