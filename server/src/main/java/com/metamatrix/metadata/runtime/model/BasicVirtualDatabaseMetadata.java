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
import java.util.Iterator;
import java.util.List;

import com.metamatrix.metadata.runtime.api.Element;
import com.metamatrix.metadata.runtime.api.Group;
import com.metamatrix.metadata.runtime.api.GroupID;
import com.metamatrix.metadata.runtime.api.MetadataSourceAPI;
import com.metamatrix.metadata.runtime.api.Model;
import com.metamatrix.metadata.runtime.api.ModelID;
import com.metamatrix.metadata.runtime.api.Procedure;
import com.metamatrix.metadata.runtime.api.VirtualDatabase;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseException;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseID;
import com.metamatrix.metadata.runtime.api.VirtualDatabaseMetadata;

public class BasicVirtualDatabaseMetadata implements VirtualDatabaseMetadata{
    

    private MetadataSourceAPI metadataSource = null;
    private VirtualDatabaseID vdbID = null;


    public BasicVirtualDatabaseMetadata(MetadataSourceAPI metadataSource, VirtualDatabaseID id) {
        this.metadataSource = metadataSource;
        this.vdbID = id;
    }
    
    public boolean isModelDetailsLoaded() {
        return metadataSource.isModelDetailsLoaded();
    }


    /**
     * Obtain a collection of ModelID's deployed in this VirtualDatabase metadata.
     * @return Collection of ModelID's
     * @throws VirtualDatabaseException if an error occurs while trying to read the data.
     */
    public Collection getDisplayableModels()  throws VirtualDatabaseException {
        return metadataSource.getDisplayableModels();
//        getModels();
    }
    
    public Collection getAllModels()  throws VirtualDatabaseException  {
        return metadataSource.getAllModels();
        
    }
    
    public Model getModel(String name)  throws VirtualDatabaseException {
        ModelID modelID = new BasicModelID(name);
        return metadataSource.getModel(modelID);
    }
    
    /**
     * Returns the visibility for a resource path. 
     * @param resourcePath
     * @return <code>true</code> if the resource is visible.
     * @since 4.2
     */
    public boolean isVisible(String resourcePath) throws VirtualDatabaseException {
        return metadataSource.isVisible(resourcePath);
    }
    

    /**
     * returns the <code>VirtualDatabase</code> for which this metadata object represents.
     * @return VirtualDatabase
     * @throws VirtualDatabaseException if an error occurs while trying to read the data.
     */
    public VirtualDatabase getVirtualDatabase() throws VirtualDatabaseException {
	    return metadataSource.getVirtualDatabase();
    }

    /**
     * returns the VirtualDatabaseID that identifies the VirtualDatabaseMetadata.
     * @return VirtualDatabaseID
     */
    public VirtualDatabaseID getVirtualDatabaseID() {
	    return vdbID;
    }

 

    public List getALLPaths(Collection models) throws  VirtualDatabaseException{
        List result = new ArrayList();
 //       Collection models = getDisplayableModels();
        Iterator iter = models.iterator();
        ModelID modelID;
        Collection groups;
        Group group;
        List elements;
        while(iter.hasNext()){
            modelID = (ModelID)((Model)iter.next()).getID();           
            groups = metadataSource.getGroupsInModel(modelID);
            Iterator iter1 = groups.iterator();
            while(iter1.hasNext()){
                group = (Group) iter1.next();
                result.add(group.getFullName());
                elements = getElementsInGroup((GroupID) group.getID());
                if(elements != null) {
                    Iterator iter2 = elements.iterator();
                    while(iter2.hasNext()){
                        Element e = (Element) iter2.next();
                        result.add(e.getFullName());
                    }
                }
            }
            Collection procedures = null;
            Procedure procedure = null;
            procedures =  metadataSource.getProceduresInModel(modelID);
            Iterator iter2 = procedures.iterator();
            while(iter2.hasNext()) {
                procedure = (Procedure) iter2.next();
                result.add(procedure.getFullName());
            }
        }
        return result;
    }
    

    /**
     * Obtain a collection of ProcedureID's for the specified modelID.
     * @param modelID is the id for the Model
     * @return Collection of ProcedureID's
     * @throws VirtualDatabaseException if an error occurs while trying to read the data.
     */
    public Collection getProcedures(ModelID modelID)  throws VirtualDatabaseException {
        return metadataSource.getProceduresInModel(modelID);
    }

    
    /**
     * Return an <b>ordered</b> list of ElementID's for the specified groupID.
     * @param groupID is the group for which the elements are to be obtained.
     * @return List of Element's.
     * @throws VirtualDatabaseException if an error occurs while trying to read the data.
     */
    public List getElementsInGroup(GroupID groupID) throws  VirtualDatabaseException {
        return metadataSource.getElementsInGroup(groupID);
    }

    

    /**
     * Obtain a collection of Group for the specified modelID.
     * @param modelID is the id for the Model
     * @return Collection of ProcedureID's
     * @throws VirtualDatabaseException if an error occurs while trying to read the data.
     */
    public Collection getGroupsInModel(ModelID modelID)  throws VirtualDatabaseException {
        return metadataSource.getGroupsInModel(modelID);
    }


}

