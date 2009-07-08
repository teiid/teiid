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
import java.util.List;

import com.metamatrix.metadata.runtime.api.Procedure;
import com.metamatrix.metadata.runtime.api.ProcedureParameter;

public class BasicProcedure extends BasicMetadataObject implements Procedure {
        private String description;
        private String path;
        private List parameters;
        private boolean returnsResults;
        private short procedureType;//01 = Stored Procedure, 2 = Function, 3 = Stored Query
        private String alias;
        private String queryPlan; //If the Basic Proc represents a StoredQuery it will have aQP
    /**
     *@link dependency
     */

    /*#BasicProcedureParameter lnkBasicProcedureParameter;*/
/**
 * Call constructor to instantiate a runtime object by passing the RuntimeID that identifies the entity and the VIrtualDatabaseID that identifes the Virtual Database the object will be contained.
 */
    public BasicProcedure(BasicProcedureID procedureID, BasicVirtualDatabaseID virtualDBID) {
        super(procedureID, virtualDBID);
    }

    public String getPath() {
	    if(path != null)
	        return path;
        return getID().getPath();
    }
    
    public String getDescription() {
        return this.description;
    }
    public String getAlias(){
        return alias;
    }
    public void setAlias(String alias){
        this.alias = alias;
    }
    public List getParameters() {
	      return parameters;
    }
    public void addParameter(ProcedureParameter pp){
        if(this.parameters == null)
            parameters = new ArrayList();
        parameters.add(pp);
    }
    public boolean returnsResults() {
	      return this.returnsResults;
    }
    public short getProcedureType() {
	      return this.procedureType;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setPath(String path) {
	      this.path = path;
    }
    public void setParameters(List params) {
	      this.parameters = params;
    }
    public void setReturnsResults(boolean returnsResults) {
	      this.returnsResults = returnsResults;
    }
    public void setProcedureType(short type) {
	      this.procedureType = type;
    }
    /**
     * Returns the queryPlan.
     * @return String
     */
    public String getQueryPlan() {
        return queryPlan;
    }

    /**
     * Sets the queryPlan.
     * @param queryPlan The queryPlan to set
     */
    public void setQueryPlan(String queryPlan) {
        this.queryPlan = queryPlan;
    }
    
    public boolean isStoredQuery(){
        return getProcedureType() == 3;
    }

}

