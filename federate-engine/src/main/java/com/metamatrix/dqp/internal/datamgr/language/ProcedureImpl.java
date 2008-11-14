/*
 * JBoss, Home of Professional Open Source.
 * Copyright (C) 2008 Red Hat, Inc.
 * Copyright (C) 2000-2007 MetaMatrix, Inc.
 * Licensed to Red Hat, Inc. under one or more contributor 
 * license agreements.  See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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

package com.metamatrix.dqp.internal.datamgr.language;

import java.util.List;

import com.metamatrix.data.language.IProcedure;
import com.metamatrix.data.metadata.runtime.MetadataID;
import com.metamatrix.data.visitor.framework.LanguageObjectVisitor;

public class ProcedureImpl extends BaseLanguageObject implements IProcedure {

    private String name;
    private List parameters;
    private MetadataID metadataID;
    
    public ProcedureImpl(String name, List parameters, MetadataID metadataID) {
        this.name = name;
        this.parameters = parameters;
        this.metadataID = metadataID;
    }
    
    /**
     * @see com.metamatrix.data.language.IExecute#getProcedureName()
     */
    public String getProcedureName() {
        return this.name;
    }

    /**
     * @see com.metamatrix.data.language.IExecute#getVariableValues()
     */
    public List getParameters() {
        return parameters;
    }

    /**
     * @see com.metamatrix.data.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /* 
     * @see com.metamatrix.data.language.IExecute#setProcedureName(java.lang.String)
     */
    public void setProcedureName(String name) {
        this.name = name;
    }

    /* 
     * @see com.metamatrix.data.language.IExecute#setParameters(java.util.List)
     */
    public void setParameters(List parameters) {
        this.parameters = parameters;
    }

    /* 
     * @see com.metamatrix.data.language.IMetadataReference#getMetadataID()
     */
    public MetadataID getMetadataID() {
        return this.metadataID;
    }

    /* 
     * @see com.metamatrix.data.language.IMetadataReference#setMetadataID(com.metamatrix.data.metadata.runtime.MetadataID)
     */
    public void setMetadataID(MetadataID metadataID) {
        this.metadataID = metadataID;
    }
    
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }
        
        if(obj == null) {
            return false;
        }
        
        IProcedure proc = (IProcedure) obj;
        return getProcedureName().equalsIgnoreCase(proc.getProcedureName());
    }

}
