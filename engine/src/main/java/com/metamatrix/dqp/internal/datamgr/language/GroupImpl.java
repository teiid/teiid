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

import com.metamatrix.connector.language.IGroup;
import com.metamatrix.connector.metadata.runtime.MetadataID;
import com.metamatrix.connector.visitor.framework.LanguageObjectVisitor;
import com.metamatrix.core.util.HashCodeUtil;

public class GroupImpl extends BaseLanguageObject implements IGroup {

    private String context;
    private String definition;    
    private MetadataID metadataID;
    
    public GroupImpl(String context, String definition, MetadataID id) {
        this.context = context;
        this.definition = definition;
        this.metadataID = id;
    }

    /**
     * @see com.metamatrix.connector.language.IGroup#getContext()
     */
    public String getContext() {
        return context;
    }

    /**
     * @see com.metamatrix.connector.language.IGroup#getDefinition()
     */
    public String getDefinition() {
        return this.definition;
    }

    /**
     * @see com.metamatrix.connector.language.IMetadataReference#getMetadataID()
     */
    public MetadataID getMetadataID() {
        return metadataID;
    }
        
    public void setMetadataID(MetadataID id){
        this.metadataID = id;
    }

    /**
     * @see com.metamatrix.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /* 
     * @see com.metamatrix.data.language.IGroup#setDefinition(java.lang.String)
     */
    public void setDefinition(String definition) {
        this.definition = definition;
    }

    /* 
     * @see com.metamatrix.data.language.IGroup#setContext(java.lang.String)
     */
    public void setContext(String context) {
        this.context = context;
    }
    
    public int hashCode() {
        return HashCodeUtil.hashCode(HashCodeUtil.hashCode(0, this.getDefinition()), this.getContext());
    }
    
    public boolean equals(Object obj) {
        if(this == obj) {
            return true;
        }

        if(obj == null || ! (obj instanceof IGroup)) {
            return false;
        }
        IGroup other = (IGroup) obj;

        // Two group symbols will be equal only if both use aliases or both
        // don't use aliases.  In either case, comparing context names is
        // enough.
        if( (this.getDefinition() == null && other.getDefinition() == null) ||
            (this.getDefinition() != null && other.getDefinition() != null) ) {

            return this.getContext().equalsIgnoreCase(other.getContext());

        }
        return false;
    }

}
