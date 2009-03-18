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

package org.teiid.dqp.internal.datamgr.language;

import org.teiid.connector.language.IGroup;
import org.teiid.connector.metadata.runtime.Group;
import org.teiid.connector.visitor.framework.LanguageObjectVisitor;

import com.metamatrix.core.util.HashCodeUtil;

public class GroupImpl extends BaseLanguageObject implements IGroup {

    private String context;
    private String definition;    
    private Group metadataObject;
    
    public GroupImpl(String context, String definition, Group group) {
        this.context = context;
        this.definition = definition;
        this.metadataObject = group;
    }

    /**
     * @see org.teiid.connector.language.IGroup#getContext()
     */
    public String getContext() {
        return context;
    }

    /**
     * @see org.teiid.connector.language.IGroup#getDefinition()
     */
    public String getDefinition() {
        return this.definition;
    }

    @Override
    public Group getMetadataObject() {
    	return this.metadataObject;
    }
    
    public void setMetadataObject(Group metadataObject) {
		this.metadataObject = metadataObject;
	}

    /**
     * @see org.teiid.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
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
