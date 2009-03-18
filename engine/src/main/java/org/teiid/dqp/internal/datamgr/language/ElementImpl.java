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

import org.teiid.connector.language.IElement;
import org.teiid.connector.language.IGroup;
import org.teiid.connector.metadata.runtime.Element;
import org.teiid.connector.visitor.framework.LanguageObjectVisitor;

public class ElementImpl extends BaseLanguageObject implements IElement {

    private IGroup group;
    private String name;
    private Element metadataObject;
    private Class type;
    
    public ElementImpl(IGroup group, String name, Element metadataObject, Class type) {
        this.group = group;
        this.name = name;
        this.metadataObject = metadataObject;
        this.type = type;
    }
    
    /**
     * @see org.teiid.connector.language.IElement#getName()
     */
    public String getName() {
        return this.name;
    }

    /**
     * @see org.teiid.connector.language.IElement#getGroup()
     */
    public IGroup getGroup() {
        return group;
    }

    @Override
    public Element getMetadataObject() {
    	return this.metadataObject;
    }
    
    public void setMetadataObject(Element metadataObject) {
		this.metadataObject = metadataObject;
	}
    
    /**
     * @see org.teiid.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /* 
     * @see com.metamatrix.data.language.IElement#setGroup(com.metamatrix.data.language.IGroup)
     */
    public void setGroup(IGroup group) {
        this.group = group;
    }

    /* 
     * @see com.metamatrix.data.language.IExpression#getType()
     */
    public Class getType() {
        return this.type;
    }

    /* 
     * @see com.metamatrix.data.language.IElement#setName(java.lang.String)
     */
    public void setName(String name) {
        this.name = name;
    }

    /* 
     * @see com.metamatrix.data.language.IExpression#setType(java.lang.Class)
     */
    public void setType(Class type) {
        this.type = type;
    }

    /**
     * Compare the symbol based ONLY on name.  Symbols are not compared based on
     * their underlying physical metadata IDs but rather on their representation
     * in the context of a particular query.  Case is not important when comparing
     * symbol names.
     * @param obj Other object
     * @return True if other obj is a Symbol (or subclass) and name is equal
     */
    public boolean equals(Object obj) {
        if(obj == this) {
            return true;
        }

        if(obj != null && obj instanceof IElement) {
            IElement other = (IElement) obj;
            
            // Compare groups
            if(other.getGroup() == null) {
                if(this.getGroup() != null) {
                    return false;
                }
            } else {
                if(this.getGroup() == null) {
                    return false;
                }
                if(! other.getGroup().equals(this.getGroup())) {
                    return false;
                }
            }
            
            // Compare elements
            String thisShortName = this.getName();
            int dotIndex = thisShortName.lastIndexOf('.');
            if(dotIndex >= 0) {
                thisShortName = thisShortName.substring(dotIndex+1);
            }
            
            String otherShortName = other.getName();
            dotIndex = otherShortName.lastIndexOf('.');
            if(dotIndex >= 0) {
                otherShortName = otherShortName.substring(dotIndex+1);
            }
            
            return thisShortName.equalsIgnoreCase(otherShortName);
        }
        return false;
    }

}
