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

package com.metamatrix.dqp.internal.datamgr.language;

import com.metamatrix.connector.language.IParameter;
import com.metamatrix.connector.metadata.runtime.MetadataID;
import com.metamatrix.connector.visitor.framework.LanguageObjectVisitor;

public class ParameterImpl extends BaseLanguageObject implements IParameter {

    private int index;
    private int direction;
    private Object value;
    private boolean valueSpecified;
    private Class type;
    private MetadataID metadataID;
    
    public ParameterImpl(int index, int direction, Object value, Class type, MetadataID metadataID) {
        setIndex(index);
        setDirection(direction);
        setValue(value);
        setType(type);
        setMetadataID(metadataID);
    }
    
    /**
     * @see com.metamatrix.connector.language.IParameter#getIndex()
     */
    public int getIndex() {
        return this.index;
    }

    /**
     * @see com.metamatrix.connector.language.IParameter#getDirection()
     */
    public int getDirection() {
        return this.direction;
    }

    /**
     * @see com.metamatrix.connector.language.IParameter#getType()
     */
    public Class getType() {
        return this.type;
    }

    /*
     * @see com.metamatrix.data.language.IParameter#getValue()
     */
    public Object getValue() {
        return this.value;
    }

    /**
     * @see com.metamatrix.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /* 
     * @see com.metamatrix.data.language.IParameter#setIndex(int)
     */
    public void setIndex(int index) {
        this.index = index;
    }

    /* 
     * @see com.metamatrix.data.language.IParameter#setDirection(int)
     */
    public void setDirection(int direction) {
        this.direction = direction;
    }

    /* 
     * @see com.metamatrix.data.language.IParameter#setType(java.lang.Class)
     */
    public void setType(Class type) {
        this.type = type;
    }

    /* 
     * @see com.metamatrix.data.language.IParameter#setValue(java.lang.Object)
     */
    public void setValue(Object value) {
        this.value = value;
        if(value != null) {
            setValueSpecified(true);
        }
    }

    public MetadataID getMetadataID() {
        return this.metadataID;
    }

    /* 
     * @see com.metamatrix.data.language.IMetadataReference#setMetadataID(com.metamatrix.data.metadata.runtime.MetadataID)
     */
    public void setMetadataID(MetadataID metadataID) {
        this.metadataID = metadataID;
    }
    
    /** 
     * @see com.metamatrix.connector.language.IParameter#getValueSpecified()
     * @since 4.3.2
     */
    public boolean getValueSpecified() {
        return this.valueSpecified;
    }
    
    /** 
     * @see com.metamatrix.connector.language.IParameter#setValueSpecified(boolean)
     * @since 4.3.2
     */
    public void setValueSpecified(boolean specified) {
        this.valueSpecified = specified;
    }
}
