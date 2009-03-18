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

import org.teiid.connector.language.IParameter;
import org.teiid.connector.metadata.runtime.Parameter;
import org.teiid.connector.visitor.framework.LanguageObjectVisitor;

public class ParameterImpl extends BaseLanguageObject implements IParameter {

    private int index;
    private Direction direction;
    private Object value;
    private boolean valueSpecified;
    private Class type;
    private Parameter metadataObject;
    
    public ParameterImpl(int index, Direction direction, Object value, Class type, Parameter metadataObject) {
        setIndex(index);
        setDirection(direction);
        setValue(value);
        setType(type);
        this.metadataObject = metadataObject;
    }
    
    /**
     * @see org.teiid.connector.language.IParameter#getIndex()
     */
    public int getIndex() {
        return this.index;
    }

    /**
     * @see org.teiid.connector.language.IParameter#getDirection()
     */
    public Direction getDirection() {
        return this.direction;
    }

    /**
     * @see org.teiid.connector.language.IParameter#getType()
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
     * @see org.teiid.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
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
    public void setDirection(Direction direction) {
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

    @Override
    public Parameter getMetadataObject() {
    	return this.metadataObject;
    }

    public void setMetadataObject(Parameter metadataObject) {
		this.metadataObject = metadataObject;
	}
    
    /** 
     * @see org.teiid.connector.language.IParameter#getValueSpecified()
     * @since 4.3.2
     */
    public boolean getValueSpecified() {
        return this.valueSpecified;
    }
    
    /** 
     * @see org.teiid.connector.language.IParameter#setValueSpecified(boolean)
     * @since 4.3.2
     */
    public void setValueSpecified(boolean specified) {
        this.valueSpecified = specified;
    }
}
