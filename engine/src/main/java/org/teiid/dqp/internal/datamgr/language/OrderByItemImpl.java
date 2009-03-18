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
import org.teiid.connector.language.IOrderByItem;
import org.teiid.connector.visitor.framework.LanguageObjectVisitor;

public class OrderByItemImpl extends BaseLanguageObject implements IOrderByItem {
    
    private String name;
    private boolean direction = false;
    private IElement element;       // optional, may be null
    
    public OrderByItemImpl(String name, boolean direction, IElement element) {
        this.name = name;
        this.direction = direction;
        this.element = element;
    }

    /**
     * @see org.teiid.connector.language.IOrderByItem#getName()
     */
    public String getName() {
        return this.name;
    }

    /**
     * @see org.teiid.connector.language.IOrderByItem#getDirection()
     */
    public boolean getDirection() {
        return direction;
    }

    /**
     * @see org.teiid.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /* 
     * @see com.metamatrix.data.language.IOrderByItem#setName(java.lang.String)
     */
    public void setName(String name) {
        this.name = name;
    }

    /* 
     * @see com.metamatrix.data.language.IOrderByItem#setDirection(boolean)
     */
    public void setDirection(boolean direction) {
        this.direction = direction;
    }

    /* 
     * @see com.metamatrix.data.language.IOrderByElementItem#getElement()
     */
    public IElement getElement() {
        return this.element;
    }

    /* 
     * @see com.metamatrix.data.language.IOrderByElementItem#setElement(com.metamatrix.data.language.IElement)
     */
    public void setElement(IElement element) {
        this.element = element;
    }

}
