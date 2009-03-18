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

import java.util.List;

import org.teiid.connector.language.IFromItem;
import org.teiid.connector.language.IJoin;
import org.teiid.connector.visitor.framework.LanguageObjectVisitor;


public class JoinImpl extends BaseLanguageObject implements IJoin {

    private IFromItem leftItem = null;
    private IFromItem rightItem = null;
    private JoinType joinType;
    private List criteria = null;
    
    public JoinImpl(IFromItem left, IFromItem right, JoinType joinType, List criteria) {
        this.leftItem = left;
        this.rightItem = right;
        this.joinType = joinType;
        this.criteria = criteria;
    }
    /**
     * @see org.teiid.connector.language.IJoin#getLeftItem()
     */
    public IFromItem getLeftItem() {
        return leftItem;
    }

    /**
     * @see org.teiid.connector.language.IJoin#getRightItem()
     */
    public IFromItem getRightItem() {
        return rightItem;
    }

    /**
     * @see org.teiid.connector.language.IJoin#getJoinType()
     */
    public JoinType getJoinType() {
        return this.joinType;
    }

    /**
     * @see org.teiid.connector.language.IJoin#getCriteria()
     */
    public List getCriteria() {
        return criteria;
    }

    /**
     * @see org.teiid.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }
    /* 
     * @see com.metamatrix.data.language.IJoin#setLeftItem(com.metamatrix.data.language.IFromItem)
     */
    public void setLeftItem(IFromItem item) {
        this.leftItem = item;
    }
    /* 
     * @see com.metamatrix.data.language.IJoin#setRightItem(com.metamatrix.data.language.IFromItem)
     */
    public void setRightItem(IFromItem item) {
        this.rightItem = item;
    }
    /* 
     * @see com.metamatrix.data.language.IJoin#setJoinType(int)
     */
    public void setJoinType(JoinType type) {
        this.joinType = type;
    }
    /* 
     * @see com.metamatrix.data.language.IJoin#setCriteria(java.util.List)
     */
    public void setCriteria(List criteria) {
        this.criteria = criteria;
    }

}
