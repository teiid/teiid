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

import com.metamatrix.connector.language.ICriteria;
import com.metamatrix.connector.language.IGroup;
import com.metamatrix.connector.language.ISetClauseList;
import com.metamatrix.connector.language.IUpdate;
import com.metamatrix.connector.visitor.framework.LanguageObjectVisitor;

public class UpdateImpl extends BaseLanguageObject implements IUpdate {
    
    private IGroup group;
    private ISetClauseList changes;
    private ICriteria criteria;
    
    public UpdateImpl(IGroup group, ISetClauseList changes, ICriteria criteria) {
        this.group = group;
        this.changes = changes;
        this.criteria = criteria;
    }

    /**
     * @see com.metamatrix.connector.language.IUpdate#getGroup()
     */
    public IGroup getGroup() {
        return group;
    }

    /**
     * @see com.metamatrix.connector.language.IUpdate#getChanges()
     */
    public ISetClauseList getChanges() {
        return changes;
    }

    /**
     * @see com.metamatrix.connector.language.IUpdate#getCriteria()
     */
    public ICriteria getCriteria() {
        return criteria;
    }

    /**
     * @see com.metamatrix.connector.language.ILanguageObject#acceptVisitor(com.metamatrix.data.visitor.LanguageObjectVisitor)
     */
    public void acceptVisitor(LanguageObjectVisitor visitor) {
        visitor.visit(this);
    }

    /* 
     * @see com.metamatrix.data.language.IUpdate#setGroup(com.metamatrix.data.language.IGroup)
     */
    public void setGroup(IGroup group) {
        this.group = group;
    }

    public void setChanges(ISetClauseList changes) {
        this.changes = changes;
    }

    /* 
     * @see com.metamatrix.data.language.IUpdate#setCriteria(com.metamatrix.data.language.ICriteria)
     */
    public void setCriteria(ICriteria criteria) {
        this.criteria = criteria;
    }

}
